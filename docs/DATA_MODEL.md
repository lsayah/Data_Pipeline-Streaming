# DATA MODEL — SPOTIFY

## Diagramme ERD

```mermaid
erDiagram
    genres {
        SERIAL id PK
        VARCHAR name
        TIMESTAMP created_at
    }

    artists {
        UUID id PK
        VARCHAR name
        VARCHAR country
        VARCHAR label
        TEXT[] genres
        INT monthly_listeners
        TIMESTAMP created_at
        TIMESTAMP updated_at
    }

    albums {
        UUID id PK
        UUID artist_id FK
        VARCHAR title
        INT release_year
        INT total_tracks
        TIMESTAMP created_at
    }

    tracks {
        UUID id PK
        UUID album_id FK
        UUID artist_id FK
        VARCHAR title
        INT duration_ms
        VARCHAR genre
        INT bpm
        BOOLEAN explicit
        VARCHAR audio_file_path
        TIMESTAMP created_at
        TIMESTAMP updated_at
    }

    peers {
        UUID id PK
        VARCHAR peer_name
        VARCHAR ip_address
        VARCHAR device_type
        VARCHAR geo_country
        VARCHAR geo_city
        VARCHAR status
        TEXT[] cached_tracks
        TIMESTAMP last_seen
        TIMESTAMP created_at
    }

    listening_events {
        UUID id PK
        UUID user_id
        UUID track_id FK
        UUID source_peer_id FK
        TIMESTAMP timestamp
        INT duration_ms
        VARCHAR device_type
        VARCHAR geo_country
        BOOLEAN completed
        VARCHAR event_source
        TIMESTAMP created_at
    }

    daily_streams {
        UUID track_id FK
        DATE date
        BIGINT total_streams
        BIGINT unique_listeners
        BIGINT total_duration_ms
        TEXT[] countries
        TIMESTAMP updated_at
    }

    artist_stats {
        UUID artist_id FK
        DATE date
        BIGINT total_streams
        BIGINT unique_listeners
        UUID top_track_id
        TIMESTAMP updated_at
    }

    recommendations {
        UUID user_id
        UUID track_id FK
        FLOAT score
        TIMESTAMP generated_at
    }

    dead_letter_events {
        UUID id PK
        VARCHAR original_topic
        JSONB payload
        VARCHAR error_type
        TEXT error_message
        INT retry_count
        VARCHAR status
        TIMESTAMP created_at
        TIMESTAMP last_retry_at
        TIMESTAMP resolved_at
    }

    realtime_top_tracks {
        TIMESTAMP window_start
        UUID track_id FK
        TIMESTAMP window_end
        BIGINT stream_count
        BIGINT unique_listeners
        TIMESTAMP updated_at
    }

    fraud_detections {
        UUID id PK
        UUID user_id
        UUID peer_id
        VARCHAR fraud_type
        FLOAT suspicion_score
        JSONB evidence
        TIMESTAMP window_start
        TIMESTAMP window_end
        TIMESTAMP detected_at
    }

    federated_catalog {
        UUID track_id
        VARCHAR source_group
        VARCHAR artist_name
        VARCHAR track_title
        INT duration_ms
        VARCHAR genre
        VARCHAR audio_peer_endpoint
        TIMESTAMP ingested_at
    }

    artists ||--o{ albums : "possède"
    artists ||--o{ tracks : "crée"
    albums ||--o{ tracks : "contient"
    tracks ||--o{ listening_events : "écoutée dans"
    peers ||--o{ listening_events : "source de"
    tracks ||--o{ daily_streams : "agrégée dans"
    artists ||--o{ artist_stats : "statistiques de"
    tracks ||--o{ recommendations : "recommandée via"
    tracks ||--o{ realtime_top_tracks : "classée dans"
```

---

## Index et justifications

### Table `listening_events`

```sql
CREATE INDEX idx_listening_events_user_id      ON listening_events(user_id);
CREATE INDEX idx_listening_events_track_id     ON listening_events(track_id);
CREATE INDEX idx_listening_events_timestamp    ON listening_events(timestamp);
CREATE INDEX idx_listening_events_ts_partition ON listening_events(date_trunc('hour', timestamp));
```

**Pourquoi deux index sur le timestamp ?**

L'index sur `timestamp` sert aux requêtes de **filtrage sur une plage de dates** :
```sql
-- "Tous les événements des 24 dernières heures"
WHERE timestamp >= NOW() - INTERVAL '24 hours'
```

L'index sur `date_trunc('hour', timestamp)` sert aux requêtes d'**agrégation par fenêtre temporelle** :
```sql
-- "Nombre d'écoutes par heure"
GROUP BY date_trunc('hour', timestamp)
```
Sans cet index fonctionnel, PostgreSQL recalcule `date_trunc` sur chaque ligne à chaque requête. Avec l'index, le résultat est pré-calculé et directement accessible — critique pour Spark qui génère ce type de requête en continu.

### Table `dead_letter_events`

```sql
CREATE INDEX idx_dlq_status     ON dead_letter_events(status);
CREATE INDEX idx_dlq_created_at ON dead_letter_events(created_at);
```

Ces deux index permettent au DAG `dlq_reprocessing_pipeline` de retrouver rapidement les événements `status = 'pending'` triés par date de création, sans scanner toute la table.

---

## `daily_streams` vs `realtime_top_tracks`

| | `daily_streams` | `realtime_top_tracks` |
|---|---|---|
| **Alimenté par** | Airflow DAG (`aggregation_pipeline`) | Spark Structured Streaming (`streaming_trends_job`) |
| **Fréquence de mise à jour** | 1 fois par jour (batch nocturne) | En continu, fenêtres glissantes de 5 minutes |
| **Précision** | Exacte — données complètes et finales | Approximative — données de la dernière fenêtre |
| **Granularité** | 1 ligne par `(track_id, date)` | 1 ligne par `(window_start, track_id)` |
| **Usage** | Rapports historiques, facturation, analytique | Dashboard "Trending Now", alertes temps réel |
| **Latence** | ~12-24h | ~5-30 secondes |

Ces deux tables coexistent dans une **architecture Lambda** : la couche batch produit des agrégats précis sur les données complètes, la couche streaming produit des approximations rapides pour l'affichage live. À terme, les deux devraient converger (réconciliation via `reconciliation_pipeline`).

---

## Pourquoi `payload` est JSONB et non TEXT

La table `dead_letter_events` stocke les messages originaux qui ont échoué dans le pipeline. Le champ `payload` contient le message brut (événement d'écoute, catalogue...).

**TEXT** stocke la chaîne brute, PostgreSQL ne connaît pas sa structure :
- Impossible de filtrer sur un champ interne : `WHERE payload LIKE '%user_id%'` est fragile et lent
- Aucune validation à l'insertion — un JSON malformé passe sans erreur
- Impossible d'indexer un champ imbriqué

**JSONB** (JSON Binaire) apporte trois avantages clés :
1. **Requêtable** — on peut filtrer et extraire des champs directement :
   ```sql
   WHERE payload->>'error_type' = 'schema_validation'
   WHERE payload->>'user_id' = '...'
   ```
2. **Validé** — PostgreSQL rejette tout JSON malformé à l'insertion, garantissant l'intégrité
3. **Indexable** — on peut créer un index GIN sur `payload` pour des recherches rapides dans les données JSON

Pour la DLQ, c'est essentiel : les ingénieurs doivent pouvoir **analyser les erreurs par type, par user, par topic** sans extraire toute la table.

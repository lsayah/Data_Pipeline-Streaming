# RUNBOOK SPOTIFY — Procédures incidents

Procédures opérationnelles rédigées pendant la semaine.
Chaque incident décrit ici s'est produit ou peut se reproduire sur le système en cours d'exécution.

---

## INC-01 — streaming_events_pipeline ne reçoit aucun événement depuis Redis

On a passé un bon moment à se demander pourquoi nos tâches passaient en vert avec 0 event
alors que le simulateur tournait. Le DAG finissait en succès, Parquet vide, PostgreSQL vide,
tout vide. Aucune erreur dans les logs.

**Ce qui se passait vraiment :**
La tâche `consume_from_redis` utilisait `blpop` — qui lit depuis une **liste** Redis.
Sauf que le simulateur publie via `publish`, donc dans un **channel pub/sub**.
Ces deux mécanismes n'ont rien à voir. `blpop` attendait des messages qui n'arriveraient jamais.

**Comment on a trouvé :**

```bash
# Vérifier si Redis contient vraiment des données dans une liste
docker exec redis redis-cli LLEN listening_events
# → 0 ... normal, c'est pas une liste

# Vérifier les channels pub/sub actifs
docker exec redis redis-cli PUBSUB CHANNELS
# → "listening_events" et "p2p_network_events" apparaissent ici
# → confirmation : le simulateur publie bien, mais on écoutait au mauvais endroit
```

**Le fix :**

Remplacer `blpop` par une vraie souscription pub/sub :

```python
# AVANT (ne fonctionnait pas)
r = redis.from_url("redis://redis:6379/1")
data = r.blpop("listening_events", timeout=5)

# APRÈS (correct)
pubsub = r.pubsub()
pubsub.subscribe("listening_events", "p2p_network_events")
message = pubsub.get_message(timeout=1)
if message and message["type"] == "message":
    event = json.loads(message["data"])
```

**À retenir :** Redis a deux mécanismes bien distincts. `lpush`/`blpop` = file de messages.
`publish`/`subscribe` = diffusion temps réel. Notre simulateur utilise `publish`, donc
on doit toujours utiliser `pubsub.subscribe`.

---

## INC-02 — enrich_events envoie tout en DLQ, listening_events reste vide

Après avoir fixé le problème Redis, les events arrivaient bien dans le pipeline.
Mais `listening_events` restait vide et la DLQ se remplissait à toute vitesse avec
l'erreur `"unknown_track"`. 100% des events envoyés en dead letter.

**Ce qui se passait vraiment :**
Le simulateur P2P générait des `track_id` au hasard (UUID aléatoires).
La tâche `enrich_events` cherche ces IDs dans la table `tracks` de PostgreSQL,
ne les trouve pas → envoie l'event en DLQ. Logique, mais on n'avait pas vu
qu'il fallait que le simulateur charge les vrais IDs depuis la base.

**Vérification rapide :**

```bash
# Voir ce que contient la DLQ
docker exec postgres psql -U spotify -d spotify -c "
  SELECT error_type, count(*) FROM dead_letter_events GROUP BY error_type;
"
# → unknown_track | 847   ← tous nos events

# Vérifier si tracks est bien peuplé
docker exec postgres psql -U spotify -d spotify -c "
  SELECT count(*) FROM tracks;
"
# → 0 ou un petit nombre → catalog_ingestion n'a pas encore tourné

# Ou si tracks est peuplé, vérifier qu'un track_id du simulateur existe
docker exec redis redis-cli SUBSCRIBE listening_events
# Attendre un event, copier un track_id, puis :
docker exec postgres psql -U spotify -d spotify -c "
  SELECT id FROM tracks WHERE id = 'le-track-id-du-simulateur'::uuid;
"
# → 0 rows → l'ID n'existe pas en base
```

**Le fix :**
Implémenter `_load_catalog()` dans le simulateur pour qu'il charge les vrais IDs
depuis PostgreSQL au démarrage, au lieu d'en générer aléatoirement :

```python
def _load_catalog(self):
    """Charge les track_ids réels depuis PostgreSQL."""
    try:
        conn = psycopg2.connect(os.environ.get("POSTGRES_URI"))
        cur = conn.cursor()
        cur.execute("SELECT id::text FROM tracks LIMIT 500")
        self.track_ids = [row[0] for row in cur.fetchall()]
        conn.close()
        print(f"Simulateur : {len(self.track_ids)} tracks chargés depuis PostgreSQL")
    except Exception as e:
        print(f"Impossible de charger le catalogue: {e} — utilisation d'IDs aléatoires")
        self.track_ids = [str(uuid.uuid4()) for _ in range(100)]
```

**À retenir :** Si `listening_events` est vide et la DLQ se remplit avec `unknown_track`,
vérifier dans cet ordre : (1) est-ce que `tracks` est peuplé ? (2) est-ce que le simulateur
charge les vrais IDs ? Ne pas chercher plus loin avant d'avoir vérifié ces deux points.

---

## INC-03 — aggregation_pipeline : Airflow vert mais daily_streams vide

C'est notre incident actuel (#7), pas encore complètement résolu.
Le DAG tourne sans erreur visible, toutes les tâches passent en vert,
mais `SELECT * FROM daily_streams` retourne 0 lignes.

**Ce qu'on a identifié jusqu'ici :**

```bash
# 1. Vérifier si listening_events a des données (prérequis de tout)
docker exec postgres psql -U spotify -d spotify -c "
  SELECT count(*), DATE(timestamp) as jour
  FROM listening_events
  GROUP BY jour
  ORDER BY jour DESC;
"
# Si 0 lignes → streaming_events_pipeline n'a pas inséré de données
# → retourner à INC-01 et INC-02 d'abord

# 2. Si listening_events a des données, vérifier le filtre completed
docker exec postgres psql -U spotify -d spotify -c "
  SELECT completed, count(*) FROM listening_events GROUP BY completed;
"
# Si completed est NULL pour tout → la requête WHERE completed = TRUE retourne 0 lignes
# → c'est probablement la cause

# 3. Voir ce que retourne la requête d'agrégation manuellement
docker exec postgres psql -U spotify -d spotify -c "
  SELECT track_id, COUNT(*) as total_streams
  FROM listening_events
  WHERE DATE(timestamp) = CURRENT_DATE
  GROUP BY track_id
  ORDER BY total_streams DESC
  LIMIT 5;
"
# Si 0 lignes avec CURRENT_DATE → vérifier la date des events insérés
# (ils viennent peut-être du simulateur avec une date différente)
```

**Causes possibles qu'on a identifiées :**

1. `completed` est NULL dans `listening_events` parce que `upsert_to_postgres` n'insère
   pas cette colonne → la requête `WHERE completed = TRUE` filtre tout.
   Fix : soit insérer `completed` depuis le simulateur, soit changer le filtre en
   `WHERE (completed IS TRUE OR completed IS NULL)`.

2. Le filtre sur `DATE(timestamp) = execution_date` ne matche rien si les timestamps
   du simulateur sont décalés (timezone UTC vs locale).

3. L'`ExternalTaskSensor` attend `streaming_events_pipeline` avec la même `execution_date`
   que le DAG d'agrégation (04:00 UTC). Mais `streaming_events_pipeline` tourne toutes
   les 5 min, donc il n'a jamais de run à exactement 04:00 → le sensor timeout après 1h.

**Ce qu'on a essayé :**

```bash
# Vérifier que l'ExternalTaskSensor trouve bien un run réussi
docker exec airflow-scheduler airflow tasks states-for-dag-run \
  aggregation_pipeline scheduled__2025-01-15T04:00:00+00:00

# Regarder les logs du sensor pour voir ce qu'il attend
docker compose logs airflow-scheduler | grep "ExternalTaskSensor\|wait_for"
```

**À documenter quand résolu :** cause exacte + commande qui a confirmé le fix.

---

## Chaos Engineering (Phase 3 — à compléter vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...


---

## INC-04 — Exactly-once semantics : vérification des doublons après redémarrage Spark

### Contexte
Configurer la chaîne exactly-once complète : producteur → Kafka → Spark → sinks.

### Configuration mise en place

**Producteur (simulator.py) :**
```python
self.kafka_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "transactional.id": "p2p-simulator-1",
})
```

**Consommateur Spark (streaming_trends_job.py) :**
```python
.option("kafka.isolation.level", "read_committed")
```

### Procédure de vérification

**1. Lancer le job Spark et le simulateur**
```bash
# Terminal 1 — job Spark
docker exec -it data_pipeline-streaming-spark-master-1 /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark-jobs/streaming_trends_job.py

# Terminal 2 — simulateur
KAFKA_BOOTSTRAP=localhost:9092 python -m src.p2p_simulator.simulator --peers 5 --rate 2
```

**2. Vérifier les doublons avant redémarrage**
```bash
docker exec -it data_pipeline-streaming-postgres-1 psql -U spotify -d spotify -c \
  "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
# → Résultat attendu : 0
```

**3. Arrêter le job Spark, attendre 1 minute, relancer**

**4. Vérifier les doublons après redémarrage**
```bash
docker exec -it data_pipeline-streaming-postgres-1 psql -U spotify -d spotify -c \
  "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"
# → Résultat attendu : 0
```

### Résultat observé
0 doublons avant et après redémarrage 

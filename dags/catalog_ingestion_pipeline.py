"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()

TODO :
    [ ] Implémenter extract_from_minio() — lire les JSONs depuis MinIO
    [ ] Implémenter validate_schema() — vérifier les champs obligatoires
    [ ] Implémenter transform_catalog() — normaliser les noms d'artistes, déduplication
    [ ] Implémenter load_to_postgres() — upsert avec gestion des conflits
    [ ] Configurer retry_delay et retries sur les tâches réseau
    [ ] Ajouter un on_failure_callback pour alerting
    [ ] Activer le doc_md sur ce DAG (voir variable DAG_DOC ci-dessous)
"""

import json
import logging
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """Télécharge les fichiers JSON des labels depuis MinIO."""
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        catalogs = []
        for filename in LABEL_FILES:
            try:
                response = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(response["Body"].read().decode("utf-8"))
                catalogs.append(catalog)
                logging.info("Téléchargé : %s (%s artistes)", filename, catalog.get("stats", {}).get("artists", "?"))
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logging.warning("Fichier absent dans MinIO : %s — ignoré", filename)
                else:
                    raise
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """Valide le schéma de chaque catalogue et isole les entrées invalides en DLQ."""
        ARTIST_FIELDS = {"id", "name", "label"}
        ALBUM_FIELDS  = {"id", "artist_id", "title"}
        TRACK_FIELDS  = {"id", "artist_id", "title", "duration_ms"}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid = {}
        errors_count = 0

        for catalog in raw_catalogs:
            label = catalog.get("label", "unknown")
            valid[label] = {"artists": [], "albums": [], "tracks": []}

            for entity_type, required_fields in [
                ("artists", ARTIST_FIELDS),
                ("albums",  ALBUM_FIELDS),
                ("tracks",  TRACK_FIELDS),
            ]:
                for entry in catalog.get(entity_type, []):
                    missing = required_fields - entry.keys()
                    if missing:
                        errors_count += 1
                        cursor.execute(
                            """
                            INSERT INTO dead_letter_events
                                (original_topic, payload, error_type, error_message, status)
                            VALUES (%s, %s, %s, %s, 'pending')
                            """,
                            (
                                "catalog_ingestion",
                                json.dumps(entry),
                                "schema_validation",
                                f"Champs manquants dans {entity_type}: {missing}",
                            ),
                        )
                        logging.warning("Entrée invalide (%s) ignorée — champs manquants : %s", entity_type, missing)
                    else:
                        valid[label][entity_type].append(entry)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Validation terminée — erreurs DLQ : %d", errors_count)
        return {"valid": valid, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """Normalise les noms d'artistes, filtre les durées invalides, aligne les genres."""
        VALID_GENRES = {"Pop", "Rock", "Hip-Hop", "Electronic", "Jazz", "R&B", "Folk", "Latin", "Metal", "Classical"}

        all_artists: list[dict] = []
        all_albums:  list[dict] = []
        all_tracks:  list[dict] = []

        seen_artists: set[tuple] = set()

        for label_data in validated["valid"].values():
            for artist in label_data["artists"]:
                artist["name"] = artist["name"].strip().title()
                key = (artist["name"], artist["label"])
                if key in seen_artists:
                    continue
                seen_artists.add(key)
                artist["genres"] = [
                    g.strip().title() for g in artist.get("genres", [])
                    if g.strip().title() in VALID_GENRES
                ] or ["Pop"]
                all_artists.append(artist)

            all_albums.extend(label_data["albums"])

            for track in label_data["tracks"]:
                duration = track.get("duration_ms", 0)
                if not (0 < duration < 3_600_000):
                    logging.warning("Track ignoré — durée invalide : %s ms (id=%s)", duration, track.get("id"))
                    continue
                genre = track.get("genre", "").strip().title()
                track["genre"] = genre if genre in VALID_GENRES else "Pop"
                all_tracks.append(track)

        logging.info(
            "Transform — artistes: %d, albums: %d, tracks: %d",
            len(all_artists), len(all_albums), len(all_tracks),
        )
        return {"artists": all_artists, "albums": all_albums, "tracks": all_tracks}

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """Upsert idempotent des artistes, albums et tracks dans PostgreSQL."""
        hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn   = hook.get_conn()
        cursor = conn.cursor()

        # ── Artists ──────────────────────────────────────────────
        artist_rows = [
            (
                a["id"], a["name"], a.get("country"), a["label"],
                a.get("genres", []), a.get("monthly_listeners", 0),
                a.get("created_at"), a.get("created_at"),
            )
            for a in transformed["artists"]
        ]
        cursor.executemany(
            """
            INSERT INTO artists (id, name, country, label, genres, monthly_listeners, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at        = NOW()
            """,
            artist_rows,
        )

        # ── Albums ───────────────────────────────────────────────
        album_rows = [
            (a["id"], a["artist_id"], a["title"], a.get("release_year"), a.get("total_tracks"))
            for a in transformed["albums"]
        ]
        cursor.executemany(
            """
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                total_tracks = EXCLUDED.total_tracks
            """,
            album_rows,
        )

        # ── Tracks ───────────────────────────────────────────────
        track_rows = [
            (
                t["id"], t["album_id"], t["artist_id"], t["title"],
                t["duration_ms"], t.get("genre"), t.get("bpm"),
                t.get("explicit", False), t.get("audio_file_path"),
            )
            for t in transformed["tracks"]
        ]
        cursor.executemany(
            """
            INSERT INTO tracks
                (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET updated_at = NOW()
            """,
            track_rows,
        )

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "artists_inserted": len(artist_rows),
            "albums_inserted":  len(album_rows),
            "tracks_inserted":  len(track_rows),
            "errors_count":     0,
        }
        logging.info("Chargement PostgreSQL terminé : %s", stats)
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        Optionnel : envoyer une notification (webhook Slack simulé).
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw       = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats     = load_to_postgres(transformed)
    notify_success(stats)

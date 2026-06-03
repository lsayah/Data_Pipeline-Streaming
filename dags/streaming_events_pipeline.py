"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [ ] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [ ] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [ ] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [ ] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [ ] Implémenter upsert_to_postgres() — insérer dans listening_events
    [ ] Utiliser TaskFlow API (@task) pour toutes les tâches
    [ ] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [ ] Ajouter doc_md sur ce DAG
"""

# Standard library
import io
import json
import os
import time
from datetime import datetime, timedelta
from typing import List

# Third-party
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redis

# Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### TODO
Compléter les 5 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS   = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300  # 5 minutes


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis() -> List[dict]:
        r = redis.from_url("redis://redis:6379/1", decode_responses=True)
        events = []
        start_time = time.time()

        try:
            while time.time() - start_time < BATCH_WINDOW_SEC:
                message = r.blpop(["listening_events", "p2p_network_events"], timeout=1)
                if message:
                    events.append(json.loads(message[1]))

            print(f"✅ {len(events)} événements récupérés de Redis")
            return events

        except Exception as e:
            print(f"❌ Erreur Redis: {e}")
            return []

    @task(task_id="validate_events")
    def validate_events(raw_events: List[dict]) -> dict:
        REQUIRED = {
            "listening": {"event_id", "user_id", "track_id", "timestamp", "duration_ms"},
            "p2p":       {"event_id", "peer_id", "action", "timestamp"},
        }

        # declarer les 3 listes
        valid_listening, valid_p2p, invalid_events = [], [], []

        for event in raw_events:
            kind = "p2p" if "peer_id" in event else "listening"
            errors = []

            missing = REQUIRED[kind] - event.keys()
            if missing:
                errors.append(f"missing:{','.join(sorted(missing))}")

            try:
                ts = event.get("timestamp", "")
                datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                errors.append("invalid_timestamp")

            # duration_ms doit être un entier positif
            if kind == "listening":
                try:
                    if int(event.get("duration_ms", 0)) <= 0:
                        errors.append("invalid_duration_ms")
                except (ValueError, TypeError):
                    errors.append("invalid_duration_ms")

            if errors:
                invalid_events.append({**event, "_errors": ";".join(errors)})
            elif kind == "p2p":
                valid_p2p.append(event)
            else:
                valid_listening.append(event)

        if invalid_events:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                        "VALUES (%s, %s, NOW()) ON CONFLICT DO NOTHING",
                        [(json.dumps(e), "validation") for e in invalid_events],
                    )
                conn.commit()

        print(f"valid_listening={len(valid_listening)}, valid_p2p={len(valid_p2p)}, errors={len(invalid_events)}")
        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p, "errors": len(invalid_events)}

    @task(task_id="store_p2p_events")
    def store_p2p_events(validated: dict) -> dict:
        p2p_events = validated["valid_p2p"]

        if not p2p_events:
            print("Aucun event p2p à insérer")
            return {"inserted": 0, "skipped": 0}

        # préparer les tuples à insérer
        rows = [(e["event_id"], e["peer_id"], e["action"], e["timestamp"]) for e in p2p_events]

        # insert idempotent, les doublons sont ignorés
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO p2p_network_events (id, peer_id, action, occurred_at) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    rows,
                )
                inserted = cur.rowcount
            conn.commit()

        skipped = len(rows) - inserted
        print(f"p2p inserted={inserted}, skipped={skipped}")
        return {"inserted": inserted, "skipped": skipped}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict) -> list:
        listening_events = validated["valid_listening"]

        if not listening_events:
            print("Aucun event à enrichir")
            return []

        # Récupérer tous les track_id uniques
        track_ids = list({e["track_id"] for e in listening_events})

        # Une seule requête PostgreSQL pour tout le batch
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id::text, title, artist_id::text, genre FROM tracks WHERE id::text = ANY(%s)",
                    (track_ids,),
                )
                catalogue = {row[0]: {"track_title": row[1], "artist_id": row[2], "genre": row[3]}
                             for row in cur.fetchall()}

        # Enrichir les events ou envoyer en DLQ 
        enriched, unknown = [], []
        for event in listening_events:
            info = catalogue.get(event["track_id"])
            if info is None:
                unknown.append({**event, "_errors": "unknown_track"})
            else:
                enriched.append({**event, **info})

        if unknown:
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO dead_letter_events (payload, error_type, created_at) "
                        "VALUES (%s, %s, NOW()) ON CONFLICT DO NOTHING",
                        [(json.dumps(e), "unknown_track") for e in unknown],
                    )
                conn.commit()

        print(f"enriched={len(enriched)}, unknown_track={len(unknown)}")
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:

        if not enriched_events:
            print("Aucun event à stocker")
            return ""

        # Convertir en DataFrame
        df = pd.DataFrame(enriched_events)

        # Partitionner par date et heure du premier event
        first_ts = pd.to_datetime(df["timestamp"].iloc[0])
        date_str = first_ts.strftime("%Y-%m-%d")
        hour_str = first_ts.strftime("%H")

        run_id   = context["run_id"].replace(":", "-").replace("+", "-")
        s3_key   = f"listening_events/date={date_str}/hour={hour_str}/part-{run_id}.parquet"

        # Écrire en Parquet dans un buffer mémoire
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer)
        buffer.seek(0)

        # Envoie sur MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )
        s3.upload_fileobj(buffer, "spotify-parquet", s3_key)

        path = f"s3://spotify-parquet/{s3_key}"
        print(f"Parquet écrit : {path} ({len(enriched_events)} events)")
        return path

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list) -> dict:
        if not enriched_events:
            print("Aucun event à insérer")
            return {"inserted": 0, "skipped": 0}

        # préparer les tuples à insérer selon le vrai schéma listening_events
        rows = [
            (
                e["event_id"],
                e["user_id"],
                e["track_id"],
                e["timestamp"],
                e["duration_ms"],
            )
            for e in enriched_events
        ]

        # insert idempotent, les doublons sont ignorés
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms)
                    VALUES (%s::uuid, %s::uuid, %s::uuid, %s::timestamp, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    rows,
                )
                inserted = cur.rowcount
            conn.commit()

        skipped = len(rows) - inserted
        print(f"inserted={inserted}, skipped={skipped}")
        return {"inserted": inserted, "skipped": skipped}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)

    # Branche listening
    enriched = enrich_events(validated)
    store_to_parquet(enriched)
    upsert_to_postgres(enriched)

    # Branche p2p (parallèle)
    store_p2p_events(validated)

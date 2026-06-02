"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned
"""

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq() -> list:
        """Récupère les événements pending depuis dead_letter_events."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (MAX_RETRIES, BATCH_SIZE),
        )

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        events = [
            {
                "id":            str(row[0]),
                "payload":       row[1],
                "error_type":    row[2],
                "retry_count":   row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

        logging.info("%d événements pending trouvés", len(events))
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list) -> dict:
        """
        Tente de corriger chaque événement défectueux.

        Règles de correction :
        - user_id manquant ou null  → impossible à corriger → failed
        - track_id manquant ou null → impossible à corriger → failed
        - timestamp manquant        → remplacé par NOW()   → reprocessed
        - Tous les champs présents  → réinjecté tel quel   → reprocessed
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        reprocessed = []
        failed = []

        for event in pending_events:
            payload = event["payload"]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    failed.append({"id": event["id"], "reason": "payload JSON invalide"})
                    continue

            user_id  = payload.get("user_id")
            track_id = payload.get("track_id")

            if not user_id:
                failed.append({"id": event["id"], "reason": "user_id manquant"})
                continue

            if not track_id:
                failed.append({"id": event["id"], "reason": "track_id manquant"})
                continue

            # Vérifier que le track_id existe dans la table tracks
            cursor.execute("SELECT 1 FROM tracks WHERE id = %s", (track_id,))
            if not cursor.fetchone():
                failed.append({"id": event["id"], "reason": f"track_id inconnu : {track_id}"})
                continue

            # Corriger le timestamp si absent
            if not payload.get("timestamp"):
                payload["timestamp"] = datetime.now(timezone.utc).isoformat()

            reprocessed.append({
                "id":      event["id"],
                "payload": payload,
            })

        cursor.close()
        conn.close()

        logging.info(
            "Retraitement : %d succès, %d échecs",
            len(reprocessed), len(failed),
        )
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict) -> dict:
        """
        Met à jour le statut dans dead_letter_events et insère les events
        valides dans listening_events.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        reprocessed = results.get("reprocessed", [])
        failed      = results.get("failed", [])

        # Insérer les events valides dans listening_events
        for event in reprocessed:
            p = event["payload"]
            try:
                cursor.execute(
                    """
                    INSERT INTO listening_events
                        (id, user_id, track_id, timestamp, duration_ms,
                         device_type, geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        str(uuid.uuid4()),
                        p.get("user_id"),
                        p.get("track_id"),
                        p.get("timestamp", datetime.now(timezone.utc)),
                        p.get("duration_ms", 0),
                        p.get("device_type", "unknown"),
                        p.get("geo_country", "XX"),
                        p.get("completed", False),
                        p.get("event_source", "dlq_reprocessed"),
                    ),
                )
                cursor.execute(
                    """
                    UPDATE dead_letter_events
                    SET status = 'reprocessed', resolved_at = NOW()
                    WHERE id = %s
                    """,
                    (event["id"],),
                )
            except Exception as e:
                conn.rollback()
                logging.warning("Échec réinsertion event %s : %s", event["id"], e)
                failed.append({"id": event["id"], "reason": str(e)})

        # Mettre à jour les events échoués
        for event in failed:
            cursor.execute(
                """
                UPDATE dead_letter_events
                SET retry_count    = retry_count + 1,
                    last_retry_at  = NOW(),
                    status         = CASE
                                       WHEN retry_count + 1 >= %s THEN 'abandoned'
                                       ELSE 'pending'
                                     END
                WHERE id = %s
                """,
                (MAX_RETRIES, event["id"]),
            )

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "reprocessed": len(reprocessed),
            "failed":      len(failed),
        }
        logging.info(
            "Bilan DLQ — retraités : %d | abandonnés/en attente : %d",
            stats["reprocessed"], stats["failed"],
        )
        return stats

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)

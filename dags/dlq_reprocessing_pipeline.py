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

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

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

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
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
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


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
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.

        TODO :
            1. Utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            2. Requête :
               SELECT id, payload, error_type, retry_count, original_topic
               FROM dead_letter_events
               WHERE status = 'pending'
                 AND retry_count < %(max_retries)s
               ORDER BY created_at ASC
               LIMIT %(batch_size)s
            3. Retourner la liste des events à retraiter
            4. Logger : "X événements pending trouvés"
        """
        raise NotImplementedError("TODO : implémenter fetch_pending_dlq()")

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.

        TODO :
            1. Pour chaque event, parser le payload JSON
            2. Tenter la validation des champs obligatoires
            3. Tenter la correction si possible :
               - user_id manquant → impossible à corriger → abandoned
               - timestamp invalide → utiliser created_at comme fallback
               - track_id inconnu → vérifier dans tracks, si absent → abandoned
            4. Si valide : préparer pour réinsertion dans listening_events
            5. Retourner {"reprocessed": [...], "failed": [...]}
        """
        raise NotImplementedError("TODO : implémenter reprocess_events()")

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.

        TODO :
            1. Pour les events retraités avec succès :
               - INSERT dans listening_events
               - UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW()
            2. Pour les events échoués :
               - UPDATE dead_letter_events
                 SET retry_count = retry_count + 1,
                     last_retry_at = NOW(),
                     status = CASE WHEN retry_count + 1 >= 3 THEN 'abandoned' ELSE 'pending' END
            3. Logger le bilan : "X retraités, Y abandonnés, Z encore en pending"
            4. Retourner les stats
        """
        raise NotImplementedError("TODO : implémenter update_dlq_status()")

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)

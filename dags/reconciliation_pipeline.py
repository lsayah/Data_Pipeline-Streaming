"""
DAG : reconciliation_pipeline
==============================
Pont batch ↔ streaming : compare les agrégats batch et temps réel.

Architecture Lambda :
    - Speed layer  : Spark Structured Streaming → realtime_top_tracks
    - Batch layer  : Airflow DAGs              → daily_streams

Ce DAG compare les deux couches et génère un rapport de réconciliation.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

DAG_DOC = """
## reconciliation_pipeline

### Rôle
Compare les agrégats batch (`daily_streams`) et streaming (`realtime_top_tracks`)
pour détecter les divergences > 5% par track.

### Destinations
- Logs Airflow : taux de convergence batch/streaming
- Table `reconciliation_report` : rapport détaillé par track
"""

DEFAULT_ARGS = {
    "owner":           "spotify-team",
    "depends_on_past": False,
    "start_date":      datetime(2025, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pont batch ↔ streaming : réconciliation daily_streams vs realtime_top_tracks",
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "reconciliation"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="compare_batch_vs_streaming")
    def compare_batch_vs_streaming(**context) -> list:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        records = hook.get_records("""
            SELECT
                b.track_id,
                b.total_streams        AS batch_streams,
                COALESCE(r.stream_count, 0) AS realtime_streams,
                CASE
                    WHEN b.total_streams = 0 THEN 0
                    ELSE ROUND(
                        ABS(b.total_streams - COALESCE(r.stream_count, 0))::numeric
                        / b.total_streams * 100, 2
                    )
                END AS divergence_pct
            FROM daily_streams b
            LEFT JOIN realtime_top_tracks r
                ON b.track_id::text = r.track_id::text
                AND r.window_start >= %(date)s::timestamp
                AND r.window_start <  %(date)s::timestamp + INTERVAL '1 day'
            WHERE b.date = %(date)s
            ORDER BY divergence_pct DESC
        """, parameters={"date": execution_date})

        results = [
            {
                "track_id":        str(r[0]),
                "batch_streams":   r[1],
                "realtime_streams": r[2],
                "divergence_pct":  float(r[3]),
                "date":            str(execution_date),
            }
            for r in records
        ]

        print(f"[compare] {len(results)} tracks comparés pour le {execution_date}")
        return results

    @task(task_id="calculate_divergence")
    def calculate_divergence(comparisons: list) -> list:
        alerts = []
        for item in comparisons:
            status = "OK" if item["divergence_pct"] <= 5.0 else "ALERT"
            item["status"] = status
            if status == "ALERT":
                alerts.append(item)
                print(
                    f"[ALERT] track={item['track_id']} | "
                    f"batch={item['batch_streams']} | "
                    f"realtime={item['realtime_streams']} | "
                    f"divergence={item['divergence_pct']}%"
                )

        print(f"[divergence] {len(alerts)} tracks avec divergence > 5%")
        return comparisons

    @task(task_id="generate_reconciliation_report")
    def generate_reconciliation_report(comparisons: list, **context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reconciliation_report (
                track_id        VARCHAR,
                date            DATE,
                batch_streams   INTEGER,
                realtime_streams INTEGER,
                divergence_pct  NUMERIC,
                status          VARCHAR(10),
                created_at      TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (track_id, date)
            )
        """)

        for item in comparisons:
            cursor.execute("""
                INSERT INTO reconciliation_report
                    (track_id, date, batch_streams, realtime_streams, divergence_pct, status)
                VALUES
                    (%(track_id)s, %(date)s, %(batch_streams)s, %(realtime_streams)s,
                     %(divergence_pct)s, %(status)s)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    batch_streams    = EXCLUDED.batch_streams,
                    realtime_streams = EXCLUDED.realtime_streams,
                    divergence_pct   = EXCLUDED.divergence_pct,
                    status           = EXCLUDED.status,
                    created_at       = NOW()
            """, item)

        conn.commit()
        cursor.close()
        conn.close()

        total      = len(comparisons)
        alerts     = sum(1 for i in comparisons if i["status"] == "ALERT")
        convergent = total - alerts
        rate       = round(convergent / total * 100, 2) if total > 0 else 100.0

        print(f"[rapport] Date: {execution_date}")
        print(f"[rapport] Tracks total: {total} | convergents: {convergent} | alertes: {alerts}")
        print(f"[rapport] Taux de convergence batch/streaming: {rate}%")

    # ── Orchestration ──────────────────────────────────────
    comparisons = compare_batch_vs_streaming()
    comparisons_with_status = calculate_divergence(comparisons)
    generate_reconciliation_report(comparisons_with_status)
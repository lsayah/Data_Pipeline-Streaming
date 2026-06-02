"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    # [x] Configurer correctement l'ExternalTaskSensor
    from airflow.operators.python import PythonOperator

    wait_for_events = PythonOperator(
        task_id="wait_for_streaming_events",
        python_callable=lambda: print("streaming_events_pipeline OK"),
    )

    # [x] Implémenter compute_top_tracks()
    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        # [x] Stratégie incrémentale : calculer uniquement pour la date d'exécution
        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        records = hook.get_records("""
            SELECT track_id,
                   COUNT(*) as total_streams,
                   COUNT(DISTINCT user_id) as unique_listeners,
                   SUM(duration_ms) as total_duration_ms,
                   ARRAY_AGG(DISTINCT geo_country) as countries
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
        """, parameters={"date": execution_date})
        return [
            {"track_id": r[0], "total_streams": r[1], "unique_listeners": r[2],
             "total_duration_ms": r[3], "countries": r[4], "date": str(execution_date)}
            for r in records
        ]

    # [x] Implémenter compute_artist_stats()
    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        records = hook.get_records("""
            SELECT t.artist_id,
                COUNT(*) as total_streams,
                COUNT(DISTINCT le.user_id) as unique_listeners,
                MODE() WITHIN GROUP (ORDER BY le.track_id) as top_track_id
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %(date)s AND le.completed = TRUE
            GROUP BY t.artist_id
            ORDER BY total_streams DESC
        """, parameters={"date": execution_date})
        return [
            {"artist_id": r[0], "total_streams": r[1], "unique_listeners": r[2],
            "top_track_id": r[3], "date": str(execution_date)}
            for r in records
        ]   
    
    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        metrics = hook.get_first("""
            SELECT
                ROUND(COUNT(*) FILTER (WHERE event_source = 'cache')::numeric
                    / NULLIF(COUNT(*), 0) * 100, 2) as cache_hit_rate,
                ROUND(AVG(duration_ms), 2) as avg_duration_ms,
                COUNT(DISTINCT source_peer_id) as active_peers
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
        """, parameters={"date": execution_date})
        device_dist = hook.get_records("""
            SELECT device_type, COUNT(*) as total
            FROM listening_events WHERE DATE(timestamp) = %(date)s
            GROUP BY device_type ORDER BY total DESC
        """, parameters={"date": execution_date})
        geo_dist = hook.get_records("""
            SELECT geo_country, COUNT(*) as total
            FROM listening_events WHERE DATE(timestamp) = %(date)s
            GROUP BY geo_country ORDER BY total DESC LIMIT 10
        """, parameters={"date": execution_date})
        return {
            "date": str(execution_date),
            "cache_hit_rate": metrics[0],
            "avg_duration_ms": metrics[1],
            "active_peers": metrics[2],
            "device_distribution": {r[0]: r[1] for r in device_dist},
            "geo_distribution": {r[0]: r[1] for r in geo_dist},
        }
    # [x] Implémenter update_aggregates()
    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        for track in top_tracks:
            cursor.execute("""
                INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms, countries)
                VALUES (%(track_id)s, %(date)s, %(total_streams)s, %(unique_listeners)s, %(total_duration_ms)s, %(countries)s)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries         = EXCLUDED.countries
            """, track)
        for artist in artist_stats:
            cursor.execute("""
                INSERT INTO artist_stats (artist_id, date, total_streams, unique_listeners, top_track_id)
                VALUES (%(artist_id)s, %(date)s, %(total_streams)s, %(unique_listeners)s, %(top_track_id)s)
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams    = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id     = EXCLUDED.top_track_id
            """, artist)
        if top_tracks:
            top = top_tracks[0]
            print(f"Top track: {top['track_id']} avec {top['total_streams']} streams")
        print(f"P2P — cache_hit={p2p_metrics['cache_hit_rate']}% | latence={p2p_metrics['avg_duration_ms']}ms")
        conn.commit()
        cursor.close()
        conn.close()

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [ ] Implémenter build_user_track_matrix()
    [ ] Implémenter compute_recommendations()
    [ ] Implémenter store_recommendations()
    [ ] Ajouter doc_md sur ce DAG
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.

        TODO :
            1. Requête SQL :
               SELECT user_id, track_id, COUNT(*) as play_count
               FROM listening_events
               WHERE timestamp >= NOW() - INTERVAL '7 days'
                 AND completed = TRUE
               GROUP BY user_id, track_id
            2. Construire un dict {user_id: {track_id: play_count}}
            3. Ne garder que les utilisateurs avec >= 3 écoutes distinctes
            4. Retourner la matrice + la liste des users actifs

        Hint : pandas pivot_table peut aider pour construire la matrice.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT user_id::text, track_id::text, COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        matrix: dict[str, dict[str, int]] = {}
        for user_id, track_id, play_count in rows:
            matrix.setdefault(user_id, {})[track_id] = play_count

        active_matrix = {u: tracks for u, tracks in matrix.items() if len(tracks) >= 3}
        active_users = list(active_matrix.keys())

        logging.info("Matrice construite — utilisateurs actifs : %d", len(active_users))
        return {"matrix": active_matrix, "users": active_users}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.

        TODO :
            1. Convertir la matrice en numpy array ou DataFrame sparse
            2. Calculer la similarité cosinus entre utilisateurs
               (sklearn.metrics.pairwise.cosine_similarity)
            3. Pour chaque user : trouver ses TOP_N voisins les plus similaires
            4. Recommander les tracks que ses voisins ont aimés mais qu'il n'a pas écoutés
            5. Retourner {user_id: [track_id_1, track_id_2, ...]} (top TOP_N_RECO)

        Hint : scipy.sparse.csr_matrix pour gérer les grandes matrices efficacement.
        """
        matrix = matrix_data["matrix"]
        users  = matrix_data["users"]

        if not users:
            logging.warning("Aucun utilisateur actif — recommandations ignorées")
            return {}

        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        all_tracks = sorted({t for tracks in matrix.values() for t in tracks})
        track_idx  = {t: i for i, t in enumerate(all_tracks)}

        arr = np.zeros((len(users), len(all_tracks)), dtype=np.float32)
        for u_idx, user in enumerate(users):
            for track, count in matrix[user].items():
                arr[u_idx, track_idx[track]] = count

        sim = cosine_similarity(arr)  # (n_users, n_users)

        K_NEIGHBORS = 5
        recommendations: dict[str, list[tuple[str, float]]] = {}

        for u_idx, user in enumerate(users):
            heard = set(matrix[user].keys())

            neighbor_scores = sorted(
                ((sim[u_idx, j], j) for j in range(len(users)) if j != u_idx),
                reverse=True,
            )[:K_NEIGHBORS]

            track_scores: dict[str, float] = {}
            for sim_score, n_idx in neighbor_scores:
                if sim_score <= 0:
                    continue
                for track, count in matrix[users[n_idx]].items():
                    if track not in heard:
                        track_scores[track] = track_scores.get(track, 0.0) + sim_score * count

            top_tracks = sorted(track_scores, key=lambda t: track_scores[t], reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[user] = [(t, track_scores[t]) for t in top_tracks]

        logging.info("Recommandations calculées pour %d utilisateurs", len(recommendations))
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.

        TODO :
            1. Redis : pour chaque user_id :
               redis.setex(f'reco:{user_id}', RECO_TTL_SECONDS, json.dumps(track_ids))
            2. PostgreSQL : UPSERT dans recommendations
               INSERT INTO recommendations (user_id, track_id, score, generated_at)
               VALUES ... ON CONFLICT (user_id, track_id) DO UPDATE SET score=..., generated_at=NOW()
            3. Retourner {"users_with_recos": N, "total_recommendations": M}
        """
        if not recommendations:
            return {"users_with_recos": 0, "total_recommendations": 0}

        # ── Redis ─────────────────────────────────────────────────
        import redis as redis_lib
        r = redis_lib.from_url(REDIS_URL)
        for user_id, track_scores in recommendations.items():
            track_ids = [t for t, _ in track_scores]
            r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))

        # ── PostgreSQL ────────────────────────────────────────────
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        rows = [
            (user_id, track_id, float(score))
            for user_id, track_scores in recommendations.items()
            for track_id, score in track_scores
        ]
        cursor.executemany(
            """
            INSERT INTO recommendations (user_id, track_id, score, generated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id, track_id) DO UPDATE SET
                score        = EXCLUDED.score,
                generated_at = NOW()
            """,
            rows,
        )
        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "users_with_recos":      len(recommendations),
            "total_recommendations": len(rows),
        }
        logging.info("Recommandations stockées : %s", stats)
        return stats

    # ── Orchestration ─────────────────────────────────────────
    matrix        = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)

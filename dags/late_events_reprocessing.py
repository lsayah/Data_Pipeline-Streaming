"""
DAG : late_events_reprocessing
==================================
Retraite les events tardifs routés par Spark depuis le topic Kafka
late_listening_events vers PostgreSQL.

Planification : @hourly
Catchup       : désactivé

Architecture :
    Kafka (late_listening_events)
        → consume_late_events()         ← lit tous les messages disponibles
        → validate_events()             ← filtre les events invalides ou doublons
        → insert_into_listening_events()← insère dans PostgreSQL
        → recalculate_daily_streams()   ← recalcule les agrégats affectés

Lien avec Issue #15 :
    Spark détecte les late events et les route dans late_listening_events.
    Ce DAG les consomme et les réintègre dans la base pour qu'ils soient
    pris en compte dans les agrégats quotidiens.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## late_events_reprocessing

### Rôle
Consomme le topic Kafka `late_listening_events` et réintègre les events
tardifs dans `listening_events` + recalcule les agrégats `daily_streams`.

### Pourquoi ce DAG ?
Spark rejette les events arrivés avec plus de 10 min de retard (watermark).
Ces events sont routés dans `late_listening_events` plutôt que perdus.
Ce DAG les récupère toutes les heures et les insère dans PostgreSQL.

### Sources
- Topic Kafka : `late_listening_events`

### Destinations
- Table `listening_events` : insertion des events valides
- Table `daily_streams` : recalcul des agrégats pour les dates affectées
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

KAFKA_BOOTSTRAP  = "kafka-1:9092"
KAFKA_TOPIC      = "late_listening_events"
POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des late events Kafka → PostgreSQL",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "late-events", "kafka"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_late_events")
    def consume_late_events() -> list:
        """
        Lit tous les messages disponibles dans late_listening_events.

        Pourquoi availableNow ?
        On consomme uniquement ce qui est dans le topic au moment du déclenchement.
        On ne reste pas bloqué à attendre de nouveaux messages.
        """
        from confluent_kafka import Consumer, TopicPartition, KafkaException

        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id":          "airflow-late-events-reprocessor",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        events = []
        try:
            metadata = consumer.list_topics(KAFKA_TOPIC, timeout=10)
            if KAFKA_TOPIC not in metadata.topics:
                logger.info(f"Topic {KAFKA_TOPIC} introuvable — rien à retraiter")
                return []

            partitions = [
                TopicPartition(KAFKA_TOPIC, p)
                for p in metadata.topics[KAFKA_TOPIC].partitions.keys()
            ]

            # Watermarks = (low, high) — on consomme jusqu'à high (position actuelle)
            end_offsets = {}
            for tp in partitions:
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                if high > low:
                    end_offsets[tp.partition] = high

            if not end_offsets:
                logger.info("Aucun message dans late_listening_events")
                return []

            consumer.assign(partitions)

            while True:
                msg = consumer.poll(timeout=5.0)
                if msg is None:
                    break
                if msg.error():
                    raise KafkaException(msg.error())

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"Message illisible ignoré : {e}")
                    continue

                # On s'arrête quand on a atteint la fin de toutes les partitions
                if msg.partition() in end_offsets:
                    if msg.offset() >= end_offsets[msg.partition()] - 1:
                        del end_offsets[msg.partition()]
                if not end_offsets:
                    break

        finally:
            consumer.close()

        logger.info(f"{len(events)} late events consommés depuis Kafka")
        return events

    @task(task_id="validate_events")
    def validate_events(events: list) -> list:
        """
        Filtre les events invalides et les doublons déjà en base.

        Pourquoi valider ?
        Un late event peut avoir des champs manquants ou être déjà inséré
        lors d'une exécution précédente du DAG. L'idempotence est cruciale.
        """
        if not events:
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Récupérer les event_id déjà en base pour éviter les doublons
        existing_ids = set()
        if events:
            ids = tuple(e.get("event_id") for e in events if e.get("event_id"))
            if ids:
                rows = hook.get_records(
                    "SELECT id::text FROM listening_events WHERE id::text = ANY(%s)",
                    parameters=(list(ids),)
                )
                existing_ids = {r[0] for r in rows}

        valid = []
        for event in events:
            # Champs obligatoires
            if not all(event.get(f) for f in ["event_id", "user_id", "track_id", "event_time"]):
                logger.warning(f"Event ignoré — champs manquants : {event.get('event_id')}")
                continue
            # Doublon
            if event.get("event_id") in existing_ids:
                logger.info(f"Event déjà en base, ignoré : {event.get('event_id')}")
                continue
            valid.append(event)

        logger.info(f"{len(valid)}/{len(events)} events valides après filtrage")
        return valid

    @task(task_id="insert_into_listening_events")
    def insert_into_listening_events(events: list) -> list:
        """
        Insère les late events valides dans la table listening_events.

        Pourquoi ON CONFLICT DO NOTHING ?
        Idempotence — si le DAG tourne deux fois de suite, pas de doublons.
        Les dates affectées sont retournées pour la tâche suivante.
        """
        if not events:
            logger.info("Aucun event à insérer")
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        affected_dates = set()
        inserted = 0

        for event in events:
            try:
                event_time = event.get("event_time", "")
                date_part = event_time[:10] if event_time else None

                cursor.execute("""
                    INSERT INTO listening_events
                        (id, user_id, track_id, source_peer_id, timestamp,
                         duration_ms, device_type, geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event.get("event_id"),
                    event.get("user_id"),
                    event.get("track_id"),
                    event.get("source_peer"),
                    event.get("event_time"),
                    event.get("duration_ms"),
                    event.get("device_type"),
                    event.get("geo_country"),
                    event.get("completed", False),
                    event.get("event_source"),
                ))

                if date_part:
                    affected_dates.add(date_part)
                inserted += 1

            except Exception as e:
                logger.error(f"Erreur insertion event {event.get('event_id')}: {e}")
                continue

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"{inserted} events insérés dans listening_events")
        return list(affected_dates)

    @task(task_id="recalculate_daily_streams")
    def recalculate_daily_streams(affected_dates: list):
        """
        Recalcule les agrégats daily_streams pour les dates affectées.

        Pourquoi recalculer ?
        On vient d'insérer des events dans listening_events avec des timestamps
        passés. Les agrégats daily_streams de ces jours sont maintenant faux
        — il faut les mettre à jour.
        """
        if not affected_dates:
            logger.info("Aucune date affectée — pas de recalcul nécessaire")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        for date_str in affected_dates:
            hook.run("""
                INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms)
                SELECT
                    track_id,
                    DATE(timestamp) as date,
                    COUNT(*) as total_streams,
                    COUNT(DISTINCT user_id) as unique_listeners,
                    SUM(duration_ms) as total_duration_ms
                FROM listening_events
                WHERE DATE(timestamp) = %s AND completed = TRUE
                GROUP BY track_id, DATE(timestamp)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms
            """, parameters=(date_str,))
            logger.info(f"Agrégats recalculés pour {date_str}")

    # ── Orchestration ─────────────────────────────────────────
    late_events    = consume_late_events()
    valid_events   = validate_events(late_events)
    affected_dates = insert_into_listening_events(valid_events)
    recalculate_daily_streams(affected_dates)

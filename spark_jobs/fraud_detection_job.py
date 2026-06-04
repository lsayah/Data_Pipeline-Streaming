"""
Spark Job : fraud_detection_job
================================
Détecte en temps réel les comportements frauduleux sur la plateforme Spotify P2P.

3 règles de détection :
  - bot_stream     : > 100 écoutes en 10 min par user_id       (applyInPandasWithState)
  - short_duration : durée moyenne < 5 s sur 10 min par user_id (applyInPandasWithState)
  - p2p_failure    : taux de cache_miss > 50 % sur 15 min       (window agg + foreachBatch)

Architecture :
    Kafka "listening_events"
        → groupBy(user_id).applyInPandasWithState  → Rules 1 & 2
        → foreachBatch → Kafka "fraud_alerts" + PostgreSQL

    Kafka "p2p_network_events"
        → window agg 15 min + foreachBatch         → Rule 3
        → Kafka "fraud_alerts" + PostgreSQL

Lancement :
    docker exec data_pipeline-streaming-spark-master-1 /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/ivy2 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.postgresql:postgresql:42.7.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/spark-jobs/fraud_detection_job.py
"""

import json
import os
import pandas as pd
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType,
    DoubleType, LongType,
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
KAFKA_LISTENING = "listening_events"
KAFKA_P2P       = "p2p_network_events"
KAFKA_OUTPUT    = "fraud_alerts"
CHECKPOINT_BASE = "/tmp/spark-checkpoints/fraud_detection"
POSTGRES_URL    = os.getenv("SPOTIFY_POSTGRES_URL",
                            "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS  = {
    "user":     "spotify",
    "password": "spotify",
    "driver":   "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMAS
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  False),
    StructField("user_id",      StringType(),  False),
    StructField("track_id",     StringType(),  False),
    StructField("source_peer",  StringType(),  True),
    StructField("timestamp",    StringType(),  False),
    StructField("duration_ms",  IntegerType(), True),
    StructField("device_type",  StringType(),  True),
    StructField("geo_country",  StringType(),  True),
    StructField("completed",    BooleanType(), True),
    StructField("event_source", StringType(),  True),
])

P2P_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(), False),
    StructField("event_type",  StringType(), True),
    StructField("source_peer", StringType(), True),
    StructField("target_peer", StringType(), True),
    StructField("timestamp",   StringType(), False),
    StructField("data_bytes",  LongType(),   True),
])

# État par user_id (Rules 1 & 2)
LISTENING_STATE_SCHEMA = StructType([
    StructField("listen_count",    IntegerType(), True),
    StructField("sum_duration",    LongType(),    True),
    StructField("event_count",     IntegerType(), True),
    StructField("suspicion_score", DoubleType(),  True),
])

# Schéma de sortie des alertes
OUTPUT_SCHEMA = StructType([
    StructField("user_id",         StringType(),    True),
    StructField("fraud_type",      StringType(),    True),
    StructField("suspicion_score", DoubleType(),    True),
    StructField("window_start",    TimestampType(), True),
    StructField("window_end",      TimestampType(), True),
])

_EMPTY_OUTPUT = pd.DataFrame(
    columns=["user_id", "fraud_type", "suspicion_score", "window_start", "window_end"]
)


# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_listening_stream(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_LISTENING)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )
    return (
        raw
        .select(F.from_json(F.col("value").cast("string"),
                            LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast(TimestampType()))
        .drop("timestamp")
    )


def read_p2p_stream(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_P2P)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )
    return (
        raw
        .select(F.from_json(F.col("value").cast("string"),
                            P2P_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("p2p_event_time", F.col("timestamp").cast(TimestampType()))
        .drop("timestamp")
    )


# ─────────────────────────────────────────────────────────────
# RULES 1 & 2 — applyInPandasWithState sur stream brut
# ─────────────────────────────────────────────────────────────

def detect_listening_fraud(
    key: tuple,
    pdfs: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Appliqué sur le stream brut listening_events groupé par user_id.

    Rule 1 (bot_stream)     : listen_count > 100 dans la fenêtre d'état
    Rule 2 (short_duration) : avg(duration_ms) < 5 000 ms dans la fenêtre d'état

    Timeout 10 min : l'état est réinitialisé si l'utilisateur est inactif.
    """
    user_id = key[0]
    now = pd.Timestamp.now("UTC").tz_convert(None)

    if state.hasTimedOut:
        state.remove()
        yield _EMPTY_OUTPUT
        return

    # Récupérer l'état courant (state.get retourne un tuple positionnel)
    if state.exists:
        s          = state.get
        count      = int(s[0]) if s[0] is not None else 0
        sum_dur    = int(s[1]) if s[1] is not None else 0
        evt_count  = int(s[2]) if s[2] is not None else 0
        score      = float(s[3]) if s[3] is not None else 0.0
    else:
        count, sum_dur, evt_count, score = 0, 0, 0, 0.0

    # Agréger le batch entrant
    for pdf in pdfs:
        count += len(pdf)
        valid  = pdf[pdf["duration_ms"].notna()]
        sum_dur   += int(valid["duration_ms"].sum())
        evt_count += len(valid)

    alerts = []

    # Rule 1 : > 100 écoutes → bot_stream
    if count > 100:
        new_score = min(score + 0.5, 1.0)
        if new_score > score:
            score = new_score
            alerts.append({
                "user_id":         user_id,
                "fraud_type":      "bot_stream",
                "suspicion_score": score,
                "window_start":    now - pd.Timedelta(minutes=10),
                "window_end":      now,
            })

    # Rule 2 : durée moyenne < 5 s → short_duration
    if evt_count > 0 and (sum_dur / evt_count) < 5000:
        new_score = min(score + 0.3, 1.0)
        if new_score > score:
            score = new_score
            alerts.append({
                "user_id":         user_id,
                "fraud_type":      "short_duration",
                "suspicion_score": score,
                "window_start":    now - pd.Timedelta(hours=1),
                "window_end":      now,
            })

    # Mettre à jour l'état — tuple positionnel aligné sur LISTENING_STATE_SCHEMA
    state.update((count, sum_dur, evt_count, score))
    state.setTimeoutDuration(10 * 60 * 1000)

    yield pd.DataFrame(alerts) if alerts else _EMPTY_OUTPUT


# ─────────────────────────────────────────────────────────────
# SINK — Rules 1 & 2 (alertes issues de applyInPandasWithState)
# ─────────────────────────────────────────────────────────────

def write_listening_alerts(alerts_df):
    """Écrit les alertes bot_stream / short_duration dans Kafka et PostgreSQL."""

    def _write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        rows = batch_df.collect()

        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname="spotify", user="spotify", password="spotify",
        )
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO fraud_detections
                        (user_id, fraud_type, suspicion_score,
                         evidence, window_start, window_end)
                    VALUES (%s::uuid, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        (
                            r.user_id, r.fraud_type, r.suspicion_score,
                            json.dumps({"rule": r.fraud_type, "score": r.suspicion_score}),
                            r.window_start, r.window_end,
                        )
                        for r in rows
                    ],
                )
            conn.commit()
        finally:
            conn.close()

        (
            batch_df
            .select(F.to_json(F.struct("*")).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", KAFKA_OUTPUT)
            .save()
        )

    return (
        alerts_df
        .writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_BASE + "/listening")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


# ─────────────────────────────────────────────────────────────
# RULE 3 — p2p_failure via window agg + foreachBatch
# ─────────────────────────────────────────────────────────────

def write_p2p_fraud(p2p_df):
    """Rule 3 : taux cache_miss > 50 % sur 15 min → p2p_failure."""

    p2p_alerts = (
        p2p_df
        .withWatermark("p2p_event_time", "2 minutes")
        .groupBy(F.window("p2p_event_time", "15 minutes"), "source_peer")
        .agg(
            F.count("*").alias("total"),
            F.sum(
                F.when(F.col("event_type") == "cache_miss", 1).otherwise(0)
            ).alias("failures"),
        )
        .withColumn("failure_rate", F.col("failures") / F.col("total"))
        .filter(F.col("failure_rate") > 0.5)
    )

    def _write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        rows = batch_df.collect()

        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname="spotify", user="spotify", password="spotify",
        )
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO fraud_detections
                        (user_id, fraud_type, suspicion_score,
                         evidence, window_start, window_end)
                    VALUES (%s::uuid, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        (
                            r.source_peer, "p2p_failure", float(r.failure_rate),
                            json.dumps({"rule": "p2p_failure",
                                        "failure_rate": float(r.failure_rate)}),
                            r.window.start, r.window.end,
                        )
                        for r in rows
                    ],
                )
            conn.commit()
        finally:
            conn.close()

        alert_df = batch_df.select(
            F.col("source_peer").alias("user_id"),
            F.lit("p2p_failure").alias("fraud_type"),
            F.col("failure_rate").alias("suspicion_score"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
        (
            alert_df
            .select(F.to_json(F.struct("*")).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", KAFKA_OUTPUT)
            .save()
        )

    return (
        p2p_alerts
        .writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_BASE + "/p2p")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage fraud_detection_job...")
    print(f"Kafka input  : {KAFKA_LISTENING} + {KAFKA_P2P}")
    print(f"Kafka output : {KAFKA_OUTPUT}")
    print(f"Checkpoint   : {CHECKPOINT_BASE}")

    listening_df = read_listening_stream(spark)
    p2p_df       = read_p2p_stream(spark)

    # Rules 1 & 2 — applyInPandasWithState sur stream brut
    alerts_df = (
        listening_df
        .groupBy("user_id")
        .applyInPandasWithState(
            detect_listening_fraud,
            OUTPUT_SCHEMA,
            LISTENING_STATE_SCHEMA,
            "append",
            GroupStateTimeout.ProcessingTimeTimeout,
        )
    )

    write_listening_alerts(alerts_df)   # Rules 1 & 2
    write_p2p_fraud(p2p_df)             # Rule 3

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

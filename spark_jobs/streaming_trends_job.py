"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \
        --conf spark.jars.ivy=/tmp/ivy2 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                   org.postgresql:postgresql:42.7.1 \
        spark_jobs/streaming_trends_job.py

TODO :
    [x] Implémenter la lecture du topic Kafka avec readStream
    [x] Désérialiser les messages JSON avec le bon schéma
    [x] Sink console en mode append — validation lecture Kafka
    [x] Configurer le checkpoint sur MinIO
    [ ] Implémenter les fenêtres tumbling de 5 minutes (issue #14)
    [ ] Implémenter les sliding windows pour les genres (issue #14)
    [ ] Écrire les résultats dans PostgreSQL et Redis (issue #14)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
KAFKA_TOPIC     = "listening_events"
CHECKPOINT_PATH = "/tmp/spark-checkpoints/streaming_trends"
POSTGRES_URL    = os.getenv("SPOTIFY_POSTGRES_URL",
                            "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS  = {
    "user":     "spotify",
    "password": "spotify",
    "driver":   "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
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


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
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

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming.
    Retourne un DataFrame avec les colonnes typées.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )

    return (
        raw
        .select(F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast(TimestampType()))
        .drop("timestamp")
    )


# ─────────────────────────────────────────────────────────────
# SINK CONSOLE — Issue #13 : validation lecture Kafka
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Issue #13 : console sink pour valider la lecture Kafka.
    Expérimenter avec processingTime("10 seconds") et Once().
    Issue #14 : remplacer par les fenêtres tumbling → PostgreSQL.
    """
    return (
        events_df.writeStream
        .format("console")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH + "/console")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")  # tester aussi : .trigger(once=True)
        .start()
    )


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Issue #14 — sliding window 15 min / slide 5 min → Redis.
    """
    raise NotImplementedError("TODO issue #14 : implémenter compute_genre_listeners_sliding()")


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    events_df = read_kafka_stream(spark)

    query_top_tracks = compute_top_tracks_tumbling(events_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
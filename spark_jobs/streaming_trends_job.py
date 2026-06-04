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
REDIS_HOST      = os.getenv("REDIS_HOST", "redis")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))

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
        .withWatermark("event_time", "10 minutes")
    )


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING — Issue #14
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top tracks par fenêtre fixe de 5 minutes → PostgreSQL.

    Pourquoi tumbling window ?
    On découpe le temps en tranches fixes non chevauchantes : [0h00-0h05], [0h05-0h10]...
    Chaque tranche calcule le classement indépendamment.
    """
    windowed = (
        events_df
        .groupBy(F.window("event_time", "5 minutes"), "track_id")
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners"),
        )
    )

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        import psycopg2
        rows = (
            batch_df
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "track_id",
                "stream_count",
                "unique_listeners",
            )
            .collect()
        )
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname="spotify", user="spotify", password="spotify",
        )
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO realtime_top_tracks
                        (window_start, window_end, track_id, stream_count, unique_listeners)
                    VALUES (%s, %s, %s::uuid, %s, %s)
                    ON CONFLICT (window_start, track_id) DO UPDATE SET
                        stream_count     = EXCLUDED.stream_count,
                        unique_listeners = EXCLUDED.unique_listeners,
                        updated_at       = NOW()
                    """,
                    (row.window_start, row.window_end, row.track_id,
                     row.stream_count, row.unique_listeners),
                )
        conn.commit()
        conn.close()

    return (
        windowed
        .writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "/top_tracks")
        .start()
    )


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min / slide 5 min) → Redis.

    Pourquoi sliding window ?
    Toutes les 5 min on recalcule sur les 15 dernières minutes.
    Utile pour avoir un "top genres en ce moment" qui se met à jour en continu.
    """
    enriched = events_df.join(
        catalog_df.select("id", "genre"),
        F.col("track_id") == F.col("id"),
        "left"
    )

    windowed = (
        enriched
        .groupBy(F.window("event_time", "15 minutes", "5 minutes"), "genre")
        .agg(F.approx_count_distinct("user_id").alias("unique_listeners"))
    )

    def write_to_redis(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        import redis, json
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True)
        rows = batch_df.collect()
        result = {row["genre"]: row["unique_listeners"] for row in rows if row["genre"]}
        if result:
            try:
                r.set("genre_listeners:live", json.dumps(result))
            except redis.RedisError as e:
                print(f"Redis write error (non-bloquant): {e}")

    return (
        windowed
        .writeStream
        .outputMode("update")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_PATH + "/genre_listeners")
        .start()
    )


# ─────────────────────────────────────────────────────────────
# ROUTAGE LATE EVENTS — Issue #15
# ─────────────────────────────────────────────────────────────

def route_late_events(events_df):
    """
    Détecte les events arrivés avec plus de 10 min de retard et les route
    vers le topic late_listening_events pour retraitement Airflow plus tard.
    """
    late_events = events_df.filter(
        F.col("event_time") < (F.current_timestamp() - F.expr("INTERVAL 10 MINUTES"))
    )

    return (
        late_events
        .select(F.to_json(F.struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "late_listening_events")
        .option("checkpointLocation", CHECKPOINT_PATH + "/late_events")
        .outputMode("append")
        .start()
    )


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

    # Catalogue statique pour la jointure genre (chargé une seule fois au démarrage)
    catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)

    query_top_tracks = compute_top_tracks_tumbling(events_df)
    query_genres     = compute_genre_listeners_sliding(events_df, catalog_df)
    query_late       = route_late_events(events_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
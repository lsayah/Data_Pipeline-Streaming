"""

Architecture :
    Kafka "listening_events"  ──┐
                                ├──→ stream-static join (catalogue PG)
    PostgreSQL (tracks/artists) ┘       ↓
                                    stream-stream join (watermark 2min)
    Kafka "p2p_network_events"  ────────┘
                                    ↓
                            dropDuplicates(event_id)
                                    ↓
                    ┌───────────────┴───────────────┐
                    ↓                               ↓
            Kafka "enriched_events"        MinIO Parquet partitionné
                                           date/hour

Lancement :
docker exec data_pipeline-streaming-spark-master-1 /opt/spark/bin/spark-submit `
    --conf spark.jars.ivy=/tmp/ivy2 `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 `
    /opt/spark-jobs/streaming_enrichment_job.py


"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, LongType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
KAFKA_INPUT     = "listening_events"
KAFKA_P2P       = "p2p_network_events"
KAFKA_OUTPUT    = "enriched_events"
CHECKPOINT_BASE = "/tmp/spark-checkpoints/streaming_enrichment"
PARQUET_PATH    = "s3a://spotify-parquet/enriched_events"
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


# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-enrichment")
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
    """Lit le topic listening_events depuis Kafka."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_INPUT)
        .option("startingOffsets", "earliest")
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
    """Lit le topic p2p_network_events depuis Kafka."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_P2P)
        .option("startingOffsets", "earliest")
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
# CATALOGUE POSTGRESQL (jointure statique)
# ─────────────────────────────────────────────────────────────

def load_catalog(spark: SparkSession):
    """Charge tracks + artists depuis PostgreSQL (DataFrame statique)."""
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable",
                "(SELECT t.id::text AS track_id, t.title, t.genre, "
                "a.name AS artist_name "
                "FROM tracks t LEFT JOIN artists a ON t.artist_id = a.id) catalog")
        .options(**POSTGRES_PROPS)
        .load()
    )


# ─────────────────────────────────────────────────────────────
# ENRICHISSEMENT
# ─────────────────────────────────────────────────────────────

def enrich_stream(listening_df, catalog_df, p2p_df):
    """
    1. Stream-static join  : listening × catalogue (track → artiste, genre)
    2. Stream-stream join  : résultat × p2p (watermark 2 min)
    3. Déduplication       : dropDuplicates sur event_id
    """
    # 1. Jointure avec le catalogue (broadcast car DataFrame statique)
    enriched = listening_df.join(
        F.broadcast(catalog_df),
        listening_df.track_id == catalog_df.track_id,
        "left",
    ).drop(catalog_df.track_id)

    # 2. Watermarks pour la jointure stream-stream
    # Renommer les colonnes P2P pour éviter l'ambiguïté avec listening
    enriched_wm = enriched.withWatermark("event_time", "2 minutes")
    p2p_wm = (
        p2p_df
        .withColumnRenamed("source_peer", "p2p_source_peer")
        .withColumnRenamed("target_peer", "p2p_target_peer")
        .withColumnRenamed("event_id",    "p2p_event_id")
        .withWatermark("p2p_event_time", "2 minutes")
    )

    joined = enriched_wm.join(
        p2p_wm,
        expr(
            "source_peer = p2p_source_peer AND "
            "p2p_event_time BETWEEN event_time - INTERVAL 2 MINUTES "
            "AND event_time + INTERVAL 2 MINUTES"
        ),
        "left",
    )

    # 3. Déduplication par event_id
    # On supprime p2p_event_time (peut être NULL) pour garder une seule colonne
    # event time (event_time, toujours présent car timestamp obligatoire côté listening)
    return joined.drop("p2p_event_time").dropDuplicates(["event_id"])


# ─────────────────────────────────────────────────────────────
# SINKS
# ─────────────────────────────────────────────────────────────

def write_to_kafka(enriched_df):
    """Écrit les events enrichis dans le topic Kafka enriched_events."""
    return (
        enriched_df
        .select(F.to_json(F.struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", KAFKA_OUTPUT)
        .option("checkpointLocation", CHECKPOINT_BASE + "/kafka")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


def write_to_parquet(enriched_df):
    """Écrit en Parquet sur MinIO partitionné par date et heure."""

    def _write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df
            .withColumn("date", F.date_format("event_time", "yyyy-MM-dd"))
            .withColumn("hour", F.date_format("event_time", "HH"))
            .write
            .mode("append")
            .partitionBy("date", "hour")
            .parquet(PARQUET_PATH)
        )

    return (
        enriched_df
        .writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_BASE + "/parquet")
        .trigger(processingTime="10 seconds")
        .start()
    )


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_enrichment_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Input  : {KAFKA_INPUT} + {KAFKA_P2P}")
    print(f"Output : {KAFKA_OUTPUT} + {PARQUET_PATH}")

    catalog_df   = load_catalog(spark)
    listening_df = read_listening_stream(spark)
    p2p_df       = read_p2p_stream(spark)

    enriched_df  = enrich_stream(listening_df, catalog_df, p2p_df)

    write_to_kafka(enriched_df)
    write_to_parquet(enriched_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

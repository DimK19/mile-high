import os
import json
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    count,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


# -----------------------------
# 1) Config (env-driven)
# -----------------------------

# Updated spark_job.py Config section
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_IN", "traffic-events") # Changed to KAFKA_TOPIC_IN

POSTGRES_HOST = os.getenv("PG_HOST", "postgres")           # Changed to PG_HOST
POSTGRES_PORT = int(os.getenv("PG_PORT", "5432"))          # Changed to PG_PORT
POSTGRES_DB = os.getenv("PG_DB", "traffic")                # Changed to PG_DB
POSTGRES_USER = os.getenv("PG_USER", "traffic")
POSTGRES_PASSWORD = os.getenv("PG_PASSWORD", "traffic")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/traffic-5m")


# -----------------------------
# 2) Input schema (matches your traffic-events JSON)
# -----------------------------

traffic_schema = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("chunk_id", StringType(), True),
        StructField("source_video_id", StringType(), True),
        StructField("chunk_index", IntegerType(), True),
        StructField("track_id", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("avg_speed_kmh", DoubleType(), True),
        StructField("max_speed_kmh", DoubleType(), True),
    ]
)


# -----------------------------
# 3) Postgres upsert helper (foreachBatch)
# -----------------------------

def upsert_window_stats(batch_df, batch_id: int) -> None:
    """
    Write one micro-batch of aggregated 5-minute stats into Postgres using UPSERT.
    """
    rows = (
        batch_df.select(
            col("window_start_utc"),
            col("window_end_utc"),
            col("direction"),
            col("vehicle_count"),
            col("avg_speed_kmh"),
        )
        .collect()
    )

    # If there is no data in this microbatch, do nothing.
    if not rows:
        return

    values = [
        (
            r["window_start_utc"],
            r["window_end_utc"],
            r["direction"],
            int(r["vehicle_count"]),
            float(r["avg_speed_kmh"]) if r["avg_speed_kmh"] is not None else None,
        )
        for r in rows
    ]

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO window_stats_5m
                    (window_start_utc, window_end_utc, direction, vehicle_count, avg_speed_kmh)
                VALUES %s
                ON CONFLICT (window_start_utc, direction)
                DO UPDATE SET
                    window_end_utc = EXCLUDED.window_end_utc,
                    vehicle_count  = EXCLUDED.vehicle_count,
                    avg_speed_kmh   = EXCLUDED.avg_speed_kmh,
                    created_utc     = NOW();
                """,
                values,
            )
        conn.commit()
    finally:
        conn.close()


# -----------------------------
# 4) Spark job
# -----------------------------

def main() -> None:
    spark = (
        SparkSession.builder.appName("traffic-analytics-5m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Read Kafka topic as streaming source
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka value is bytes -> cast to string -> parse JSON using schema
    parsed = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), traffic_schema).alias("e"))
        .select("e.*")
    )

    # Keep only your summary events and create an event-time timestamp column
    events = (
        parsed.filter(col("event_type") == "vehicle_track_summary")
        .withColumn("event_time", to_timestamp(col("timestamp_utc")))
        .filter(col("event_time").isNotNull())
    )

    # 5-minute tumbling window aggregates
    agg_5m = (
        events.withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes").alias("w"),
            col("direction"),
        )
        .agg(
            count("*").alias("vehicle_count"),
            avg(col("avg_speed_kmh")).alias("avg_speed_kmh"),
        )
        .select(
            col("w.start").alias("window_start_utc"),
            col("w.end").alias("window_end_utc"),
            col("direction"),
            col("vehicle_count"),
            col("avg_speed_kmh"),
        )
    )

    query = (
        agg_5m.writeStream.foreachBatch(upsert_window_stats)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

"""
Spark Structured Streaming job — cold path only.

Reads mta.trip_updates, mta.vehicle_positions, and mta.alerts from Kafka,
parses each message, classifies rows as valid or rejected, writes valid events
to S3 Parquet partitioned by dt=YYYY-MM-DD, and quarantines rejected rows with
a rejection_reason column for observability.

No aggregation — all analysis is done downstream in Snowflake.

Submit:
    spark-submit src/transform/spark_streaming.py
"""
import logging
import os
import sys
from typing import Callable

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv()

from config.settings import Config
from src.transform.schemas import ALERTS_SCHEMA, TRIP_UPDATE_SCHEMA, VEHICLE_POSITION_SCHEMA

logger = logging.getLogger(__name__)

# delay bounds for trip_updates — outside this range is a feed artifact
_DELAY_MIN_SECONDS = -3600   # 1 hour early
_DELAY_MAX_SECONDS = 7200    # 2 hours late


def build_spark(cfg: Config) -> SparkSession:
    return (
        SparkSession.builder.appName("mta-reliability-streaming")
        # local[4]: gives catch-up batches write parallelism; writer is I/O-bound not CPU-bound
        .master("local[4]")
        # 4g headroom on 16GB — not a throughput gain; job is I/O-bound, never memory-bound
        .config("spark.driver.memory", "4g")
        # no groupBy => no shuffle, but cap partition count if one ever appears
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        # free insurance: coalesces post-shuffle partitions automatically
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", cfg.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap: str, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        # 50k: ~10-20x steady-state (~2.4-4.8k/batch); drain speed comes from batch
        # repetition not batch size — full-day backlog clears in minutes over back-to-back batches
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )


def parse_events(raw_df, schema: StructType):
    # parse only — no filtering here; classification happens in foreachBatch
    return (
        raw_df.select(
            F.from_json(F.col("value").cast("string"), schema).alias("d")
        )
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("dt", F.date_format("event_time", "yyyy-MM-dd"))
    )


def _classify_trip_updates(df: DataFrame) -> DataFrame:
    """Adds rejection_reason column. NULL = valid row."""
    return df.withColumn(
        "rejection_reason",
        F.when(F.col("event_time").isNull(), F.lit("null_event_time"))
         .when(
             F.col("delay_seconds").isNotNull() &
             ~F.col("delay_seconds").between(_DELAY_MIN_SECONDS, _DELAY_MAX_SECONDS),
             F.lit("delay_out_of_range"),
         )
         .otherwise(F.lit(None).cast(StringType())),
    )


def _classify_default(df: DataFrame) -> DataFrame:
    """Adds rejection_reason column for topics without delay_seconds."""
    return df.withColumn(
        "rejection_reason",
        F.when(F.col("event_time").isNull(), F.lit("null_event_time"))
         .otherwise(F.lit(None).cast(StringType())),
    )


def make_s3_writer(
    s3_path: str,
    quarantine_path: str,
    label: str,
    classify_fn: Callable[[DataFrame], DataFrame],
):
    """Returns a foreachBatch callback that classifies, writes valid rows to
    s3_path, and quarantines rejected rows to quarantine_path with a
    rejection_reason column."""

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        # single classification pass — cache so valid + rejected filters don't re-scan
        classified = classify_fn(
            batch_df.withColumn("ingested_at", F.current_timestamp())
        ).cache()

        valid_df = (
            classified
            .filter(F.col("rejection_reason").isNull())
            .drop("rejection_reason", "ingested_at")
        )
        rejected_df = classified.filter(F.col("rejection_reason").isNotNull())

        # coalesce(1): one file per topic per trigger — kills small-file problem at source
        # safe at 2-min trigger cadence where batch size fits in a single writer task
        valid_df.coalesce(1).write.format("parquet").mode("append").partitionBy("dt").save(s3_path)
        rejected_df.coalesce(1).write.format("parquet").mode("append").partitionBy("dt").save(quarantine_path)

        # counts hit warm cache
        rejected_count = rejected_df.count()
        total_count = classified.count()
        classified.unpersist()

        logger.info(
            "%s batch_id=%d total=%d valid=%d rejected=%d",
            label, batch_id, total_count, total_count - rejected_count, rejected_count,
        )

    return write_batch


def run(cfg: Config) -> None:
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    trip_df = parse_events(
        read_kafka_stream(spark, cfg.kafka_bootstrap_servers, cfg.kafka_topic_trip_updates),
        TRIP_UPDATE_SCHEMA,
    )
    trip_query = (
        trip_df.writeStream
        .foreachBatch(make_s3_writer(
            f"s3a://{cfg.s3_bucket}/raw/trip_updates/",
            f"s3a://{cfg.s3_bucket}/raw/quarantine_trip_updates/",
            "trip_updates",
            _classify_trip_updates,
        ))
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold/trip_updates")
        # 2-min trigger: bundles 4-8 polls per write, cuts file count ~5x with zero analytical loss
        .trigger(processingTime="2 minutes")
        .outputMode("append")
        .start()
    )

    vp_df = parse_events(
        read_kafka_stream(spark, cfg.kafka_bootstrap_servers, cfg.kafka_topic_vehicle_positions),
        VEHICLE_POSITION_SCHEMA,
    )
    vp_query = (
        vp_df.writeStream
        .foreachBatch(make_s3_writer(
            f"s3a://{cfg.s3_bucket}/raw/vehicle_positions/",
            f"s3a://{cfg.s3_bucket}/raw/quarantine_vehicle_positions/",
            "vehicle_positions",
            _classify_default,
        ))
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold/vehicle_positions")
        # 2-min trigger: bundles 4-8 polls per write, cuts file count ~5x with zero analytical loss
        .trigger(processingTime="2 minutes")
        .outputMode("append")
        .start()
    )

    al_df = parse_events(
        read_kafka_stream(spark, cfg.kafka_bootstrap_servers, cfg.kafka_topic_alerts),
        ALERTS_SCHEMA,
    )
    al_query = (
        al_df.writeStream
        .foreachBatch(make_s3_writer(
            f"s3a://{cfg.s3_bucket}/raw/alerts/",
            f"s3a://{cfg.s3_bucket}/raw/quarantine_alerts/",
            "alerts",
            _classify_default,
        ))
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold/alerts")
        # 2-min trigger: bundles 4-8 polls per write, cuts file count ~5x with zero analytical loss
        .trigger(processingTime="2 minutes")
        .outputMode("append")
        .start()
    )

    logger.info("Streaming queries started — trip_updates, vehicle_positions, alerts → S3.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(Config())

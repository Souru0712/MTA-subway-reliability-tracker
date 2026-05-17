"""
Spark Structured Streaming job — cold path only.

Reads mta.trip_updates, mta.vehicle_positions, and mta.alerts from Kafka,
parses each message, and writes raw events to S3 Parquet partitioned by dt=YYYY-MM-DD.

No aggregation — all analysis is done downstream in Snowflake.

Submit:
    spark-submit src/transform/spark_streaming.py
"""
import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv()

from config.settings import Config
from src.transform.schemas import ALERTS_SCHEMA, TRIP_UPDATE_SCHEMA, VEHICLE_POSITION_SCHEMA

logger = logging.getLogger(__name__)


def build_spark(cfg: Config) -> SparkSession:
    return (
        SparkSession.builder.appName("mta-reliability-streaming")
        # no shuffle partitions config — no groupBy means no shuffle
        .config("spark.hadoop.fs.s3a.access.key", cfg.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap: str, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )


def parse_events(raw_df, schema: StructType):
    return (
        raw_df.select(
            F.from_json(F.col("value").cast("string"), schema).alias("d")
        )
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("dt", F.date_format("event_time", "yyyy-MM-dd"))
        .filter(F.col("event_time").isNotNull())  # drop malformed messages
    )


def make_s3_writer(s3_path: str, label: str):
    """Returns a foreachBatch callback that writes to S3 and logs progress."""
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        count = batch_df.count()
        batch_df.write.format("parquet").mode("append").partitionBy("dt").save(s3_path)
        logger.info("%s batch_id=%d wrote %d rows to %s", label, batch_id, count, s3_path)
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
        .foreachBatch(make_s3_writer(f"s3a://{cfg.s3_bucket}/raw/trip_updates/", "trip_updates"))
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold/trip_updates")
        .outputMode("append")
        .start()
    )

    vp_df = parse_events(
        read_kafka_stream(spark, cfg.kafka_bootstrap_servers, cfg.kafka_topic_vehicle_positions),
        VEHICLE_POSITION_SCHEMA,
    )
    vp_query = (
        vp_df.writeStream
        .foreachBatch(make_s3_writer(f"s3a://{cfg.s3_bucket}/raw/vehicle_positions/", "vehicle_positions"))
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold/vehicle_positions")
        .outputMode("append")
        .start()
    )

    al_df = parse_events(
        read_kafka_stream(spark, cfg.kafka_bootstrap_servers, cfg.kafka_topic_alerts),
        ALERTS_SCHEMA,
    )
    al_query = (
        al_df.writeStream
        .foreachBatch(make_s3_writer(f"s3a://{cfg.s3_bucket}/raw/alerts/", "alerts"))
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold/alerts")
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

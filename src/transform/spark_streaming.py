"""
Spark Structured Streaming job — cold path only.

Reads mta.trip_updates from Kafka, parses each message, and writes
raw events to S3 Parquet partitioned by dt=YYYY-MM-DD.

No aggregation — all analysis is done downstream in Snowflake.

Submit:
    spark-submit src/transform/spark_streaming.py
"""
import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv()

from config.settings import Config
from src.transform.schemas import TRIP_UPDATE_SCHEMA

logger = logging.getLogger(__name__)


def build_spark(cfg: Config) -> SparkSession:
    return (
        SparkSession.builder.appName("mta-reliability-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.access.key", cfg.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, cfg: Config):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_topic_trip_updates)
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
    )


def run(cfg: Config) -> None:
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_kafka_stream(spark, cfg)
    parsed_df = parse_events(raw_df, TRIP_UPDATE_SCHEMA)

    # Cold path: raw parsed events → S3 Parquet, partitioned by dt
    cold_query = (
        parsed_df.writeStream.format("parquet")
        .option("path", f"s3a://{cfg.s3_bucket}/raw/trip_updates/")
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold")
        .partitionBy("dt")
        .outputMode("append")
        .start()
    )

    logger.info("Streaming query started. Writing raw events to S3.")
    cold_query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(Config())

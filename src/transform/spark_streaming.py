"""
Spark Structured Streaming job.

Reads mta.trip_updates from Kafka, applies a 2-min watermark and 5-min tumbling
windows, then fans out to:
  - Hot path: windowed reliability aggregates → Postgres (via foreachBatch)
  - Cold path: raw parsed events → S3 Parquet partitioned by dt=YYYY-MM-DD

Submit:
    spark-submit \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      src/transform/spark_streaming.py
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
from src.load.postgres_writer import write_aggregates_to_postgres
from src.transform.schemas import TRIP_UPDATE_SCHEMA

logger = logging.getLogger(__name__)


def build_spark(cfg: Config) -> SparkSession:
    return (
        SparkSession.builder.appName("mta-reliability-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.hadoop.fs.s3a.access.key", cfg.aws_access_key_id
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key", cfg.aws_secret_access_key
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com"
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, cfg: Config):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_topic_trip_updates)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
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


def build_aggregates(parsed_df):
    return (
        parsed_df.withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "5 minutes").alias("window"),
            F.col("route_id"),
            F.col("stop_id"),
        )
        .agg(
            F.percentile_approx("delay_seconds", 0.5).alias("delay_p50_seconds"),
            F.percentile_approx("delay_seconds", 0.95).alias("delay_p95_seconds"),
            # on_time_pct: fraction where |delay| <= 300s
            F.avg(
                F.when(F.abs("delay_seconds") <= 300, 1).otherwise(0)
            ).alias("on_time_pct"),
            F.count("*").alias("sample_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "route_id",
            "stop_id",
            "on_time_pct",
            "delay_p50_seconds",
            "delay_p95_seconds",
            "sample_count",
        )
    )


def run(cfg: Config) -> None:
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_kafka_stream(spark, cfg)
    parsed_df = parse_events(raw_df, TRIP_UPDATE_SCHEMA)
    agg_df = build_aggregates(parsed_df)

    # Hot path: aggregates → Postgres
    hot_query = (
        agg_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_aggregates_to_postgres(
                batch_df, batch_id, cfg
            )
        )
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/hot")
        .outputMode("update")
        .start()
    )

    # Cold path: raw parsed events → S3 Parquet, partitioned by dt
    cold_query = (
        parsed_df.writeStream.format("parquet")
        .option("path", f"s3a://{cfg.s3_bucket}/raw/trip_updates/")
        .option("checkpointLocation", cfg.spark_checkpoint_base + "/cold")
        .partitionBy("dt")
        .outputMode("append")
        .start()
    )

    logger.info("Streaming queries started. Awaiting termination.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(Config())

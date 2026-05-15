"""Quick test — batch read from Kafka to verify connection and data."""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kafka-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
print(f"Connecting to: {bootstrap}")

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", "mta.trip_updates")
    .option("startingOffsets", """{"mta.trip_updates":{"0":0}}""")
    .option("endingOffsets", """{"mta.trip_updates":{"0":10}}""")
    .load()
)

count = df.count()
print(f"Messages read (first 10): {count}")
spark.stop()

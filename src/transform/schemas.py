from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Schema for mta.trip_updates messages
TRIP_UPDATE_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("feed_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("arrival_time", LongType(), True),
        StructField("departure_time", LongType(), True),
        StructField("delay_seconds", LongType(), True),
    ]
)

# Schema for mta.vehicle_positions messages
VEHICLE_POSITION_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("feed_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("current_status", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

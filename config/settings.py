from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Config:
    # MTA
    mta_feed_urls: list[str] = field(
        default_factory=lambda: os.environ["MTA_FEED_URLS"].split(",")
    )
    mta_static_gtfs_url: str = field(
        default_factory=lambda: os.getenv(
            "MTA_STATIC_GTFS_URL",
            "http://web.mta.info/developers/data/nyct/subway/google_transit.zip",
        )
    )

    # Kafka
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_topic_trip_updates: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_TRIP_UPDATES", "mta.trip_updates")
    )
    kafka_topic_vehicle_positions: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_VEHICLE_POSITIONS", "mta.vehicle_positions")
    )
    kafka_topic_alerts: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_ALERTS", "mta.alerts")
    )

    # S3 / AWS
    aws_access_key_id: str = field(
        default_factory=lambda: os.environ["AWS_ACCESS_KEY_ID"]
    )
    aws_secret_access_key: str = field(
        default_factory=lambda: os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    aws_default_region: str = field(
        default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )
    s3_bucket: str = field(
        default_factory=lambda: os.environ["S3_BUCKET"]
    )
    spark_checkpoint_base: str = field(
        default_factory=lambda: os.environ["SPARK_CHECKPOINT_BASE"]
    )

    # Snowflake
    snowflake_account: str = field(
        default_factory=lambda: os.environ["SNOWFLAKE_ACCOUNT"]
    )
    snowflake_user: str = field(
        default_factory=lambda: os.environ["SNOWFLAKE_USER"]
    )
    snowflake_password: str = field(
        default_factory=lambda: os.environ["SNOWFLAKE_PASSWORD"]
    )
    snowflake_warehouse: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    )
    snowflake_database: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", "MTA")
    )
    snowflake_role: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER")
    )
    snowflake_stage: str = "RAW.S3_STAGE"
    snowflake_raw_table: str = "RAW.MTA_EVENTS"

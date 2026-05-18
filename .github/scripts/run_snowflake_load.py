"""
GitHub Actions entrypoint for daily S3 → Snowflake load.
Reads DS from environment (set by workflow_dispatch input or defaults to yesterday).
Loads trip_updates, vehicle_positions, and alerts partitions for the given date.
"""
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import boto3

from src.load.snowflake_loader import copy_from_s3, ensure_tables, get_connection

TOPICS = ["trip_updates", "vehicle_positions", "alerts"]


@dataclass
class SnowflakeConfig:
    snowflake_account: str = os.environ["SNOWFLAKE_ACCOUNT"]
    snowflake_user: str = os.environ["SNOWFLAKE_USER"]
    snowflake_password: str = os.environ["SNOWFLAKE_PASSWORD"]
    snowflake_warehouse: str = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    snowflake_database: str = os.environ.get("SNOWFLAKE_DATABASE", "MTA")
    snowflake_role: str = os.environ.get("SNOWFLAKE_ROLE", "MTA_TRANSFORMER")
    snowflake_stage: str = "RAW.S3_STAGE"


def partition_exists(bucket: str, prefix: str) -> bool:
    client = boto3.client("s3")
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def main() -> None:
    ds = os.environ.get("DS") or (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    print(f"Loading S3 partitions for dt={ds}")

    bucket = os.environ["S3_BUCKET"]
    cfg = SnowflakeConfig()
    conn = get_connection(cfg)

    try:
        ensure_tables(conn)

        any_loaded = False
        for topic in TOPICS:
            s3_check_prefix = f"raw/{topic}/dt={ds}/"   # full path from bucket root
            stage_prefix = f"{topic}/dt={ds}/"           # relative to stage URL (s3://bucket/raw/)
            if not partition_exists(bucket, s3_check_prefix):
                print(f"No S3 objects at s3://{bucket}/{s3_check_prefix} — skipping.")
                continue
            rows = copy_from_s3(conn, topic, stage_prefix, cfg)
            if rows == 0:
                print(f"WARNING: COPY INTO loaded 0 rows from {prefix}")
            else:
                print(f"Loaded {rows} rows from {prefix}")
                any_loaded = True

        if not any_loaded:
            print("No data loaded for any topic — check S3 and Spark job.")
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()

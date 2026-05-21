"""
GitHub Actions entrypoint for daily S3 → Snowflake load.

Default mode: rolling 7-day window — attempts every day from today-7 to yesterday.
COPY INTO is file-level idempotent (Snowflake load history), so days already loaded
are skipped automatically. Missed Spark runs self-heal on the next cron execution
as long as the gap is within the 7-day Kafka retention window.

Override: set DS env var to a specific date (YYYY-MM-DD) to load a single day only.
"""
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import boto3

from src.load.snowflake_loader import copy_from_s3, ensure_tables, get_connection

TOPICS = ["trip_updates", "vehicle_positions", "alerts"]
QUARANTINE_TOPICS = ["quarantine_trip_updates", "quarantine_vehicle_positions", "quarantine_alerts"]
ROLLING_WINDOW_DAYS = 7   # matches Kafka retention — no point scanning beyond this


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


def dates_to_load() -> list[str]:
    """Return list of dates to attempt. Single date if DS is set, else rolling window."""
    ds = os.environ.get("DS")
    if ds:
        return [ds]
    today = datetime.now(timezone.utc).date()
    return [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, ROLLING_WINDOW_DAYS + 1)  # yesterday back to today-7
    ]


def load_date(conn, bucket: str, cfg: SnowflakeConfig, ds: str) -> bool:
    """Load all topics for a single date. Returns True if any rows were loaded."""
    any_loaded = False

    for topic in TOPICS:
        s3_check_prefix = f"raw/{topic}/dt={ds}/"
        stage_prefix = f"{topic}/dt={ds}/"
        if not partition_exists(bucket, s3_check_prefix):
            continue
        rows = copy_from_s3(conn, topic, stage_prefix, cfg)
        if rows == 0:
            print(f"  {topic} dt={ds}: already loaded or empty")
        else:
            print(f"  {topic} dt={ds}: loaded {rows} rows")
            any_loaded = True

    for topic in QUARANTINE_TOPICS:
        s3_check_prefix = f"raw/{topic}/dt={ds}/"
        stage_prefix = f"{topic}/dt={ds}/"
        if not partition_exists(bucket, s3_check_prefix):
            continue
        rows = copy_from_s3(conn, topic, stage_prefix, cfg)
        if rows > 0:
            print(f"  quarantine {topic} dt={ds}: loaded {rows} rows")

    return any_loaded


def main() -> None:
    dates = dates_to_load()
    bucket = os.environ["S3_BUCKET"]
    cfg = SnowflakeConfig()
    conn = get_connection(cfg)

    print(f"Rolling window: checking {len(dates)} date(s): {dates[0]} → {dates[-1]}")

    try:
        ensure_tables(conn)

        total_loaded = 0
        for ds in dates:
            if load_date(conn, bucket, cfg, ds):
                total_loaded += 1

        if total_loaded == 0:
            print("No new data loaded across all dates — Spark may not have run recently.")
            sys.exit(1)

        print(f"Done. Loaded new data for {total_loaded}/{len(dates)} date(s).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

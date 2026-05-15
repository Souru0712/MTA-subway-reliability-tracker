"""
GitHub Actions entrypoint for daily S3 → Snowflake load.
Reads DS from environment (set by workflow_dispatch input or defaults to yesterday).
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.load.snowflake_loader import copy_from_s3, ensure_table, get_connection


def main() -> None:
    ds = os.environ.get("DS") or (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    print(f"Loading S3 partition: dt={ds}")

    bucket = os.environ["S3_BUCKET"]
    prefix = f"raw/trip_updates/dt={ds}/"

    import boto3
    client = boto3.client("s3")
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        print(f"No S3 objects found at s3://{bucket}/{prefix} — skipping.")
        sys.exit(0)

    # build a minimal config — only Snowflake fields needed here
    from dataclasses import dataclass

    @dataclass
    class SnowflakeConfig:
        snowflake_account: str = os.environ["SNOWFLAKE_ACCOUNT"]
        snowflake_user: str = os.environ["SNOWFLAKE_USER"]
        snowflake_password: str = os.environ["SNOWFLAKE_PASSWORD"]
        snowflake_warehouse: str = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        snowflake_database: str = os.environ.get("SNOWFLAKE_DATABASE", "MTA")
        snowflake_role: str = os.environ.get("SNOWFLAKE_ROLE", "MTA_TRANSFORMER")
        snowflake_stage: str = "RAW.S3_STAGE"
        snowflake_raw_table: str = "RAW.MTA_EVENTS"

    cfg = SnowflakeConfig()
    conn = get_connection(cfg)
    try:
        ensure_table(conn)
        rows = copy_from_s3(conn, prefix, cfg)
        if rows == 0:
            print(f"WARNING: COPY INTO loaded 0 rows from {prefix}")
            sys.exit(1)
        print(f"Loaded {rows} rows from {prefix}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

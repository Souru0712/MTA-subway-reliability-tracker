"""
Prefect flow: daily S3 Parquet → Snowflake compaction.

Replaces the Airflow DAG. Runs at 3 AM UTC daily via Prefect Cloud.
Worker runs on Hetzner VM, pulls latest code from GitHub on each run.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from prefect import flow, task
from prefect.logging import get_run_logger

# allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@task(retries=2, retry_delay_seconds=300, log_prints=True)
def check_s3_partition(ds: str) -> None:
    """Verify yesterday's S3 partition exists before attempting COPY."""
    import boto3
    logger = get_run_logger()
    bucket = os.environ["S3_BUCKET"]
    prefix = f"raw/trip_updates/dt={ds}/"
    client = boto3.client("s3")
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        raise FileNotFoundError(f"No S3 objects found at s3://{bucket}/{prefix}")
    logger.info("S3 partition found: s3://%s/%s", bucket, prefix)


@task(retries=2, retry_delay_seconds=300, log_prints=True)
def copy_into_snowflake(ds: str) -> None:
    """Run COPY INTO Snowflake for the given date partition."""
    logger = get_run_logger()
    from config.settings import Config
    from src.load.snowflake_loader import copy_from_s3, ensure_table, get_connection

    cfg = Config()
    conn = get_connection(cfg)
    try:
        ensure_table(conn)
        s3_prefix = f"raw/trip_updates/dt={ds}/"
        rows = copy_from_s3(conn, s3_prefix, cfg)
        if rows == 0:
            raise RuntimeError(f"COPY INTO loaded 0 rows from {s3_prefix}")
        logger.info("Loaded %d rows from %s", rows, s3_prefix)
    finally:
        conn.close()


@flow(name="mta-s3-to-snowflake", log_prints=True)
def mta_s3_to_snowflake(ds: str | None = None) -> None:
    """
    Daily compaction: S3 Parquet → Snowflake RAW.MTA_EVENTS.
    ds: date string YYYY-MM-DD. Defaults to yesterday (UTC).
    """
    if ds is None:
        ds = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    check_s3_partition(ds)
    copy_into_snowflake(ds)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    mta_s3_to_snowflake()

"""
Download MTA static GTFS, join stop_times + trips, write flat CSV to S3.
Run once before starting the Spark streaming job. Re-run weekly to refresh.
"""
import io
import logging
import zipfile
from io import BytesIO

import boto3
import pandas as pd
import requests

from config.settings import Config

logger = logging.getLogger(__name__)

S3_KEY = "static/stop_times_flat.csv"


def download_static_gtfs(url: str) -> zipfile.ZipFile:
    logger.info("Downloading static GTFS from %s", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    logger.info(
        "Downloaded %d bytes | Content-Type: %s", len(resp.content), content_type
    )

    # If the server returned HTML instead of a zip, log it before failing
    if not zipfile.is_zipfile(io.BytesIO(resp.content)):
        preview = resp.text[:500]
        raise ValueError(
            f"Response from MTA is not a valid zip file.\n"
            f"Content-Type: {content_type}\n"
            f"Response preview:\n{preview}"
        )

    return zipfile.ZipFile(io.BytesIO(resp.content))


def build_flat_schedule(zf: zipfile.ZipFile) -> pd.DataFrame:
    with zf.open("stop_times.txt") as f:
        stop_times = pd.read_csv(f, usecols=["trip_id", "stop_id", "arrival_time", "departure_time"])

    with zf.open("trips.txt") as f:
        trips = pd.read_csv(f, usecols=["trip_id", "route_id", "direction_id"])

    df = stop_times.merge(trips, on="trip_id", how="left")
    logger.info("Flat schedule: %d rows, %d unique routes", len(df), df["route_id"].nunique())
    return df


def write_to_s3(df: pd.DataFrame, cfg: Config) -> str:
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    client = boto3.client(
        "s3",
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
        region_name=cfg.aws_default_region,
    )
    client.put_object(Bucket=cfg.s3_bucket, Key=S3_KEY, Body=buf.getvalue())
    uri = f"s3://{cfg.s3_bucket}/{S3_KEY}"
    logger.info("Wrote %d rows to %s", len(df), uri)
    return uri


def run(cfg: Config) -> str:
    zf = download_static_gtfs(cfg.mta_static_gtfs_url)
    df = build_flat_schedule(zf)
    return write_to_s3(df, cfg)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    from dotenv import load_dotenv
    load_dotenv()
    run(Config())

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["oscarlam84@gmail.com"],
}


def _check_s3_partition(ds: str, **_) -> None:
    """Verify that yesterday's S3 partition exists before attempting COPY."""
    import boto3
    import os

    bucket = os.environ["S3_BUCKET"]
    prefix = f"raw/trip_updates/dt={ds}/"
    client = boto3.client("s3")
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        raise FileNotFoundError(f"No S3 objects found at s3://{bucket}/{prefix}")


def _copy_into_snowflake(ds: str, **_) -> None:
    import sys
    import os

    sys.path.insert(0, "/opt/airflow")
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
    finally:
        conn.close()


def _notify_failure(**context) -> None:
    ti = context["task_instance"]
    raise RuntimeError(
        f"DAG dag_mta_s3_to_snowflake failed at task {ti.task_id} "
        f"for execution date {context['ds']}"
    )


with DAG(
    dag_id="dag_mta_s3_to_snowflake",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mta", "snowflake", "cold-compaction"],
) as dag:

    check_s3 = PythonOperator(
        task_id="check_s3_partition",
        python_callable=_check_s3_partition,
    )

    copy_snowflake = PythonOperator(
        task_id="copy_into_snowflake",
        python_callable=_copy_into_snowflake,
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_s3 >> copy_snowflake >> notify_failure

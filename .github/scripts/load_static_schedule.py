"""
Manual GitHub Actions script — loads static/stop_times_flat.csv from S3 into
Snowflake RAW.STOP_TIMES_FLAT. Run once after static_schedule.py uploads the CSV.
Re-run whenever the static schedule is refreshed.
"""
import os
import sys

import snowflake.connector

_CREATE_STAGE_SQL = """
CREATE OR REPLACE STAGE RAW.S3_STATIC_STAGE
    URL = 's3://{bucket}/static/'
    CREDENTIALS = (
        AWS_KEY_ID     = '{key_id}'
        AWS_SECRET_KEY = '{secret_key}'
    )
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        PARSE_HEADER = TRUE
    )
"""

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS RAW.STOP_TIMES_FLAT (
    trip_id         VARCHAR,
    stop_id         VARCHAR,
    arrival_time    VARCHAR,
    departure_time  VARCHAR,
    route_id        VARCHAR,
    direction_id    VARCHAR
)
"""

_TRUNCATE_SQL = "TRUNCATE TABLE RAW.STOP_TIMES_FLAT"

_COPY_SQL = """
COPY INTO RAW.STOP_TIMES_FLAT
FROM @RAW.S3_STATIC_STAGE/stop_times_flat.csv
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE
FORCE = TRUE
"""


def main() -> None:
    bucket = os.environ["S3_BUCKET"]
    key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MTA"),
        role=os.environ.get("SNOWFLAKE_ROLE", "MTA_TRANSFORMER"),
    )

    try:
        cur = conn.cursor()

        cur.execute(_CREATE_STAGE_SQL.format(
            bucket=bucket, key_id=key_id, secret_key=secret_key
        ))
        print("Stage RAW.S3_STATIC_STAGE ready")

        cur.execute(_CREATE_TABLE_SQL)
        print("Table RAW.STOP_TIMES_FLAT ready")

        # truncate before reload — static schedule is a full replace
        cur.execute(_TRUNCATE_SQL)
        print("Truncated RAW.STOP_TIMES_FLAT")

        cur.execute(_COPY_SQL)
        result = cur.fetchone()
        rows_loaded = result[3] if result and len(result) > 3 else 0
        print(f"Loaded {rows_loaded} rows into RAW.STOP_TIMES_FLAT")

        if rows_loaded == 0:
            print("WARNING: 0 rows loaded — check that static/stop_times_flat.csv exists in S3")
            sys.exit(1)

        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

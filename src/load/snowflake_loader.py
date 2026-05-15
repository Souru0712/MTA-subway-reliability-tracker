import logging

import snowflake.connector

from config.settings import Config

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS RAW.MTA_EVENTS (
    event_time        TIMESTAMP_NTZ,
    feed_id           VARCHAR,
    trip_id           VARCHAR,
    route_id          VARCHAR,
    stop_id           VARCHAR,
    arrival_time      NUMBER,
    departure_time    NUMBER,
    delay_seconds     NUMBER,
    dt                VARCHAR,
    ingestion_date    DATE
)
"""

_COPY_SQL = """
COPY INTO RAW.MTA_EVENTS
FROM @{stage}/{s3_prefix}
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE
"""


def get_connection(cfg: Config) -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
        role=cfg.snowflake_role,
    )


def ensure_table(conn: snowflake.connector.SnowflakeConnection) -> None:
    cur = conn.cursor()
    cur.execute(_CREATE_TABLE_SQL)
    cur.close()
    logger.info("RAW.MTA_EVENTS table ready")


def copy_from_s3(
    conn: snowflake.connector.SnowflakeConnection,
    s3_prefix: str,
    cfg: Config,
) -> int:
    """Run COPY INTO for the given S3 prefix (e.g. raw/trip_updates/dt=2026-04-27)."""
    sql = _COPY_SQL.format(stage=cfg.snowflake_stage, s3_prefix=s3_prefix)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchone()
    # result[3] is rows_loaded when files are copied; shorter tuple means no new files
    rows_loaded = result[3] if result and len(result) > 3 else 0
    cur.close()
    logger.info("COPY INTO loaded %d rows from %s", rows_loaded, s3_prefix)
    return rows_loaded

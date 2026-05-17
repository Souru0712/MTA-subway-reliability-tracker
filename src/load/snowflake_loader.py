import logging

import snowflake.connector

from config.settings import Config

logger = logging.getLogger(__name__)

_CREATE_TRIP_UPDATES_SQL = """
CREATE TABLE IF NOT EXISTS RAW.TRIP_UPDATES (
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

_CREATE_VEHICLE_POSITIONS_SQL = """
CREATE TABLE IF NOT EXISTS RAW.VEHICLE_POSITIONS (
    event_time        TIMESTAMP_NTZ,
    feed_id           VARCHAR,
    trip_id           VARCHAR,
    route_id          VARCHAR,
    stop_id           VARCHAR,
    current_status    VARCHAR,
    latitude          FLOAT,
    longitude         FLOAT,
    timestamp         NUMBER,
    dt                VARCHAR,
    ingestion_date    DATE
)
"""

_CREATE_ALERTS_SQL = """
CREATE TABLE IF NOT EXISTS RAW.ALERTS (
    event_time            TIMESTAMP_NTZ,
    feed_id               VARCHAR,
    alert_id              VARCHAR,
    route_id              VARCHAR,
    stop_id               VARCHAR,
    cause                 VARCHAR,
    effect                VARCHAR,
    header                VARCHAR,
    active_period_start   NUMBER,
    active_period_end     NUMBER,
    dt                    VARCHAR,
    ingestion_date        DATE
)
"""

_COPY_SQL = """
COPY INTO {table}
FROM @{stage}/{s3_prefix}
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE
"""

_TABLES = {
    "trip_updates": _CREATE_TRIP_UPDATES_SQL,
    "vehicle_positions": _CREATE_VEHICLE_POSITIONS_SQL,
    "alerts": _CREATE_ALERTS_SQL,
}

_TABLE_NAMES = {
    "trip_updates": "RAW.TRIP_UPDATES",
    "vehicle_positions": "RAW.VEHICLE_POSITIONS",
    "alerts": "RAW.ALERTS",
}


def get_connection(cfg: Config) -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
        role=cfg.snowflake_role,
    )


def ensure_tables(conn: snowflake.connector.SnowflakeConnection) -> None:
    cur = conn.cursor()
    for topic, ddl in _TABLES.items():
        cur.execute(ddl)
        logger.info("%s table ready", _TABLE_NAMES[topic])
    cur.close()


def copy_from_s3(
    conn: snowflake.connector.SnowflakeConnection,
    topic: str,
    s3_prefix: str,
    cfg: Config,
) -> int:
    """Run COPY INTO for the given topic and S3 prefix (e.g. trip_updates/dt=2026-04-27)."""
    table = _TABLE_NAMES[topic]
    sql = _COPY_SQL.format(table=table, stage=cfg.snowflake_stage, s3_prefix=s3_prefix)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchone()
    rows_loaded = result[3] if result and len(result) > 3 else 0
    cur.close()
    logger.info("COPY INTO %s loaded %d rows from %s", table, rows_loaded, s3_prefix)
    return rows_loaded

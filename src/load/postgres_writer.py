import logging

import psycopg2
import psycopg2.extras

from config.settings import Config

logger = logging.getLogger(__name__)

_UPSERT_SQL = """
INSERT INTO reliability_metrics (
    window_start, window_end, route_id, stop_id,
    on_time_pct, delay_p50_seconds, delay_p95_seconds, sample_count
)
VALUES %s
ON CONFLICT (window_start, route_id, stop_id_key)
DO UPDATE SET
    on_time_pct       = EXCLUDED.on_time_pct,
    delay_p50_seconds = EXCLUDED.delay_p50_seconds,
    delay_p95_seconds = EXCLUDED.delay_p95_seconds,
    sample_count      = EXCLUDED.sample_count,
    updated_at        = now();
"""


def write_aggregates_to_postgres(batch_df, batch_id: int, cfg: Config) -> None:
    """foreachBatch callback: upsert windowed aggregates into Postgres."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    records = [
        (
            row["window_start"],
            row["window_end"],
            row["route_id"],
            row["stop_id"],
            row["on_time_pct"],
            row["delay_p50_seconds"],
            row["delay_p95_seconds"],
            row["sample_count"],
        )
        for row in rows
    ]

    conn = psycopg2.connect(cfg.postgres_mta_dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _UPSERT_SQL, records)
        logger.info(
            "batch_id=%d upserted %d rows to reliability_metrics", batch_id, len(records)
        )
    finally:
        conn.close()

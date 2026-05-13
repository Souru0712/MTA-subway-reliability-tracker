"""
Upsert idempotency test using a mock psycopg2 connection.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_row(**kwargs):
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    row.__getitem__ = lambda self, key: getattr(self, key)
    return row


@patch("src.load.postgres_writer.psycopg2")
def test_write_aggregates_called_with_correct_records(mock_psycopg2):
    from src.load.postgres_writer import write_aggregates_to_postgres

    now = datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc)
    later = datetime(2026, 4, 28, 12, 5, tzinfo=timezone.utc)

    row = _make_mock_row(
        window_start=now,
        window_end=later,
        route_id="4",
        stop_id="640N",
        on_time_pct=0.85,
        delay_p50_seconds=30.0,
        delay_p95_seconds=180.0,
        sample_count=42,
    )

    mock_batch = MagicMock()
    mock_batch.isEmpty.return_value = False
    mock_batch.collect.return_value = [row]

    mock_conn = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    cfg = MagicMock()
    cfg.postgres_mta_dsn = "postgresql://mta:mta@localhost/mta"

    write_aggregates_to_postgres(mock_batch, batch_id=1, cfg=cfg)

    mock_psycopg2.connect.assert_called_once_with(cfg.postgres_mta_dsn)

    # verify execute_values was called with the correct record tuple
    execute_values_call = mock_psycopg2.extras.execute_values.call_args
    records = execute_values_call.args[2]
    assert len(records) == 1
    assert records[0] == (now, later, "4", "640N", 0.85, 30.0, 180.0, 42)


@patch("src.load.postgres_writer.psycopg2")
def test_write_aggregates_skips_empty_batch(mock_psycopg2):
    from src.load.postgres_writer import write_aggregates_to_postgres

    mock_batch = MagicMock()
    mock_batch.isEmpty.return_value = True

    cfg = MagicMock()
    write_aggregates_to_postgres(mock_batch, batch_id=0, cfg=cfg)

    mock_psycopg2.connect.assert_not_called()
    mock_psycopg2.extras.execute_values.assert_not_called()


@patch("src.load.postgres_writer.psycopg2")
def test_write_aggregates_closes_connection_on_error(mock_psycopg2):
    from src.load.postgres_writer import write_aggregates_to_postgres

    now = datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc)
    row = _make_mock_row(
        window_start=now,
        window_end=now,
        route_id="4",
        stop_id=None,
        on_time_pct=0.9,
        delay_p50_seconds=10.0,
        delay_p95_seconds=60.0,
        sample_count=5,
    )

    mock_batch = MagicMock()
    mock_batch.isEmpty.return_value = False
    mock_batch.collect.return_value = [row]

    mock_conn = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    # simulate DB error during execute_values
    mock_psycopg2.extras.execute_values.side_effect = RuntimeError("DB error")

    cfg = MagicMock()
    cfg.postgres_mta_dsn = "postgresql://mta:mta@localhost/mta"

    with pytest.raises(RuntimeError, match="DB error"):
        write_aggregates_to_postgres(mock_batch, batch_id=1, cfg=cfg)

    # connection must be closed even when an exception is raised
    mock_conn.close.assert_called_once()

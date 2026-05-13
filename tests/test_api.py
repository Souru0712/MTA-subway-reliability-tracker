"""
FastAPI endpoint integration tests using httpx AsyncClient with mocked DB pool.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from api.main import app


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    row = {
        "window_start": datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
        "window_end": datetime(2026, 4, 28, 12, 5, tzinfo=timezone.utc),
        "route_id": "4",
        "stop_id": None,
        "on_time_pct": 0.88,
        "delay_p50_seconds": 25.0,
        "delay_p95_seconds": 150.0,
        "sample_count": 100,
    }
    pool.fetch = AsyncMock(return_value=[row])
    return pool


async def test_reliability_returns_200(mock_pool):
    with patch("api.main.get_pool", return_value=mock_pool):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/reliability?line=4&window=1d")

    assert resp.status_code == 200
    body = resp.json()
    assert body["line"] == "4"
    assert body["window"] == "1d"
    assert len(body["records"]) == 1
    assert body["records"][0]["on_time_pct"] == pytest.approx(0.88)


async def test_reliability_with_station_filter(mock_pool):
    with patch("api.main.get_pool", return_value=mock_pool):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/reliability?line=A&station=A27N&window=6h")

    assert resp.status_code == 200
    # verify station was passed through to pool.fetch
    call_args = mock_pool.fetch.call_args
    assert call_args.args[2] == "A27N"


async def test_reliability_invalid_window(mock_pool):
    with patch("api.main.get_pool", return_value=mock_pool):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/reliability?line=4&window=99d")

    assert resp.status_code == 400


async def test_reliability_missing_line():
    # line is a required param — FastAPI returns 422 Unprocessable Entity
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/reliability?window=1d")

    assert resp.status_code == 422


async def test_health():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}

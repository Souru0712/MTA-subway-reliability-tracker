import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from api.db import close_pool, get_pool, init_pool, resolve_lookback
from api.models import ReliabilityRecord, ReliabilityResponse

logger = logging.getLogger(__name__)

_QUERY = """
SELECT
    window_start,
    window_end,
    route_id,
    stop_id,
    on_time_pct,
    delay_p50_seconds,
    delay_p95_seconds,
    sample_count
FROM reliability_metrics
WHERE route_id = $1
  AND ($2::text IS NULL OR stop_id = $2)
  AND window_start >= now() - $3::interval
ORDER BY window_start DESC
LIMIT 500;
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    yield
    await close_pool()


app = FastAPI(title="MTA Reliability API", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/reliability", response_model=ReliabilityResponse)
async def get_reliability(
    line: str = Query(..., description="Route ID, e.g. '4' or 'A'"),
    window: str = Query("1d", description="Lookback window: 1h, 6h, 12h, 1d, 7d"),
    station: str | None = Query(None, description="Optional stop_id filter"),
):
    try:
        lookback = resolve_lookback(window)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    pool = get_pool()
    rows = await pool.fetch(_QUERY, line, station, lookback)

    records = [
        ReliabilityRecord(
            window_start=row["window_start"],
            window_end=row["window_end"],
            route_id=row["route_id"],
            stop_id=row["stop_id"],
            on_time_pct=row["on_time_pct"],
            delay_p50_seconds=row["delay_p50_seconds"],
            delay_p95_seconds=row["delay_p95_seconds"],
            sample_count=row["sample_count"],
        )
        for row in rows
    ]

    return ReliabilityResponse(line=line, window=window, records=records)

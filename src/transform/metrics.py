"""
Pure aggregation functions called inside Spark foreachBatch.
All inputs/outputs are plain Python types — no Spark or I/O dependencies.
"""
from typing import Any


# A train is "on time" if it arrives within this many seconds of scheduled
ON_TIME_THRESHOLD_SECONDS = 300


def on_time_pct(delays: list[int | None]) -> float | None:
    """Return fraction of arrivals with |delay| <= threshold. None if no data."""
    valid = [d for d in delays if d is not None]
    if not valid:
        return None
    on_time = sum(1 for d in valid if abs(d) <= ON_TIME_THRESHOLD_SECONDS)
    return round(on_time / len(valid), 4)


def percentile(values: list[float | None], p: float) -> float | None:
    """Return the p-th percentile (0–100) of values. None if no data."""
    valid = sorted(v for v in values if v is not None)
    if not valid:
        return None
    idx = (len(valid) - 1) * p / 100
    lo = int(idx)
    hi = lo + 1
    if hi >= len(valid):
        return float(valid[lo])
    frac = idx - lo
    return round(valid[lo] + frac * (valid[hi] - valid[lo]), 2)

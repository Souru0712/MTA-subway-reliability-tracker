import os
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncGenerator

import asyncpg

_pool: asyncpg.Pool | None = None

_LOOKBACK = {
    "1h":  timedelta(hours=1),
    "6h":  timedelta(hours=6),
    "12h": timedelta(hours=12),
    "1d":  timedelta(days=1),
    "7d":  timedelta(days=7),
}


async def init_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=os.environ["POSTGRES_MTA_DSN"],
        min_size=2,
        max_size=10,
    )


async def close_pool() -> None:
    if _pool:
        await _pool.close()


def get_pool() -> asyncpg.Pool:
    assert _pool is not None, "DB pool not initialized"
    return _pool


def resolve_lookback(window: str) -> timedelta:
    """Map window param (e.g. '1d') to a timedelta for asyncpg interval binding."""
    if window not in _LOOKBACK:
        raise ValueError(
            f"Invalid window '{window}'. Allowed: {list(_LOOKBACK)}"
        )
    return _LOOKBACK[window]

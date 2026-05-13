from datetime import datetime

from pydantic import BaseModel


class ReliabilityRecord(BaseModel):
    window_start: datetime
    window_end: datetime
    route_id: str
    stop_id: str | None
    on_time_pct: float | None
    delay_p50_seconds: float | None
    delay_p95_seconds: float | None
    sample_count: int


class ReliabilityResponse(BaseModel):
    line: str
    window: str
    records: list[ReliabilityRecord]

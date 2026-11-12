from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeAlias


@dataclass(frozen=True)
class FlightRecord:
    source: str
    record_key: str
    observed_at: str
    flight_id: str
    flight_date: str
    airline_code: str
    origin: str
    destination: str
    status: str
    scheduled_departure: str
    actual_departure: str | None
    payload: dict[str, Any]
    raw_hash: str


DOMAIN_RECORD_TYPE: TypeAlias = FlightRecord

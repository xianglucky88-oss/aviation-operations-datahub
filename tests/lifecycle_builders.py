"""Synthetic record builders for lifecycle reconstruction tests.

Kept separate from downloaded/untrusted data: helpers live with the test
suite and only emit platform-shaped raw dictionaries.
"""

from __future__ import annotations

import copy
from typing import Any

from aviation_operations.normalize import normalize


def raw_observation(
    *,
    source: str,
    observed_at: str,
    status: str,
    flight_id: str = "CA100",
    flight_date: str = "2026-11-01",
    actual_departure: str | None = None,
    scheduled_departure: str = "2026-11-01T06:30:00Z",
    airline_code: str = "CA",
    origin: str = "PEK",
    destination: str = "SHA",
) -> dict[str, Any]:
    return {
        "source": source,
        "observed_at": observed_at,
        "payload": {
            "flight_id": flight_id,
            "flight_date": flight_date,
            "airline_code": airline_code,
            "origin": origin,
            "destination": destination,
            "status": status,
            "scheduled_departure": scheduled_departure,
            "actual_departure": actual_departure,
        },
    }


def normalized(rows: list[dict[str, Any]]):
    return [normalize(row, row["source"]) for row in rows]


def shuffled(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deterministic out-of-order permutation (reverse + swap)."""

    result = copy.deepcopy(rows)
    result.reverse()
    if len(result) > 2:
        result[0], result[2] = result[2], result[0]
    return result


def full_trajectory_rows() -> list[dict[str, Any]]:
    """A complete, well-formed lifecycle observed by one source out of order."""

    return [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:20:00Z", status="boarding"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:25:00Z", status="delayed"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:50:00Z",
            status="departed",
            actual_departure="2026-11-01T06:48:00Z",
        ),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T08:30:00Z",
            status="arrived",
            actual_departure="2026-11-01T06:48:00Z",
        ),
    ]

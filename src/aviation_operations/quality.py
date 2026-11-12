from __future__ import annotations

from .models import FlightRecord


def validate(record: FlightRecord) -> list[str]:
    issues: list[str] = []

    if record.origin == record.destination:
        issues.append("origin and destination must differ")

    return issues

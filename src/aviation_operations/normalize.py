from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import FlightRecord
from .identity import record_key


def _text(value: Any) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("numeric field is invalid") from exc


def normalize(raw: Any, source: str) -> FlightRecord:
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    payload = raw.get("payload")
    observed_at = _text(raw.get("observed_at"))
    if not isinstance(payload, dict) or not observed_at:
        raise ValueError("observed_at and object payload are required")

    flight_id = _text(payload.get("flight_id"))
    flight_date = _text(payload.get("flight_date"))
    airline_code = _text(payload.get("airline_code")).upper()
    origin = _text(payload.get("origin")).upper()
    destination = _text(payload.get("destination")).upper()
    status = _text(payload.get("status")).lower()
    scheduled_departure = _text(payload.get("scheduled_departure"))
    actual_departure = _text(payload.get("actual_departure")) or None
    if not all([flight_id, flight_date, airline_code, origin, destination, status, scheduled_departure, observed_at]):
        raise ValueError("flight identity, status and scheduled_departure are required")
    if len(origin) != 3 or len(destination) != 3:
        raise ValueError("airport codes must have three letters")
    if status not in {"scheduled", "boarding", "departed", "arrived", "delayed", "cancelled"}:
        raise ValueError("unsupported flight status")

    normalized = {"flight_id": flight_id, "flight_date": flight_date, "airline_code": airline_code, "origin": origin, "destination": destination, "status": status, "scheduled_departure": scheduled_departure, "actual_departure": actual_departure}
    key = record_key(flight_id, flight_date, observed_at)
    canonical = json.dumps({"source": source, "observed_at": observed_at, "payload": normalized}, ensure_ascii=False, sort_keys=True)
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return FlightRecord(
        source=source,
        record_key=key,
        observed_at=observed_at,
        flight_id=normalized["flight_id"],
        flight_date=normalized["flight_date"],
        airline_code=normalized["airline_code"],
        origin=normalized["origin"],
        destination=normalized["destination"],
        status=normalized["status"],
        scheduled_departure=normalized["scheduled_departure"],
        actual_departure=normalized["actual_departure"],
        payload=normalized,
        raw_hash=raw_hash,
    )

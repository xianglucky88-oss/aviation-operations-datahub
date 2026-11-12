from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable

from .models import DOMAIN_RECORD_TYPE
from .quality import validate
from .storage import DataStore


class IngestionPipeline:
    """Coordinates normalization, quality checks and persistence only.

    An optional :class:`IdentityService` can be attached to populate the
    queryable identity index.  Identity processing never changes
    accept/reject semantics and never reinterprets ``record_key``:
    resolution failures are captured for retry, not raised into the
    domain ingestion path.
    """

    def __init__(
        self,
        records: Iterable[Any],
        source: str,
        store: DataStore,
        batch_id: int,
        normalizer: Callable[[Any, str], DOMAIN_RECORD_TYPE],
        identity_service: Any | None = None,
    ) -> None:
        self.records = records
        self.source = source
        self.store = store
        self.batch_id = batch_id
        self.normalizer = normalizer
        self.identity_service = identity_service

    def run(self) -> dict[str, int]:
        processed = accepted = rejected = changed = identity_failed = 0
        for raw in self.records:
            processed += 1
            try:
                record = self.normalizer(raw, self.source)
                issues = validate(record)
                if issues:
                    raise ValueError("; ".join(issues))
                accepted += 1
                changed += int(self.store.upsert(record))
                if self.identity_service is not None:
                    try:
                        resolution = self.identity_service.resolve(
                            flight_id=record.flight_id,
                            flight_date=record.flight_date,
                            airline_code=record.airline_code,
                            origin=record.origin,
                            destination=record.destination,
                            scheduled_departure=record.scheduled_departure,
                        )
                        self.identity_service.index_record(
                            record.record_key, record.source, resolution
                        )
                    except Exception as exc:  # recovery via identity_failures + retry
                        identity_failed += 1
                        self.identity_service.writer.record_failure(
                            record.record_key,
                            record.source,
                            str(exc),
                            record.payload,
                        )
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                source = raw.get("source") if isinstance(raw, dict) else None
                self.store.reject(self.batch_id, raw, source, str(exc))
        self.store.finish_batch(self.batch_id, "completed", processed, accepted, rejected)
        result = {"processed": processed, "accepted": accepted, "rejected": rejected, "changed": changed}
        if self.identity_service is not None:
            result["identity_failed"] = identity_failed
        result.update(self.store.counts())
        return result

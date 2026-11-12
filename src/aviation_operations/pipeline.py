from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable

from .lifecycle_store import LifecycleStore
from .models import DOMAIN_RECORD_TYPE
from .quality import validate
from .storage import DataStore


class IngestionPipeline:
    """Coordinates normalization, quality checks and persistence only."""

    def __init__(
        self,
        records: Iterable[Any],
        source: str,
        store: DataStore,
        batch_id: int,
        normalizer: Callable[[Any, str], DOMAIN_RECORD_TYPE],
        lifecycle_store: LifecycleStore | None = None,
    ) -> None:
        self.records = records
        self.source = source
        self.store = store
        self.batch_id = batch_id
        self.normalizer = normalizer
        self.lifecycle_store = lifecycle_store

    def run(self) -> dict[str, int]:
        processed = accepted = rejected = changed = indexed = 0
        for raw in self.records:
            processed += 1
            try:
                # A record may carry its own originating source; fall back to
                # the batch source so existing inputs behave unchanged.
                record_source = (
                    raw.get("source") if isinstance(raw, dict) and raw.get("source") else self.source
                )
                record = self.normalizer(raw, record_source)
                issues = validate(record)
                if issues:
                    raise ValueError("; ".join(issues))
                accepted += 1
                changed += int(self.store.upsert(record))
                if self.lifecycle_store is not None:
                    indexed += int(self.lifecycle_store.index_record(record))
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                source = raw.get("source") if isinstance(raw, dict) else None
                self.store.reject(self.batch_id, raw, source, str(exc))
        self.store.finish_batch(self.batch_id, "completed", processed, accepted, rejected)
        result = {"processed": processed, "accepted": accepted, "rejected": rejected, "changed": changed, "indexed": indexed}
        result.update(self.store.counts())
        return result

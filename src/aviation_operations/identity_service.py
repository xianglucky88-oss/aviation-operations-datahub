"""Application service combining the deterministic resolver and the store.

The service is the only object callers need when they want persisted,
queryable flight identities.  It also centralizes cache invalidation so
that mapping swaps, manual decisions and withdrawals can never return
stale resolutions.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Callable, Iterable

from .identity import (
    CandidateConflict,
    FlightIdentityResolver,
    IdentityResolution,
    MappingBundle,
    STATUS_AMBIGUOUS,
    STATUS_EXACT,
    STATUS_INVALID,
    STATUS_MANUAL,
    load_bundle,
)
from .identity_store import (
    IdentityReader,
    IdentityStoreError,
    IdentityWriter,
    apply_manual_decision,
)


class IdentityService:
    def __init__(
        self,
        database: str,
        initial_bundle: MappingBundle | None = None,
        *,
        cache_ttl_seconds: float = 300.0,
        faults: Iterable[Any] | None = None,
    ) -> None:
        self.database = database
        self._lock = threading.RLock()
        self._cache_ttl_seconds = cache_ttl_seconds
        self.writer = IdentityWriter(
            database, faults=faults, on_change=self._on_store_change
        )
        self.writer.migrate()
        if initial_bundle is not None:
            self.activate_bundle(initial_bundle)
        else:
            try:
                initial_bundle = self.writer.load_active_bundle()
            except IdentityStoreError:
                initial_bundle = _empty_bundle()
        self.resolver = FlightIdentityResolver(initial_bundle, cache_ttl_seconds=cache_ttl_seconds)
        self._manual_cache: dict[str, dict[str, Any]] = {}
        self._refresh_manual_decisions()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self.writer.close()

    def __enter__(self) -> "IdentityService":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def reader(self) -> IdentityReader:
        # A fresh read-only connection sees every committed write.
        return IdentityReader(self.database)

    # ------------------------------------------------------------------
    # Mapping lifecycle
    # ------------------------------------------------------------------

    def activate_bundle(
        self, bundle: MappingBundle, *, expected_write_version: int | None = None
    ) -> int:
        with self._lock:
            new_version = self.writer.activate_bundle(
                bundle, expected_write_version=expected_write_version
            )
            current = getattr(self, "resolver", None)
            self.resolver = (
                current.with_bundle(bundle)
                if current is not None
                else FlightIdentityResolver(
                    bundle, cache_ttl_seconds=self._cache_ttl_seconds
                )
            )
            # Existing index rows computed under older mappings are stale
            # until backfill/reindex catches up.
            self.writer.mark_stale_for_mapping(bundle.bundle_hash)
            self._refresh_manual_decisions()
            return new_version

    def load_bundle_from_file(
        self, path: str | Path, *, expected_write_version: int | None = None
    ) -> int:
        return self.activate_bundle(
            load_bundle(path), expected_write_version=expected_write_version
        )

    def _on_store_change(self) -> None:
        with self._lock:
            resolver = getattr(self, "resolver", None)
            if resolver is not None:
                resolver.cache.invalidate()

    def invalidate_cache(self) -> None:
        with self._lock:
            self.resolver.cache.invalidate()

    # ------------------------------------------------------------------
    # Manual decisions
    # ------------------------------------------------------------------

    def _refresh_manual_decisions(self) -> None:
        rows = self.writer.conn.execute(
            "SELECT * FROM manual_decisions WHERE active=1"
        ).fetchall()
        self._manual_cache = {row["review_key"]: dict(row) for row in rows}

    def confirm_identity(
        self,
        review_key: str,
        canonical_flight_key: str,
        rationale: str = "",
        covered_fingerprints: Iterable[str] | None = None,
        *,
        expected_write_version: int | None = None,
    ) -> int:
        with self._lock:
            decision_id = self.writer.save_manual_decision(
                review_key,
                canonical_flight_key,
                self.resolver.bundle.version,
                self.resolver.bundle.bundle_hash,
                rationale,
                covered_fingerprints=covered_fingerprints,
                expected_write_version=expected_write_version,
            )
            self._refresh_manual_decisions()
            return decision_id

    def withdraw_identity(self, review_key: str, *, expected_write_version: int | None = None) -> int:
        with self._lock:
            version = self.writer.withdraw_manual_decision(
                review_key, expected_write_version=expected_write_version
            )
            self._refresh_manual_decisions()
            return version

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, **fields: Any) -> IdentityResolution:
        with self._lock:
            resolution = self.resolver.resolve(**fields)
            decision = self._decision_for(resolution)
            if decision is not None and resolution.status in {STATUS_AMBIGUOUS, STATUS_INVALID}:
                return apply_manual_decision(resolution, decision)
            return resolution

    def _decision_for(self, resolution: IdentityResolution) -> dict[str, Any] | None:
        # The fingerprint is stable for a given normalized input across
        # single and batch calls, while review_key stays the human-facing
        # case id.  Match on either, deterministically.
        direct = self._manual_cache.get(resolution.review_key)
        if direct is not None:
            return direct
        for decision in self._manual_cache.values():
            fingerprints = json.loads(decision.get("covered_fingerprints_json") or "[]")
            if resolution.fingerprint in fingerprints:
                return decision
        return None

    def resolve_batch(
        self, records: Iterable[Any], *, attribute_getter: Callable[[Any], dict[str, Any]] | None = None
    ) -> tuple[list[IdentityResolution], tuple[CandidateConflict, ...]]:
        records = list(records)
        with self._lock:
            resolutions, conflicts = self.resolver.resolve_batch(
                records, attribute_getter=attribute_getter
            )
            final: list[IdentityResolution] = []
            for resolution in resolutions:
                decision = self._decision_for(resolution)
                if decision is not None and resolution.status in {STATUS_AMBIGUOUS, STATUS_INVALID}:
                    final.append(apply_manual_decision(resolution, decision))
                else:
                    final.append(resolution)
            return final, conflicts

    # ------------------------------------------------------------------
    # Persistence and recovery
    # ------------------------------------------------------------------

    def index_record(
        self, record_key: str, source: str, resolution: IdentityResolution
    ) -> bool:
        with self._lock:
            return self.writer.write_identity(record_key, source, resolution)

    def index_batch(
        self, records: Iterable[Any],
        *,
        record_key_fn: Callable[[Any, dict[str, Any]], str],
        attribute_getter: Callable[[Any], dict[str, Any]] | None = None,
    ) -> dict[str, int]:
        """Resolve and persist a batch, recording conflicts and failures.

        ``record_key_fn(record, fields)`` keeps the historical key policy
        outside this service; identity never reinterprets record_key.
        """

        records = list(records)
        resolutions, conflicts = self.resolve_batch(
            records, attribute_getter=attribute_getter
        )
        accepted = ambiguous = invalid = failed = 0
        for record, resolution in zip(records, resolutions):
            fields = (attribute_getter or _default_getter)(record)
            key = record_key_fn(record, fields)
            try:
                self.index_record(key, _source_of(record), resolution)
                if resolution.status == STATUS_EXACT or resolution.status == STATUS_MANUAL:
                    accepted += 1
                elif resolution.status == STATUS_INVALID:
                    invalid += 1
                else:
                    ambiguous += 1
            except Exception as exc:
                failed += 1
                self.writer.record_failure(key, _source_of(record), str(exc), fields)
        for conflict in conflicts:
            self.writer.record_conflict(conflict, self.resolver.bundle.version)
        return {
            "processed": len(records),
            "exact": accepted,
            "ambiguous": ambiguous,
            "invalid": invalid,
            "conflicts": len(conflicts),
            "failed": failed,
        }

    def backfill(self) -> dict[str, int]:
        with self._lock:
            return self.writer.backfill_from_records(
                lambda payload: self.resolve(**_payload_fields(payload))
            )

    def reindex(self) -> dict[str, int]:
        with self._lock:
            return self.writer.reindex_all(
                lambda payload: self.resolve(**_payload_fields(payload))
            )

    def retry_failures(self) -> dict[str, int]:
        with self._lock:
            return self.writer.retry_failures(
                lambda payload: self.resolve(**_payload_fields(payload))
            )


def _empty_bundle() -> MappingBundle:
    return MappingBundle(version="empty-v0")


def _default_getter(record: Any) -> dict[str, Any]:
    if isinstance(record, dict):
        payload = record.get("payload", record)
        return _payload_fields(payload)
    return {
        "flight_id": getattr(record, "flight_id"),
        "flight_date": getattr(record, "flight_date"),
        "airline_code": getattr(record, "airline_code", None),
        "origin": getattr(record, "origin", None),
        "destination": getattr(record, "destination", None),
        "scheduled_departure": getattr(record, "scheduled_departure", None),
    }


def _payload_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "flight_id": payload.get("flight_id"),
        "flight_date": payload.get("flight_date"),
        "airline_code": payload.get("airline_code"),
        "origin": payload.get("origin"),
        "destination": payload.get("destination"),
        "scheduled_departure": payload.get("scheduled_departure"),
    }


def _source_of(record: Any) -> str:
    if isinstance(record, dict):
        return str(record.get("source", "unknown"))
    return str(getattr(record, "source", "unknown"))

"""SQLite read model for flight lifecycle reconstruction.

The store is additive: the original ingestion tables (``records``,
``batches`` ...) are never altered.  New tables support rebuilding *one*
flight key at a time without scanning ``records``, while keeping every fact
traceable back to its ``record_key``/source/raw payload.

Concurrency and crash safety
----------------------------
* ``flight_lifecycle_locks`` is a per-flight advisory lock implemented with a
  unique primary key.  Two processes revising the same flight serialise; the
  loser gets :class:`FlightLockConflict` instead of silently overwriting.
* The rebuild itself runs in a single ``BEGIN IMMEDIATE`` write transaction.
  A crash before commit leaves no partial version; a crash after commit
  leaves a complete, recoverable version.
* Versions are append-only and content-addressed by an input/config
  fingerprint: replays of identical input do not create duplicate versions.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from .identity import flight_key as make_flight_key
from .lifecycle_model import (
    LifecycleConfig,
    Trajectory,
    rebuild,
)
from .models import FlightRecord

SCHEMA_VERSION = 1


class FlightLockConflict(RuntimeError):
    """Raised when another process already holds the lock for a flight key."""


class LifecycleStoreError(RuntimeError):
    """Raised when lifecycle storage is in an unexpected state."""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class LifecycleStore:
    """Owns the lifecycle read-model tables, locks and published versions."""

    def __init__(
        self,
        database: str,
        *,
        lock_owner: str = "lifecycle-worker",
        lock_ttl_seconds: float = 300.0,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        self.conn = sqlite3.connect(database, timeout=busy_timeout_ms / 1000.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        self.lock_owner = lock_owner
        self.lock_ttl_seconds = float(lock_ttl_seconds)
        self._fault = ""
        self._held_locks: dict[str, int] = {}
        self._migrate()
        self._recover_stale_locks()

    # ------------------------------------------------------------------ schema
    def _migrate(self) -> None:
        """Idempotent, additive migration. Safe to run on every open."""

        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS flight_observations (
                observation_key TEXT PRIMARY KEY,
                flight_key TEXT NOT NULL,
                flight_id TEXT NOT NULL,
                flight_date TEXT NOT NULL,
                record_key TEXT NOT NULL,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                event_family TEXT NOT NULL,
                scheduled_departure TEXT NOT NULL,
                actual_departure TEXT,
                raw_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                indexed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_flight_observations_key
                ON flight_observations(flight_key, observed_at, record_key);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_flight_observations_identity
                ON flight_observations(record_key, source);
            CREATE TABLE IF NOT EXISTS flight_lifecycle_locks (
                flight_key TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS flight_lifecycle_lock_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                flight_key TEXT NOT NULL,
                event TEXT NOT NULL,
                owner TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS flight_lifecycle_versions (
                version_id INTEGER PRIMARY KEY AUTOINCREMENT,
                flight_key TEXT NOT NULL,
                version_no INTEGER NOT NULL,
                config_fingerprint TEXT NOT NULL,
                config_json TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                conflict_count INTEGER NOT NULL,
                quality_warning_count INTEGER NOT NULL,
                current_state TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (flight_key, version_no)
            );
            CREATE INDEX IF NOT EXISTS idx_versions_flight
                ON flight_lifecycle_versions(flight_key, version_no);
            CREATE TABLE IF NOT EXISTS flight_lifecycle_states (
                state_id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                flight_key TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                status TEXT NOT NULL,
                event_family TEXT NOT NULL,
                record_key TEXT NOT NULL,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT,
                adopted INTEGER NOT NULL,
                decision TEXT NOT NULL,
                conflict_reason TEXT,
                actual_departure TEXT,
                quality_issues_json TEXT NOT NULL,
                raw_hash TEXT NOT NULL,
                FOREIGN KEY(version_id) REFERENCES flight_lifecycle_versions(version_id)
            );
            CREATE INDEX IF NOT EXISTS idx_states_version
                ON flight_lifecycle_states(version_id, sequence_no);
            CREATE TABLE IF NOT EXISTS flight_lifecycle_conflicts (
                conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                flight_key TEXT NOT NULL,
                record_key TEXT NOT NULL,
                source TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                event_family TEXT NOT NULL,
                conflict_axis TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason TEXT NOT NULL,
                competing_record_key TEXT,
                competing_source TEXT,
                raw_hash TEXT NOT NULL,
                FOREIGN KEY(version_id) REFERENCES flight_lifecycle_versions(version_id)
            );
            CREATE INDEX IF NOT EXISTS idx_conflicts_version
                ON flight_lifecycle_conflicts(version_id);
            """
        )
        applied = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
        ).fetchone()[0]
        if applied < SCHEMA_VERSION:
            # Backfill from the legacy records snapshot so existing databases
            # gain the read model without re-running ingestion.
            self._backfill_from_records()
            self.conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, _utcnow()),
            )
        self.conn.commit()

    def _backfill_from_records(self) -> None:
        if not self._table_exists("records"):
            return
        rows = self.conn.execute(
            """SELECT r.record_key, r.source, r.observed_at, r.payload_json, r.raw_hash
               FROM records r
               LEFT JOIN flight_observations fo ON fo.record_key = r.record_key
               WHERE fo.record_key IS NULL"""
        ).fetchall()
        for row in rows:
            payload = json.loads(row["payload_json"])
            self._insert_observation_row(
                observation_key=f"{row['record_key']}#{row['source']}",
                record_key=row["record_key"],
                source=row["source"],
                observed_at=row["observed_at"],
                raw_hash=row["raw_hash"],
                payload=payload,
            )

    def _table_exists(self, name: str) -> bool:
        return bool(
            self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone()
        )

    # ------------------------------------------------------------- observation
    def index_record(self, record: FlightRecord) -> bool:
        """Mirror one ingested record into the per-flight observation index.

        ``record_key`` already identifies ``(flight, flight_date, observed_at)``
        across sources, so it is the identity.  Returns ``True`` when a new
        observation was stored; an exact replay is a no-op, while a competing
        observation from another source at the same timestamp is preserved as
        a distinct row keyed by an internal surrogate.
        """

        with self._immediate() as conn:
            existing = conn.execute(
                "SELECT 1 FROM flight_observations WHERE record_key=? AND source=?",
                (record.record_key, record.source),
            ).fetchone()
            if existing:
                return False
            self._insert_observation_row(
                observation_key=f"{record.record_key}#{record.source}",
                record_key=record.record_key,
                source=record.source,
                observed_at=record.observed_at,
                raw_hash=record.raw_hash,
                payload=record.payload,
            )
            return True

    def _insert_observation_row(
        self,
        *,
        observation_key: str,
        record_key: str,
        source: str,
        observed_at: str,
        raw_hash: str,
        payload: dict[str, Any],
    ) -> None:
        flight_id = str(payload["flight_id"])
        flight_date = str(payload["flight_date"])
        key = make_flight_key(flight_id, flight_date)
        from .lifecycle import event_family  # local import avoids a cycle at import time

        self.conn.execute(
            """INSERT OR IGNORE INTO flight_observations(
                 observation_key, flight_key, flight_id, flight_date, record_key, source,
                 observed_at, status, event_family, scheduled_departure, actual_departure,
                 raw_hash, payload_json, indexed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                observation_key,
                key,
                flight_id,
                flight_date,
                record_key,
                source,
                observed_at,
                payload["status"],
                event_family(payload["status"]),
                payload["scheduled_departure"],
                payload.get("actual_departure"),
                raw_hash,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                _utcnow(),
            ),
        )

    def list_flight_keys(self) -> list[str]:
        return [
            row[0]
            for row in self.conn.execute(
                "SELECT DISTINCT flight_key FROM flight_observations ORDER BY flight_key"
            ).fetchall()
        ]

    def _load_observations(self, key: str) -> list[FlightRecord]:
        rows = self.conn.execute(
            """SELECT record_key, source, observed_at, status, event_family,
                      scheduled_departure, actual_departure, raw_hash, payload_json
               FROM flight_observations WHERE flight_key=?
               ORDER BY observed_at, record_key, source""",
            (key,),
        ).fetchall()
        records: list[FlightRecord] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            records.append(
                FlightRecord(
                    source=row["source"],
                    record_key=row["record_key"],
                    observed_at=row["observed_at"],
                    flight_id=payload["flight_id"],
                    flight_date=payload["flight_date"],
                    airline_code=payload["airline_code"],
                    origin=payload["origin"],
                    destination=payload["destination"],
                    status=row["status"],
                    scheduled_departure=row["scheduled_departure"],
                    actual_departure=row["actual_departure"],
                    payload=payload,
                    raw_hash=row["raw_hash"],
                )
            )
        return records

    # ------------------------------------------------------------------ locks
    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Connection]:
        """Single BEGIN IMMEDIATE transaction; commit on success, rollback on error."""

        conn = self.conn
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise FlightLockConflict(f"database write lock unavailable: {exc}") from exc
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    def _recover_stale_locks(self) -> None:
        """Clear locks left behind by a crashed process (no heartbeat table survives)."""

        now = datetime.now(timezone.utc)
        try:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            # Another process currently holds the SQLite write lock; stale-lock
            # recovery will run on the next open.
            return
        try:
            rows = conn.execute(
                "SELECT flight_key, owner, expires_at FROM flight_lifecycle_locks"
            ).fetchall()
            for row in rows:
                expires = datetime.fromisoformat(row["expires_at"])
                if expires <= now:
                    conn.execute(
                        "DELETE FROM flight_lifecycle_locks WHERE flight_key=?",
                        (row["flight_key"],),
                    )
                    conn.execute(
                        """INSERT INTO flight_lifecycle_lock_events(
                             flight_key, event, owner, detail, created_at)
                           VALUES (?, 'lock_recovered', ?, 'expired lock cleared on open', ?)""",
                        (row["flight_key"], row["owner"], _utcnow()),
                    )
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    def _log_lock_event(
        self, conn: sqlite3.Connection, key: str, event: str, detail: str | None
    ) -> None:
        conn.execute(
            """INSERT INTO flight_lifecycle_lock_events(
                 flight_key, event, owner, detail, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (key, event, self.lock_owner, detail, _utcnow()),
        )

    @contextmanager
    def flight_lock(self, key: str) -> Iterator[None]:
        """Acquire the per-flight advisory lock, waiting on SQLite's busy handler.

        Reentrant for the same store instance/owner (nested ``rebuild_flight``
        calls share one lock).  Stale locks from a crashed owner are reclaimed
        after their TTL; a still-live foreign lock raises
        :class:`FlightLockConflict`.
        """

        # Reentrant fast path: same instance already owns this flight.
        if key in self._held_locks:
            self._held_locks[key] += 1
            try:
                yield
            finally:
                self._held_locks[key] -= 1
                if self._held_locks[key] == 0:
                    del self._held_locks[key]
            return

        acquired = False
        try:
            try:
                lock_transaction = self._immediate()
                conn = lock_transaction.__enter__()
            except (sqlite3.OperationalError, FlightLockConflict) as exc:
                # Another process holds the SQLite write lock (its advisory
                # lock transaction is open). Surface a typed conflict instead
                # of leaking the driver error.
                raise FlightLockConflict(
                    f"flight {key} is locked by another process: {exc}"
                ) from exc
            try:
                self._fault_point("before_lock_acquire")
                row = conn.execute(
                    "SELECT owner, acquired_at, expires_at FROM flight_lifecycle_locks WHERE flight_key=?",
                    (key,),
                ).fetchone()
                now = datetime.now(timezone.utc)
                if row is not None:
                    expires = datetime.fromisoformat(row["expires_at"])
                    if row["owner"] == self.lock_owner:
                        # Orphaned row for our own owner (e.g. a new connection
                        # after an in-process reset): reclaim rather than deny.
                        self._log_lock_event(
                            conn, key, "lock_reclaimed", "orphaned row for same owner"
                        )
                        conn.execute(
                            "DELETE FROM flight_lifecycle_locks WHERE flight_key=?", (key,)
                        )
                    elif expires > now:
                        self._log_lock_event(
                            conn, key, "lock_denied", f"held by {row['owner']}"
                        )
                        raise FlightLockConflict(
                            f"flight {key} is locked by {row['owner']} until {row['expires_at']}"
                        )
                    else:
                        self._log_lock_event(
                            conn, key, "lock_reclaimed", f"stale lock from {row['owner']}"
                        )
                        conn.execute(
                            "DELETE FROM flight_lifecycle_locks WHERE flight_key=?", (key,)
                        )
                acquired_at = _utcnow()
                expires_at = (
                    datetime.now(timezone.utc).timestamp() + self.lock_ttl_seconds
                )
                conn.execute(
                    """INSERT INTO flight_lifecycle_locks(
                         flight_key, owner, acquired_at, expires_at)
                       VALUES (?, ?, ?, ?)""",
                    (
                        key,
                        self.lock_owner,
                        acquired_at,
                        datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(),
                    ),
                )
                self._log_lock_event(conn, key, "lock_acquired", None)
                acquired = True
                self._held_locks[key] = 1
                lock_transaction.__exit__(None, None, None)
            except BaseException:
                lock_transaction.__exit__(*sys.exc_info())
                raise
            yield
        finally:
            if acquired:
                self._held_locks.pop(key, None)
                with self._immediate() as conn:
                    conn.execute(
                        "DELETE FROM flight_lifecycle_locks WHERE flight_key=? AND owner=?",
                        (key, self.lock_owner),
                    )
                    self._log_lock_event(conn, key, "lock_released", None)

    # ---------------------------------------------------------------- rebuild
    def rebuild_flight(
        self,
        flight_id: str,
        flight_date: str,
        config: LifecycleConfig | None = None,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """Incrementally rebuild one flight; never scans the whole records table."""

        config = config or LifecycleConfig()
        key = make_flight_key(flight_id, flight_date)
        with self.flight_lock(key):
            records = self._load_observations(key)
            if not records:
                return {
                    "flight_key": key,
                    "version_id": None,
                    "version_no": 0,
                    "published": False,
                    "reason": "no_observations",
                    "observation_count": 0,
                }
            trajectory = rebuild(flight_id, flight_date, key, records, config)
            latest = self.conn.execute(
                """SELECT version_id, input_fingerprint, config_fingerprint
                   FROM flight_lifecycle_versions
                   WHERE flight_key=? ORDER BY version_no DESC LIMIT 1""",
                (key,),
            ).fetchone()
            if (
                not force
                and latest is not None
                and latest["input_fingerprint"] == trajectory.input_fingerprint
                and latest["config_fingerprint"] == trajectory.config_fingerprint
            ):
                return {
                    "flight_key": key,
                    "version_id": latest["version_id"],
                    "version_no": self._version_no(latest["version_id"]),
                    "published": False,
                    "reason": "unchanged_input",
                    "observation_count": trajectory.observation_count,
                    "input_fingerprint": trajectory.input_fingerprint,
                }
            version_id = self._publish(trajectory, config)
            return {
                "flight_key": key,
                "version_id": version_id,
                "version_no": self._version_no(version_id),
                "published": True,
                "reason": "new_version",
                "observation_count": trajectory.observation_count,
                "conflict_count": len(trajectory.conflicts),
                "input_fingerprint": trajectory.input_fingerprint,
            }

    def _version_no(self, version_id: int) -> int:
        return int(
            self.conn.execute(
                "SELECT version_no FROM flight_lifecycle_versions WHERE version_id=?",
                (version_id,),
            ).fetchone()[0]
        )

    def _publish(self, trajectory: Trajectory, config: LifecycleConfig) -> int:
        """Publish one immutable version inside a single write transaction."""

        with self._immediate() as conn:
            self._fault_point("before_publish")
            # Re-read inside the write transaction so the numbering cannot
            # race with another committed publisher.
            next_no = conn.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 FROM flight_lifecycle_versions WHERE flight_key=?",
                (trajectory.flight_key,),
            ).fetchone()[0]
            cursor = conn.execute(
                """INSERT INTO flight_lifecycle_versions(
                     flight_key, version_no, config_fingerprint, config_json,
                     input_fingerprint, observation_count, conflict_count,
                     quality_warning_count, current_state, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', ?)""",
                (
                    trajectory.flight_key,
                    next_no,
                    trajectory.config_fingerprint,
                    json.dumps(
                        {
                            "same_timestamp_policy": config.same_timestamp_policy,
                            "transition_policy": config.transition_policy,
                            "source_priority": dict(config.source_priority),
                            "max_observations": config.max_observations,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    trajectory.input_fingerprint,
                    trajectory.observation_count,
                    len(trajectory.conflicts),
                    trajectory.quality_warning_count,
                    trajectory.current_state,
                    _utcnow(),
                ),
            )
            version_id = int(cursor.lastrowid)
            self._insert_segments(conn, version_id, trajectory)
            self._insert_conflicts(conn, version_id, trajectory)
            self._fault_point("after_publish_rows")
        return version_id

    def _insert_segments(
        self, conn: sqlite3.Connection, version_id: int, trajectory: Trajectory
    ) -> None:
        conn.executemany(
            """INSERT INTO flight_lifecycle_states(
                 version_id, flight_key, sequence_no, status, event_family,
                 record_key, source, observed_at, effective_from, effective_to,
                 adopted, decision, conflict_reason, actual_departure,
                 quality_issues_json, raw_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    version_id,
                    trajectory.flight_key,
                    segment.sequence,
                    segment.status,
                    segment.event_family,
                    segment.record_key,
                    segment.source,
                    segment.observed_at,
                    segment.effective_from,
                    segment.effective_to,
                    int(segment.adopted),
                    segment.decision,
                    segment.conflict_reason,
                    segment.actual_departure,
                    json.dumps(list(segment.quality_issues), ensure_ascii=False),
                    segment.raw_hash,
                )
                for segment in trajectory.segments
            ],
        )

    def _insert_conflicts(
        self, conn: sqlite3.Connection, version_id: int, trajectory: Trajectory
    ) -> None:
        conn.executemany(
            """INSERT INTO flight_lifecycle_conflicts(
                 version_id, flight_key, record_key, source, observed_at, status,
                 event_family, conflict_axis, decision, reason,
                 competing_record_key, competing_source, raw_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    version_id,
                    trajectory.flight_key,
                    conflict.record_key,
                    conflict.source,
                    conflict.observed_at,
                    conflict.status,
                    conflict.event_family,
                    conflict.conflict_axis,
                    conflict.decision,
                    conflict.reason,
                    conflict.competing_record_key,
                    conflict.competing_source,
                    conflict.raw_hash,
                )
                for conflict in trajectory.conflicts
            ],
        )

    def recover_orphans(self) -> list[str]:
        """Delete rows belonging to versions whose header never committed.

        A crash mid-transaction normally rolls everything back automatically;
        this additionally cleans up externally restored database files and
        documents the recovery step in the lock-event audit log.
        """

        recovered: list[str] = []
        with self._immediate() as conn:
            orphan_rows = conn.execute(
                """SELECT DISTINCT version_id FROM flight_lifecycle_states s
                   WHERE NOT EXISTS (
                       SELECT 1 FROM flight_lifecycle_versions v
                       WHERE v.version_id = s.version_id)"""
            ).fetchall()
            for row in orphan_rows:
                conn.execute(
                    "DELETE FROM flight_lifecycle_states WHERE version_id=?",
                    (row["version_id"],),
                )
                conn.execute(
                    "DELETE FROM flight_lifecycle_conflicts WHERE version_id=?",
                    (row["version_id"],),
                )
                recovered.append(str(row["version_id"]))
            stale_locks = conn.execute(
                "SELECT flight_key, owner FROM flight_lifecycle_locks"
            ).fetchall()
            for row in stale_locks:
                conn.execute(
                    """INSERT INTO flight_lifecycle_lock_events(
                         flight_key, event, owner, detail, created_at)
                       VALUES (?, 'lock_recovered', ?, 'cleared on store open', ?)""",
                    (row["flight_key"], row["owner"], _utcnow()),
                )
                conn.execute(
                    "DELETE FROM flight_lifecycle_locks WHERE flight_key=?",
                    (row["flight_key"],),
                )
        return recovered

    # ------------------------------------------------------------------ query
    def latest_version(self, key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM flight_lifecycle_versions WHERE flight_key=? ORDER BY version_no DESC LIMIT 1",
            (key,),
        ).fetchone()
        return dict(row) if row else None

    def list_versions(self, key: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.conn.execute(
                "SELECT * FROM flight_lifecycle_versions WHERE flight_key=? ORDER BY version_no",
                (key,),
            ).fetchall()
        ]

    def load_trajectory(self, key: str, version_id: int | None = None) -> dict[str, Any]:
        version = self._resolve_version(key, version_id)
        if version is None:
            return {
                "flight_key": key,
                "version_id": None,
                "segments": [],
                "conflicts": [],
            }
        segments = [
            self._segment_to_dict(row)
            for row in self.conn.execute(
                "SELECT * FROM flight_lifecycle_states WHERE version_id=? ORDER BY sequence_no",
                (version["version_id"],),
            ).fetchall()
        ]
        conflicts = [
            dict(row)
            for row in self.conn.execute(
                "SELECT * FROM flight_lifecycle_conflicts WHERE version_id=? ORDER BY conflict_id",
                (version["version_id"],),
            ).fetchall()
        ]
        result = dict(version)
        result["segments"] = segments
        result["conflicts"] = conflicts
        return result

    @staticmethod
    def _segment_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["adopted"] = bool(item["adopted"])
        item["quality_issues"] = json.loads(item.pop("quality_issues_json"))
        return item

    def _resolve_version(self, key: str, version_id: int | None) -> dict[str, Any] | None:
        if version_id is not None:
            row = self.conn.execute(
                "SELECT * FROM flight_lifecycle_versions WHERE version_id=? AND flight_key=?",
                (version_id, key),
            ).fetchone()
            return dict(row) if row else None
        row = self.conn.execute(
            "SELECT * FROM flight_lifecycle_versions WHERE flight_key=? ORDER BY version_no DESC LIMIT 1",
            (key,),
        ).fetchone()
        return dict(row) if row else None

    def export_trajectory(self, key: str, version_id: int | None = None) -> dict[str, Any]:
        """Full JSON-serialisable export, including provenance for every fact."""

        trajectory = self.load_trajectory(key, version_id)
        if trajectory.get("version_id") is None:
            return trajectory
        provenance: dict[str, dict[str, Any]] = {}
        for segment in trajectory["segments"]:
            provenance.setdefault(segment["record_key"], self._provenance(segment["record_key"]))
        for conflict in trajectory["conflicts"]:
            provenance.setdefault(conflict["record_key"], self._provenance(conflict["record_key"]))
        trajectory["provenance"] = provenance
        return trajectory

    def _provenance(self, record_key: str) -> dict[str, Any]:
        row = self.conn.execute(
            """SELECT record_key, source, observed_at, raw_hash, payload_json
               FROM flight_observations WHERE record_key=? LIMIT 1""",
            (record_key,),
        ).fetchone()
        if row is None:
            # Segment may reference a record only present in the legacy snapshot.
            legacy = self.conn.execute(
                """SELECT record_key, source, observed_at, raw_hash, payload_json
                   FROM records WHERE record_key=? LIMIT 1""",
                (record_key,),
            ).fetchone() if self._table_exists("records") else None
            row = legacy
        if row is None:
            raise LifecycleStoreError(f"provenance missing for record_key {record_key!r}")
        result = dict(row)
        result["payload"] = json.loads(result.pop("payload_json"))
        return result

    def verify_traceability(self, key: str, version_id: int | None = None) -> list[str]:
        """Return missing-provenance record keys; empty list means fully traceable."""

        trajectory = self.load_trajectory(key, version_id)
        missing: list[str] = []
        for segment in trajectory.get("segments", []):
            if not self._has_provenance(segment["record_key"]):
                missing.append(segment["record_key"])
        for conflict in trajectory.get("conflicts", []):
            if not self._has_provenance(conflict["record_key"]):
                missing.append(conflict["record_key"])
        return missing

    def _has_provenance(self, record_key: str) -> bool:
        if self.conn.execute(
            "SELECT 1 FROM flight_observations WHERE record_key=? LIMIT 1",
            (record_key,),
        ).fetchone():
            return True
        if self._table_exists("records") and self.conn.execute(
            "SELECT 1 FROM records WHERE record_key=? LIMIT 1", (record_key,)
        ).fetchone():
            return True
        return False

    # -------------------------------------------------------------- test hooks
    def set_fault(self, fault: str) -> None:
        """Inject a failure point (tests only): '' disables, otherwise raises RuntimeError."""

        self._fault = fault

    def _fault_point(self, point: str) -> None:
        if self._fault == point:
            self._fault = ""
            raise RuntimeError(f"injected failure at {point}")

    def close(self) -> None:
        self.conn.close()

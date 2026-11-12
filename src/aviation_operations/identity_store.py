"""SQLite-backed identity mapping store and queryable identity index.

The store is deliberately separate from the legacy :class:`DataStore`
tables:

* ``record_key`` values keep their historical meaning forever; the
  identity index only references them.
* Every schema change is a numbered, reversible migration.  A legacy
  database without identity tables is detected (schema version 0) and
  upgraded in place without touching existing data.
* Writers use WAL + a monotonic ``identity_write_version`` token so
  concurrent processes fail loudly instead of silently overwriting each
  other.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .identity import (
    BASIS_MANUAL,
    IdentityResolution,
    MappingBundle,
    ResolutionEvidence,
    STATUS_MANUAL,
    canonical_flight_key,
    replace,
)

IDENTITY_META_VERSION_KEY = "identity_write_version"

# Process-wide locks keyed by database location.  SQLite serializes the
# actual writes (BEGIN IMMEDIATE + busy timeout); these locks additionally
# make the read-token -> check -> write window atomic between threads
# sharing a file database.
_LOCK_REGISTRY: dict[str, threading.RLock] = {}
_LOCK_REGISTRY_GUARD = threading.Lock()


def database_lock(database: str) -> threading.RLock:
    key = database if database == ":memory:" else str(Path(database).resolve())
    with _LOCK_REGISTRY_GUARD:
        lock = _LOCK_REGISTRY.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCK_REGISTRY[key] = lock
        return lock


class IdentityStoreError(RuntimeError):
    """Base class for identity store failures."""


class IdentityIndexUnavailable(IdentityStoreError):
    """Raised when a legacy database has no identity schema yet."""


class ConcurrentUpdateError(IdentityStoreError):
    """Raised when an optimistic-concurrency check fails."""


class MappingVersionConflict(IdentityStoreError):
    """Raised when a mapping version exists with a different hash."""


@dataclass(frozen=True)
class Fault:
    """One-shot failure injection rule used by the test suite."""

    stage: str
    error: Exception


# ---------------------------------------------------------------------------
# Migrations (reversible, numbered)
# ---------------------------------------------------------------------------


def _migration_1() -> tuple[str, str]:
    up = """
    CREATE TABLE identity_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    INSERT INTO identity_meta(key, value) VALUES ('identity_write_version', '0');
    CREATE TABLE identity_schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL
    );
    CREATE TABLE mapping_bundles (
        mapping_version TEXT PRIMARY KEY,
        bundle_hash TEXT NOT NULL,
        collapse_leading_zeros INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        deactivated_at TEXT
    );
    CREATE TABLE mapping_rules (
        rule_id TEXT PRIMARY KEY,
        mapping_version TEXT NOT NULL,
        rule_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        revoked_at TEXT
    );
    CREATE TABLE manual_decisions (
        decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
        review_key TEXT NOT NULL,
        covered_fingerprints_json TEXT NOT NULL DEFAULT '[]',
        canonical_flight_key TEXT NOT NULL,
        mapping_version TEXT NOT NULL,
        bundle_hash TEXT NOT NULL,
        rationale TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        revoked_at TEXT
    );
    CREATE UNIQUE INDEX ux_manual_decisions_active
        ON manual_decisions(review_key) WHERE active = 1;
    CREATE TABLE identity_index (
        record_key TEXT NOT NULL,
        source TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        status TEXT NOT NULL,
        canonical_flight_key TEXT,
        review_key TEXT NOT NULL,
        mapping_version TEXT NOT NULL,
        bundle_hash TEXT NOT NULL,
        reasons_json TEXT NOT NULL,
        candidates_json TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        stale INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(record_key, fingerprint)
    );
    CREATE INDEX ix_identity_index_canonical
        ON identity_index(canonical_flight_key);
    CREATE INDEX ix_identity_index_review ON identity_index(review_key);
    CREATE INDEX ix_identity_index_status ON identity_index(status);
    CREATE TABLE identity_conflicts (
        conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
        review_key TEXT NOT NULL,
        reason TEXT NOT NULL,
        detail TEXT NOT NULL,
        candidates_json TEXT NOT NULL,
        mapping_version TEXT NOT NULL,
        resolved INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    CREATE TABLE identity_history (
        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_key TEXT,
        fingerprint TEXT,
        old_canonical_key TEXT,
        new_canonical_key TEXT,
        old_status TEXT,
        new_status TEXT,
        mapping_version_from TEXT,
        mapping_version_to TEXT,
        bundle_hash_from TEXT,
        bundle_hash_to TEXT,
        change_reason TEXT NOT NULL,
        changed_at TEXT NOT NULL
    );
    CREATE TABLE identity_failures (
        failure_id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_key TEXT,
        source TEXT,
        error TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        retried INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    """
    down = """
    DROP TABLE IF EXISTS identity_failures;
    DROP TABLE IF EXISTS identity_history;
    DROP TABLE IF EXISTS identity_conflicts;
    DROP TABLE IF EXISTS identity_index;
    DROP TABLE IF EXISTS manual_decisions;
    DROP TABLE IF EXISTS mapping_rules;
    DROP TABLE IF EXISTS mapping_bundles;
    DROP TABLE IF EXISTS identity_schema_migrations;
    DROP TABLE IF EXISTS identity_meta;
    """
    return up, down


MIGRATIONS: dict[int, tuple[str, Callable[[], tuple[str, str]]]] = {
    1: ("initial_identity_schema", _migration_1),
}


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(database: str, *, read_only: bool) -> sqlite3.Connection:
    if database == ":memory:":
        # Shared cache so separate connections inside one process see the
        # same in-memory database.  SQLite cannot attach read-only to a
        # shared in-memory db, so read mode keeps a normal connection;
        # the IdentityReader API itself exposes no mutating methods.
        uri = "file:aviation-identity-memory?mode=memory&cache=shared"
        conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    else:
        Path(database).parent.mkdir(parents=True, exist_ok=True)
        if read_only:
            uri = f"file:{database}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=30.0)
        else:
            conn = sqlite3.connect(database, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


class _IdentitySchema:
    """Version tracking shared by readers and writers."""

    @staticmethod
    def current_version(conn: sqlite3.Connection) -> int:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='identity_schema_migrations'"
        ).fetchone()
        if table is None:
            return 0
        row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM identity_schema_migrations").fetchone()
        return int(row[0])


# ---------------------------------------------------------------------------
# Read-only query interface
# ---------------------------------------------------------------------------


class IdentityReader:
    """Read-only access to the identity index.

    The connection is opened in SQLite read-only mode; nothing in this
    class can mutate the database.
    """

    def __init__(self, database: str) -> None:
        self.database = database
        self.conn = _connect(database, read_only=True)
        if _IdentitySchema.current_version(self.conn) == 0:
            self.conn.close()
            raise IdentityIndexUnavailable(
                "database has no identity schema; run IdentityWriter.migrate() and backfill first"
            )

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "IdentityReader":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def get_by_record_key(self, record_key: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM identity_index WHERE record_key=? ORDER BY updated_at DESC",
            (record_key,),
        ).fetchone()
        return dict(row) if row else None

    def get_by_canonical(self, canonical_flight_key: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM identity_index WHERE canonical_flight_key=? AND stale=0 ORDER BY record_key",
            (canonical_flight_key,),
        ).fetchall()
        return [dict(row) for row in rows]

    def find_review(self, review_key: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM identity_index WHERE review_key=? ORDER BY record_key", (review_key,)
        ).fetchall()
        return [dict(row) for row in rows]

    def search(
        self,
        *,
        status: str | None = None,
        canonical_flight_key: str | None = None,
        review_key: str | None = None,
        include_stale: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status=?")
            params.append(status)
        if canonical_flight_key:
            clauses.append("canonical_flight_key=?")
            params.append(canonical_flight_key)
        if review_key:
            clauses.append("review_key=?")
            params.append(review_key)
        if not include_stale:
            clauses.append("stale=0")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM identity_index {where} ORDER BY record_key LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
        return [dict(row) for row in rows]

    def conflicts(self, *, include_resolved: bool = False) -> list[dict[str, Any]]:
        where = "" if include_resolved else "WHERE resolved=0"
        rows = self.conn.execute(
            f"SELECT * FROM identity_conflicts {where} ORDER BY conflict_id"
        ).fetchall()
        return [dict(row) for row in rows]

    def history(self, record_key: str | None = None) -> list[dict[str, Any]]:
        if record_key:
            rows = self.conn.execute(
                "SELECT * FROM identity_history WHERE record_key=? ORDER BY history_id",
                (record_key,),
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM identity_history ORDER BY history_id").fetchall()
        return [dict(row) for row in rows]

    def manual_decisions(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        where = "WHERE active=1" if active_only else ""
        rows = self.conn.execute(
            f"SELECT * FROM manual_decisions {where} ORDER BY decision_id"
        ).fetchall()
        return [dict(row) for row in rows]

    def mapping_versions(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM mapping_bundles ORDER BY mapping_version"
        ).fetchall()
        return [dict(row) for row in rows]

    def active_mapping_version(self) -> str | None:
        row = self.conn.execute(
            "SELECT mapping_version FROM mapping_bundles WHERE active=1 ORDER BY mapping_version DESC LIMIT 1"
        ).fetchone()
        return str(row[0]) if row else None

    def failures(self, *, include_retried: bool = False) -> list[dict[str, Any]]:
        where = "" if include_retried else "WHERE retried=0"
        rows = self.conn.execute(
            f"SELECT * FROM identity_failures {where} ORDER BY failure_id"
        ).fetchall()
        return [dict(row) for row in rows]

    def write_version(self) -> int:
        row = self.conn.execute(
            "SELECT value FROM identity_meta WHERE key=?", (IDENTITY_META_VERSION_KEY,)
        ).fetchone()
        return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class IdentityWriter:
    """Mutating side of the identity store with migration and recovery ops."""

    def __init__(
        self,
        database: str,
        *,
        faults: Iterable[Fault] | None = None,
        on_change: Callable[[], None] | None = None,
    ) -> None:
        self.database = database
        self._lock = database_lock(database)
        self.conn = _connect(database, read_only=False)
        self._faults = list(faults or [])
        self._on_change = on_change

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "IdentityWriter":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def _maybe_fault(self, stage: str) -> None:
        for index, fault in enumerate(self._faults):
            if fault.stage == stage:
                self._faults.pop(index)
                raise fault.error

    def _notify(self) -> None:
        if self._on_change:
            self._on_change()

    # ------------------------------------------------------------------
    # Migrations
    # ------------------------------------------------------------------

    def current_version(self) -> int:
        return _IdentitySchema.current_version(self.conn)

    def latest_version(self) -> int:
        return max(MIGRATIONS)

    def migrate(self, target_version: int | None = None) -> int:
        target = self.latest_version() if target_version is None else target_version
        current = self.current_version()
        if target > self.latest_version():
            raise IdentityStoreError(f"unknown schema version {target}")
        if target > current:
            for version in range(current + 1, target + 1):
                name, factory = MIGRATIONS[version]
                up, _down = factory()
                self.conn.executescript(up)
                self.conn.execute(
                    "INSERT INTO identity_schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (version, name, _utcnow()),
                )
                self._maybe_fault(f"after_migration_{version}")
            self.conn.commit()
        return self.current_version()

    def rollback(self, target_version: int = 0) -> int:
        """Roll back identity tables step by step.

        Legacy domain tables (``records``, ``batches`` ...) are never
        touched, so rollback only ever removes identity data.
        """

        current = self.current_version()
        if target_version > current:
            raise IdentityStoreError("rollback target is newer than current version")
        for version in range(current, target_version, -1):
            _name, factory = MIGRATIONS[version]
            _up, down = factory()
            self.conn.executescript(down)
            self._maybe_fault(f"after_rollback_{version}")
        self.conn.commit()
        return self.current_version()

    # ------------------------------------------------------------------
    # Optimistic write token
    # ------------------------------------------------------------------

    def write_version(self) -> int:
        row = self.conn.execute(
            "SELECT value FROM identity_meta WHERE key=?", (IDENTITY_META_VERSION_KEY,)
        ).fetchone()
        return int(row[0]) if row else 0

    def _bump_write_version(self, expected: int | None) -> int:
        current = self.write_version()
        if expected is not None and expected != current:
            raise ConcurrentUpdateError(
                f"identity write version {current} differs from expected {expected}"
            )
        self.conn.execute(
            "UPDATE identity_meta SET value=? WHERE key=?",
            (str(current + 1), IDENTITY_META_VERSION_KEY),
        )
        return current + 1

    def _check_write_version(self, expected: int | None) -> None:
        """Atomic optimistic-token check inside an open write transaction."""

        if expected is None:
            return
        row = self.conn.execute(
            "SELECT value FROM identity_meta WHERE key=?", (IDENTITY_META_VERSION_KEY,)
        ).fetchone()
        current = int(row[0]) if row else 0
        if expected != current:
            raise ConcurrentUpdateError(
                f"identity write version {current} differs from expected {expected}"
            )

    # ------------------------------------------------------------------
    # Mapping bundles
    # ------------------------------------------------------------------

    def activate_bundle(
        self, bundle: MappingBundle, *, expected_write_version: int | None = None
    ) -> int:
        """Install and activate a mapping version atomically.

        A version already stored with a different ``bundle_hash`` is
        rejected: mapping versions are immutable content references.
        """

        self.migrate()
        with self._lock:
            active = self.conn.execute(
                "SELECT bundle_hash FROM mapping_bundles WHERE active=1"
            ).fetchone()
            if active is not None and active["bundle_hash"] == bundle.bundle_hash:
                # Re-activating the identical bundle is a no-op: no token
                # bump, no stale marking, fully repeatable runs.
                self.conn.commit()
                return self.write_version()
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self._check_write_version(expected_write_version)
                existing = self.conn.execute(
                    "SELECT bundle_hash FROM mapping_bundles WHERE mapping_version=?",
                    (bundle.version,),
                ).fetchone()
                if existing is not None and existing["bundle_hash"] != bundle.bundle_hash:
                    raise MappingVersionConflict(
                        f"mapping version {bundle.version} already exists with a different hash"
                    )
                self.conn.execute(
                    """INSERT INTO mapping_bundles(mapping_version, bundle_hash,
                           collapse_leading_zeros, active, created_at)
                       VALUES (?, ?, ?, 0, ?)
                       ON CONFLICT(mapping_version) DO UPDATE SET
                           collapse_leading_zeros=excluded.collapse_leading_zeros""",
                    (
                        bundle.version,
                        bundle.bundle_hash,
                        int(bundle.collapse_leading_zeros),
                        _utcnow(),
                    ),
                )
                self._maybe_fault("after_bundle_insert")
                self._replace_rules(bundle)
                self._maybe_fault("after_rules_insert")
                self.conn.execute(
                    "UPDATE mapping_bundles SET active=0, deactivated_at=?", (_utcnow(),)
                )
                self.conn.execute(
                    "UPDATE mapping_bundles SET active=1, deactivated_at=NULL WHERE mapping_version=?",
                    (bundle.version,),
                )
                new_version = self._bump_write_version(expected_write_version)
                self._maybe_fault("before_bundle_commit")
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        self._notify()
        return new_version

    def _replace_rules(self, bundle: MappingBundle) -> None:
        now = _utcnow()
        rows: list[tuple[str, str, str, str, str]] = []
        for rule in bundle.airline_aliases:
            rows.append(
                (
                    rule.rule_id,
                    bundle.version,
                    "airline_alias",
                    json.dumps(rule.__dict__, ensure_ascii=False, sort_keys=True),
                    now,
                )
            )
        for rule in bundle.flight_aliases:
            rows.append(
                (
                    rule.rule_id,
                    bundle.version,
                    "flight_alias",
                    json.dumps(rule.__dict__, ensure_ascii=False, sort_keys=True),
                    now,
                )
            )
        for rule in bundle.codeshares:
            rows.append(
                (
                    rule.rule_id,
                    bundle.version,
                    "codeshare",
                    json.dumps(rule.__dict__, ensure_ascii=False, sort_keys=True),
                    now,
                )
            )
        for rule_id, mapping_version, rule_type, payload, created_at in rows:
            conflict = self.conn.execute(
                "SELECT payload_json FROM mapping_rules WHERE rule_id=?", (rule_id,)
            ).fetchone()
            if conflict is not None and conflict["payload_json"] != payload:
                raise MappingVersionConflict(
                    f"rule_id {rule_id} already exists with different content"
                )
            self.conn.execute(
                """INSERT INTO mapping_rules(rule_id, mapping_version, rule_type, payload_json, created_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(rule_id) DO NOTHING""",
                (rule_id, mapping_version, rule_type, payload, created_at),
            )

    def load_active_bundle(self) -> MappingBundle:
        """Reconstruct the active mapping bundle from stored rules."""

        from .identity import (
            AirlineAlias,
            CodeshareMembership,
            FlightAlias,
            MappingBundle as Bundle,
        )

        version_row = self.conn.execute(
            "SELECT mapping_version, collapse_leading_zeros FROM mapping_bundles WHERE active=1 "
            "ORDER BY mapping_version DESC LIMIT 1"
        ).fetchone()
        if version_row is None:
            raise IdentityStoreError("no active mapping bundle")
        rules = self.conn.execute(
            "SELECT rule_type, payload_json FROM mapping_rules WHERE mapping_version=? AND revoked=0",
            (version_row["mapping_version"],),
        ).fetchall()
        airlines: list[AirlineAlias] = []
        flights: list[FlightAlias] = []
        codeshares: list[CodeshareMembership] = []
        for row in rules:
            payload = json.loads(row["payload_json"])
            if row["rule_type"] == "airline_alias":
                airlines.append(AirlineAlias(**payload))
            elif row["rule_type"] == "flight_alias":
                flights.append(FlightAlias(**payload))
            elif row["rule_type"] == "codeshare":
                codeshares.append(CodeshareMembership(**payload))
        return Bundle(
            version=version_row["mapping_version"],
            airline_aliases=tuple(airlines),
            flight_aliases=tuple(flights),
            codeshares=tuple(codeshares),
            collapse_leading_zeros=bool(version_row["collapse_leading_zeros"]),
        )

    # ------------------------------------------------------------------
    # Manual decisions
    # ------------------------------------------------------------------

    def save_manual_decision(
        self,
        review_key: str,
        canonical_flight_key: str,
        mapping_version: str,
        bundle_hash: str,
        rationale: str = "",
        covered_fingerprints: Iterable[str] | None = None,
        *,
        expected_write_version: int | None = None,
    ) -> int:
        if not canonical_flight_key.startswith("FV2|"):
            raise IdentityStoreError("manual decision must target an FV2 canonical flight key")
        fingerprints = sorted(set(covered_fingerprints or ()))
        self.migrate()
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self._check_write_version(expected_write_version)
                self.conn.execute(
                    """INSERT INTO manual_decisions(review_key, covered_fingerprints_json,
                           canonical_flight_key, mapping_version, bundle_hash, rationale, active,
                           created_at)
                       VALUES (?, ?, ?, ?, ?, ?, 1, ?)""",
                    (
                        review_key,
                        json.dumps(fingerprints, ensure_ascii=False, sort_keys=True),
                        canonical_flight_key,
                        mapping_version,
                        bundle_hash,
                        rationale,
                        _utcnow(),
                    ),
                )
                decision_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                self._bump_write_version(expected_write_version)
                self._maybe_fault("before_manual_commit")
                self.conn.commit()
            except sqlite3.IntegrityError as exc:
                self.conn.rollback()
                raise ConcurrentUpdateError(
                    f"an active manual decision already exists for {review_key}"
                ) from exc
            except Exception:
                self.conn.rollback()
                raise
        self._notify()
        return decision_id

    def withdraw_manual_decision(
        self, review_key: str, *, expected_write_version: int | None = None
    ) -> int:
        """Revoke a manual mapping and mark affected index rows stale."""

        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                decision = self.conn.execute(
                    "SELECT * FROM manual_decisions WHERE review_key=? AND active=1",
                    (review_key,),
                ).fetchone()
                if decision is None:
                    raise IdentityStoreError(f"no active manual decision for {review_key}")
                self.conn.execute(
                    "UPDATE manual_decisions SET active=0, revoked_at=? WHERE review_key=? AND active=1",
                    (_utcnow(), review_key),
                )
                fingerprints = json.loads(decision["covered_fingerprints_json"] or "[]")
                self.conn.execute(
                    "UPDATE identity_index SET stale=1 WHERE status=? AND ("
                    "review_key=? OR fingerprint IN ({}))".format(
                        ",".join("?" for _ in fingerprints) if fingerprints else "''"
                    ),
                    (STATUS_MANUAL, review_key, *fingerprints),
                )
                new_version = self._bump_write_version(expected_write_version)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        self._notify()
        return new_version

    # ------------------------------------------------------------------
    # Identity index writes
    # ------------------------------------------------------------------

    def write_identity(
        self,
        record_key: str,
        source: str,
        resolution: IdentityResolution,
        *,
        expected_write_version: int | None = None,
    ) -> bool:
        """Persist one resolution; record history when the key changes."""

        self.migrate()
        now = _utcnow()
        changed = False
        reasons_json = json.dumps(list(resolution.reasons), ensure_ascii=False, sort_keys=True)
        candidates_json = json.dumps(
            list(resolution.candidates), ensure_ascii=False, sort_keys=True
        )
        evidence_json = json.dumps(
            [e.__dict__ for e in resolution.evidence], ensure_ascii=False, sort_keys=True
        )
        self._lock.acquire()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self._check_write_version(expected_write_version)
            previous = self.conn.execute(
                "SELECT * FROM identity_index WHERE record_key=? AND fingerprint=?",
                (record_key, resolution.fingerprint),
            ).fetchone()
            if previous is not None:
                content_changed = (
                    previous["canonical_flight_key"] != resolution.canonical_flight_key
                    or previous["status"] != resolution.status
                    or previous["review_key"] != resolution.review_key
                    or previous["mapping_version"] != resolution.mapping_version
                    or previous["bundle_hash"] != resolution.bundle_hash
                    or previous["reasons_json"] != reasons_json
                    or previous["candidates_json"] != candidates_json
                    or previous["evidence_json"] != evidence_json
                    or previous["source"] != source
                )
                if not content_changed and not previous["stale"]:
                    # Fully idempotent re-write: no row update, no token bump.
                    self.conn.commit()
                    return False
                if (
                    previous["canonical_flight_key"] != resolution.canonical_flight_key
                    or previous["status"] != resolution.status
                ):
                    self.conn.execute(
                        """INSERT INTO identity_history(record_key, fingerprint, old_canonical_key,
                               new_canonical_key, old_status, new_status, mapping_version_from,
                               mapping_version_to, bundle_hash_from, bundle_hash_to, change_reason,
                               changed_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            record_key,
                            resolution.fingerprint,
                            previous["canonical_flight_key"],
                            resolution.canonical_flight_key,
                            previous["status"],
                            resolution.status,
                            previous["mapping_version"],
                            resolution.mapping_version,
                            previous["bundle_hash"],
                            resolution.bundle_hash,
                            "identity_recomputed",
                            now,
                        ),
                    )
                    changed = True
                self.conn.execute(
                    """UPDATE identity_index SET source=?, status=?, canonical_flight_key=?,
                           review_key=?, mapping_version=?, bundle_hash=?, reasons_json=?,
                           candidates_json=?, evidence_json=?, stale=0, updated_at=?
                       WHERE record_key=? AND fingerprint=?""",
                    (
                        source,
                        resolution.status,
                        resolution.canonical_flight_key,
                        resolution.review_key,
                        resolution.mapping_version,
                        resolution.bundle_hash,
                        reasons_json,
                        candidates_json,
                        evidence_json,
                        now,
                        record_key,
                        resolution.fingerprint,
                    ),
                )
            else:
                self.conn.execute(
                    """INSERT INTO identity_index(record_key, source, fingerprint, status,
                           canonical_flight_key, review_key, mapping_version, bundle_hash,
                           reasons_json, candidates_json, evidence_json, stale, created_at,
                           updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)""",
                    (
                        record_key,
                        source,
                        resolution.fingerprint,
                        resolution.status,
                        resolution.canonical_flight_key,
                        resolution.review_key,
                        resolution.mapping_version,
                        resolution.bundle_hash,
                        reasons_json,
                        candidates_json,
                        evidence_json,
                        now,
                        now,
                    ),
                )
                changed = True
            self._bump_write_version(expected_write_version)
            self._maybe_fault("before_identity_commit")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self._lock.release()
        return changed

    def record_conflict(self, conflict: Any, mapping_version: str) -> None:
        self.migrate()
        self.conn.execute(
            """INSERT INTO identity_conflicts(review_key, reason, detail, candidates_json,
                   mapping_version, resolved, created_at)
               VALUES (?, ?, ?, ?, ?, 0, ?)
               ON CONFLICT DO NOTHING""",
            (
                conflict.review_key,
                conflict.reason,
                conflict.detail,
                json.dumps(list(conflict.candidates), ensure_ascii=False, sort_keys=True),
                mapping_version,
                _utcnow(),
            ),
        )
        self.conn.commit()

    def record_failure(self, record_key: str | None, source: str | None, error: str, payload: Any) -> None:
        self.migrate()
        self.conn.execute(
            """INSERT INTO identity_failures(record_key, source, error, payload_json, retried, created_at)
               VALUES (?, ?, ?, ?, 0, ?)""",
            (record_key, source, error, json.dumps(payload, ensure_ascii=False, sort_keys=True), _utcnow()),
        )
        self.conn.commit()

    def mark_stale_for_mapping(self, bundle_hash: str) -> int:
        cursor = self.conn.execute(
            "UPDATE identity_index SET stale=1 WHERE bundle_hash<>?", (bundle_hash,)
        )
        self.conn.commit()
        return cursor.rowcount

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def backfill_from_records(
        self, resolve_callable: Callable[[dict[str, Any]], IdentityResolution]
    ) -> dict[str, int]:
        """Resolve every legacy ``records`` row that lacks an identity row.

        Failures are captured in ``identity_failures`` and can be retried
        with :meth:`retry_failures`; they never abort the whole backfill.
        """

        self.migrate()
        legacy_table = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
        ).fetchone()
        if legacy_table is None:
            # A legacy database without the domain table has nothing to
            # backfill yet; this is a recoverable no-op, not an error.
            return {"processed": 0, "failed": 0, "skipped": 0}
        rows = self.conn.execute(
            "SELECT record_key, source, payload_json FROM records ORDER BY record_key"
        ).fetchall()
        indexed = {
            row[0]
            for row in self.conn.execute("SELECT DISTINCT record_key FROM identity_index").fetchall()
        }
        processed = failed = skipped = 0
        for row in rows:
            if row["record_key"] in indexed:
                skipped += 1
                continue
            processed += 1
            payload = json.loads(row["payload_json"])
            try:
                resolution = resolve_callable(payload)
                self.write_identity(row["record_key"], row["source"], resolution)
            except Exception as exc:  # captured for retry, never swallowed silently
                failed += 1
                self.record_failure(row["record_key"], row["source"], str(exc), payload)
        self._notify()
        return {"processed": processed, "failed": failed, "skipped": skipped}

    def retry_failures(
        self, resolve_callable: Callable[[dict[str, Any]], IdentityResolution]
    ) -> dict[str, int]:
        self.migrate()
        rows = self.conn.execute(
            "SELECT failure_id, record_key, source, payload_json FROM identity_failures "
            "WHERE retried=0 ORDER BY failure_id"
        ).fetchall()
        retried = recovered = 0
        for row in rows:
            retried += 1
            payload = json.loads(row["payload_json"])
            try:
                resolution = resolve_callable(payload)
                self.write_identity(row["record_key"], row["source"], resolution)
                self.conn.execute(
                    "UPDATE identity_failures SET retried=1 WHERE failure_id=?", (row["failure_id"],)
                )
                self.conn.commit()
                recovered += 1
            except Exception as exc:
                self.conn.execute(
                    "UPDATE identity_failures SET error=?, retried=1 WHERE failure_id=?",
                    (str(exc), row["failure_id"]),
                )
                self.conn.commit()
        self._notify()
        return {"retried": retried, "recovered": recovered}

    def reindex_all(
        self, resolve_callable: Callable[[dict[str, Any]], IdentityResolution]
    ) -> dict[str, int]:
        """Re-resolve the whole index after a mapping change.

        Old rows are kept as history (via :meth:`write_identity`); stale
        flags are cleared for every fingerprint that re-resolves.
        """

        self.migrate()
        rows = self.conn.execute(
            "SELECT DISTINCT record_key, source, fingerprint FROM identity_index ORDER BY record_key"
        ).fetchall()
        # Pull the original payloads from the legacy records table when
        # available; otherwise re-resolution is impossible and the row
        # stays flagged stale for manual review.
        has_legacy = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
        ).fetchone()
        legacy = (
            {
                row["record_key"]: row
                for row in self.conn.execute(
                    "SELECT record_key, source, payload_json FROM records"
                ).fetchall()
            }
            if has_legacy
            else {}
        )
        updated = still_stale = 0
        for row in rows:
            source_row = legacy.get(row["record_key"])
            if source_row is None:
                still_stale += 1
                continue
            payload = json.loads(source_row["payload_json"])
            try:
                resolution = resolve_callable(payload)
                self.write_identity(row["record_key"], source_row["source"], resolution)
                updated += 1
            except Exception as exc:
                still_stale += 1
                self.record_failure(row["record_key"], row["source"], str(exc), payload)
        self._notify()
        return {"updated": updated, "still_stale": still_stale}


def apply_manual_decision(resolution: IdentityResolution, decision: dict[str, Any]) -> IdentityResolution:
    """Apply a stored manual decision to an unresolved resolution."""

    evidence = resolution.evidence + (
        ResolutionEvidence(
            basis=BASIS_MANUAL,
            detail=f"manual decision {decision.get('rationale') or 'operator override'}",
            rule_id=str(decision.get("decision_id")),
            input_value=resolution.review_key,
            output_value=decision["canonical_flight_key"],
        ),
    )
    return replace(
        resolution,
        status=STATUS_MANUAL,
        canonical_flight_key=decision["canonical_flight_key"],
        evidence=evidence,
        reasons=tuple(sorted({*resolution.reasons, "manually_confirmed"})),
    )

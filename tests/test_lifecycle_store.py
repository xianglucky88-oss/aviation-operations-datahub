"""SQLite read-model tests: locking, versions, crash recovery and migration."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from aviation_operations.application import run_fixture
from aviation_operations.lifecycle_model import KEEP_CONFLICT, REJECT, LifecycleConfig, ResourceLimitExceeded
from aviation_operations.lifecycle_store import FlightLockConflict, LifecycleStore
from aviation_operations.normalize import normalize
from aviation_operations.storage import DataStore

from .lifecycle_builders import full_trajectory_rows, raw_observation, shuffled

FLIGHT_KEY = "CA100|2026-11-01"


@pytest.fixture()
def database(tmp_path: Path) -> str:
    return str(tmp_path / "lifecycle.sqlite3")


def seed(store: LifecycleStore, rows: list[dict]) -> None:
    for row in rows:
        assert store.index_record(normalize(row, row["source"])) is True


def test_rebuild_empty_flight_publishes_nothing_and_is_explainable(database):
    store = LifecycleStore(database)
    try:
        result = store.rebuild_flight("CA100", "2026-11-01")
        assert result == {
            "flight_key": FLIGHT_KEY,
            "version_id": None,
            "version_no": 0,
            "published": False,
            "reason": "no_observations",
            "observation_count": 0,
        }
        assert store.list_versions(FLIGHT_KEY) == []
    finally:
        store.close()


def test_incremental_rebuild_only_reads_one_flight_index(database):
    store = LifecycleStore(database)
    try:
        seed(store, full_trajectory_rows())
        other = raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:00:00Z",
            status="scheduled",
            flight_id="MU999",
        )
        store.index_record(normalize(other, other["source"]))
        result = store.rebuild_flight("CA100", "2026-11-01")
        assert result["published"] is True
        assert result["observation_count"] == 5
        loaded = store.load_trajectory(FLIGHT_KEY)
        assert loaded["flight_key"] == FLIGHT_KEY
        assert [s["status"] for s in loaded["segments"] if s["adopted"]] == [
            "scheduled",
            "boarding",
            "delayed",
            "departed",
            "arrived",
        ]
        # The other flight was never rebuilt.
        assert store.latest_version("MU999|2026-11-01") is None
    finally:
        store.close()


def test_identical_replay_does_not_create_duplicate_version(database):
    store = LifecycleStore(database)
    try:
        seed(store, shuffled(full_trajectory_rows()))
        first = store.rebuild_flight("CA100", "2026-11-01")
        second = store.rebuild_flight("CA100", "2026-11-01")
        third = store.rebuild_flight("CA100", "2026-11-01")
        assert first["published"] is True
        assert second["published"] is False and second["reason"] == "unchanged_input"
        assert third["version_id"] == first["version_id"]
        assert len(store.list_versions(FLIGHT_KEY)) == 1
    finally:
        store.close()


def test_new_observation_appends_a_new_version_and_keeps_history(database):
    store = LifecycleStore(database)
    try:
        seed(store, full_trajectory_rows()[:3])
        v1 = store.rebuild_flight("CA100", "2026-11-01")
        seed(store, full_trajectory_rows()[3:])
        v2 = store.rebuild_flight("CA100", "2026-11-01")
        assert v2["published"] is True
        assert v2["version_no"] == v1["version_no"] + 1
        versions = store.list_versions(FLIGHT_KEY)
        assert [v["version_no"] for v in versions] == [1, 2]
        # Historical version remains readable and immutable.
        history = store.load_trajectory(FLIGHT_KEY, version_id=v1["version_id"])
        assert len(history["segments"]) == 3
        latest = store.load_trajectory(FLIGHT_KEY)
        assert len(latest["segments"]) == 5
    finally:
        store.close()


def test_force_republish_creates_new_version_even_without_input_change(database):
    store = LifecycleStore(database)
    try:
        seed(store, full_trajectory_rows())
        v1 = store.rebuild_flight("CA100", "2026-11-01")
        v2 = store.rebuild_flight("CA100", "2026-11-01", force=True)
        assert v2["version_no"] == v1["version_no"] + 1
        assert v2["input_fingerprint"] == v1["input_fingerprint"]
    finally:
        store.close()


def test_configuration_change_publishes_a_new_auditable_version(database):
    store = LifecycleStore(database)
    try:
        seed(store, full_trajectory_rows())
        v1 = store.rebuild_flight("CA100", "2026-11-01", LifecycleConfig(transition_policy=REJECT))
        v2 = store.rebuild_flight("CA100", "2026-11-01", LifecycleConfig(transition_policy=KEEP_CONFLICT))
        assert v2["published"] is True
        header = store.latest_version(FLIGHT_KEY)
        config = json.loads(header["config_json"])
        assert config["transition_policy"] == KEEP_CONFLICT
        assert header["config_fingerprint"] != store.list_versions(FLIGHT_KEY)[0]["config_fingerprint"]
    finally:
        store.close()


def test_concurrent_revisions_serialise_and_never_overwrite(database):
    store = LifecycleStore(database)
    seed(store, full_trajectory_rows()[:2])
    store.close()

    outcomes: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def revise(owner: str, extra_rows: list[dict]) -> None:
        worker = LifecycleStore(database, lock_owner=owner, busy_timeout_ms=300)
        try:
            barrier.wait(timeout=5)
            try:
                with worker.flight_lock(FLIGHT_KEY):
                    seed(worker, extra_rows)
                    worker.rebuild_flight("CA100", "2026-11-01")
                    time.sleep(0.4)
                outcomes[owner] = "committed"
            except FlightLockConflict as exc:
                outcomes[owner] = f"conflict:{exc}"
        except Exception as exc:  # pragma: no cover - surfaced in assertion
            outcomes[owner] = f"error:{type(exc).__name__}:{exc}"
        finally:
            worker.close()

    t1 = threading.Thread(target=revise, args=("worker-a", full_trajectory_rows()[2:4]))
    t2 = threading.Thread(target=revise, args=("worker-b", full_trajectory_rows()[4:]))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # One worker commits; the other must be denied, never silently overwriting.
    assert any(value == "committed" for value in outcomes.values())
    assert any(value.startswith("conflict") for value in outcomes.values())
    auditor = LifecycleStore(database)
    try:
        events = [
            row["event"]
            for row in auditor.conn.execute(
                "SELECT event FROM flight_lifecycle_lock_events WHERE flight_key=? ORDER BY event_id",
                (FLIGHT_KEY,),
            ).fetchall()
        ]
        assert "lock_acquired" in events
        # Denial is either a logical lock_denied audit row or a busy-timeout
        # conflict; both must leave no lock behind.
        assert auditor.conn.execute(
            "SELECT COUNT(*) FROM flight_lifecycle_locks"
        ).fetchone()[0] == 0
        assert all(not value.startswith("error") for value in outcomes.values())
        # One coherent latest version with monotonically ordered states.
        latest = auditor.load_trajectory(FLIGHT_KEY)
        order = {"scheduled": 0, "boarding": 1, "delayed": 2, "departed": 3, "arrived": 4, "cancelled": 5}
        statuses = [s["status"] for s in latest["segments"] if s["adopted"]]
        assert statuses == sorted(statuses, key=order.get)
        auditor.close()

        # The denied worker retries after release: apply its pending rows and
        # rebuild, which must publish exactly one additional version.
        denied_rows = (
            full_trajectory_rows()[4:]
            if outcomes["worker-b"].startswith("conflict")
            else full_trajectory_rows()[2:4]
        )
        retry = LifecycleStore(database, lock_owner="worker-retry", busy_timeout_ms=2000)
        try:
            seed(retry, denied_rows)
            result = retry.rebuild_flight("CA100", "2026-11-01")
            assert result["published"] is True
            versions = retry.list_versions(FLIGHT_KEY)
            assert len(versions) == 2
            # A second retry with no new input is a no-op, never a duplicate.
            again = retry.rebuild_flight("CA100", "2026-11-01")
            assert again["published"] is False
            assert len(retry.list_versions(FLIGHT_KEY)) == 2
        finally:
            retry.close()
    except Exception:
        auditor.close()
        raise


def test_lock_conflict_is_raised_while_another_owner_holds_lock(database):
    holder = LifecycleStore(database, lock_owner="holder")
    holder.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    with holder.flight_lock(FLIGHT_KEY):
        # Same-owner re-entry on the same connection is reentrant.
        with holder.flight_lock(FLIGHT_KEY):
            assert holder._held_locks[FLIGHT_KEY] == 2
        assert holder._held_locks[FLIGHT_KEY] == 1
        contender = LifecycleStore(database, lock_owner="contender", busy_timeout_ms=200)
        try:
            with pytest.raises(FlightLockConflict):
                contender.flight_lock(FLIGHT_KEY).__enter__()
        finally:
            contender.close()
    assert FLIGHT_KEY not in holder._held_locks
    holder.close()


def test_busy_timeout_is_surfaced_as_a_typed_lock_conflict(database):
    holder = LifecycleStore(database, lock_owner="holder")
    holder.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    holder.conn.execute("BEGIN IMMEDIATE")  # hold the SQLite write lock open
    contender = LifecycleStore(database, lock_owner="contender", busy_timeout_ms=100)
    try:
        with pytest.raises(FlightLockConflict, match="locked by another process"):
            with contender.flight_lock(FLIGHT_KEY):
                pass
    finally:
        contender.close()
        holder.conn.rollback()
        holder.close()


def test_orphaned_lock_row_for_same_owner_is_reclaimed(database):
    first = LifecycleStore(database, lock_owner="same-owner")
    first.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    # Write a live-looking row for our own owner without the in-memory counter.
    with first._immediate() as conn:
        conn.execute(
            "INSERT INTO flight_lifecycle_locks(flight_key, owner, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (FLIGHT_KEY, "same-owner", "2026-01-01T00:00:00+00:00", "2030-01-01T00:00:00+00:00"),
        )
    first.close()

    second = LifecycleStore(database, lock_owner="same-owner")
    try:
        with second.flight_lock(FLIGHT_KEY):
            events = [
                row["event"]
                for row in second.conn.execute(
                    "SELECT event FROM flight_lifecycle_lock_events WHERE flight_key=?",
                    (FLIGHT_KEY,),
                ).fetchall()
            ]
            assert "lock_reclaimed" in events
    finally:
        second.close()


def test_recover_orphans_clears_stale_locks_and_reports_recovery(database):
    store = LifecycleStore(database, lock_owner="zombie")
    store.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    with store._immediate() as conn:
        conn.execute(
            "INSERT INTO flight_lifecycle_locks(flight_key, owner, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (FLIGHT_KEY, "zombie", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"),
        )
    assert store.recover_orphans() == []
    assert store.conn.execute(
        "SELECT COUNT(*) FROM flight_lifecycle_locks WHERE flight_key=?", (FLIGHT_KEY,)
    ).fetchone()[0] == 0
    events = [
        row["event"]
        for row in store.conn.execute(
            "SELECT event FROM flight_lifecycle_lock_events WHERE flight_key=?", (FLIGHT_KEY,)
        ).fetchall()
    ]
    assert "lock_recovered" in events
    store.close()


def test_provenance_falls_back_to_legacy_records_snapshot(tmp_path):
    db_path = tmp_path / "mixed.sqlite3"
    legacy = DataStore(str(db_path))
    batch_id = legacy.start_batch("legacy")
    row = full_trajectory_rows()[0]
    record = normalize(row, row["source"])
    legacy.upsert(record)
    legacy.finish_batch(batch_id, "completed", 1, 1, 0)
    legacy.close()

    store = LifecycleStore(str(db_path))
    try:
        # A version referencing a key absent from the new index but present in records.
        store.rebuild_flight("CA100", "2026-11-01")
        exported = store.export_trajectory(FLIGHT_KEY)
        assert exported["provenance"][record.record_key]["source"] == record.source
        assert store.verify_traceability(FLIGHT_KEY) == []
    finally:
        store.close()


def test_missing_provenance_is_reported_not_hidden(database):
    store = LifecycleStore(database)
    rows = [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T08:30:00Z",
            status="arrived",
            actual_departure="2026-11-01T06:48:00Z",
        ),
        raw_observation(source="airport-a", observed_at="2026-11-01T09:00:00Z", status="boarding"),
    ]
    seed(store, rows)
    store.rebuild_flight("CA100", "2026-11-01")
    # Corrupt the index to simulate facts with no backing record.
    store.conn.execute("DELETE FROM flight_observations")
    store.conn.commit()
    missing = store.verify_traceability(FLIGHT_KEY)
    assert len(missing) >= 2  # both the adopted segment and the transition conflict
    with pytest.raises(Exception):
        store.export_trajectory(FLIGHT_KEY)
    store.close()


def test_stale_foreign_lock_is_reclaimed_inside_lock_acquisition(database):
    writer = LifecycleStore(database, lock_owner="crashed-owner")
    writer.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    with writer._immediate() as conn:
        conn.execute(
            "INSERT INTO flight_lifecycle_locks(flight_key, owner, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (FLIGHT_KEY, "crashed-owner", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"),
        )
    writer.close()

    successor = LifecycleStore(database, lock_owner="successor")
    try:
        with successor.flight_lock(FLIGHT_KEY):
            result = successor.rebuild_flight("CA100", "2026-11-01")
            assert result["published"] is True
        events = [
            row["event"]
            for row in successor.conn.execute(
                "SELECT event FROM flight_lifecycle_lock_events WHERE flight_key=?", (FLIGHT_KEY,)
            ).fetchall()
        ]
        # Reclamation may happen either at store open or during lock acquisition.
        assert "lock_recovered" in events or "lock_reclaimed" in events
    finally:
        successor.close()


def test_stale_foreign_lock_expiring_between_open_and_acquire_is_reclaimed(database):
    writer = LifecycleStore(database, lock_owner="crashed-owner")
    writer.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    # Simulate a crashed foreign owner: a short-lived lock row that will never
    # be released, inserted directly.
    with writer._immediate() as conn:
        conn.execute(
            "INSERT INTO flight_lifecycle_locks(flight_key, owner, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (FLIGHT_KEY, "crashed-owner", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"),
        )
    writer.close()

    # Successor opens while the row is still present (it is already expired,
    # but we force acquisition-time reclamation by starting a second instance
    # against a copy of the locked state).
    successor = LifecycleStore(database, lock_owner="successor")
    # Open-time recovery clears expired rows; insert a fresh expired row to
    # exercise the acquisition branch deterministically.
    with successor._immediate() as conn:
        conn.execute(
            "INSERT INTO flight_lifecycle_locks(flight_key, owner, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (FLIGHT_KEY, "crashed-owner", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"),
        )
    try:
        with successor.flight_lock(FLIGHT_KEY):
            result = successor.rebuild_flight("CA100", "2026-11-01")
            assert result["published"] is True
        events = [
            row["event"]
            for row in successor.conn.execute(
                "SELECT event FROM flight_lifecycle_lock_events WHERE flight_key=?", (FLIGHT_KEY,)
            ).fetchall()
        ]
        assert "lock_reclaimed" in events
    finally:
        successor.close()


def test_query_and_export_of_unbuilt_flight_return_empty_shapes(database):
    store = LifecycleStore(database)
    try:
        loaded = store.load_trajectory(FLIGHT_KEY)
        assert loaded == {"flight_key": FLIGHT_KEY, "version_id": None, "segments": [], "conflicts": []}
        exported = store.export_trajectory(FLIGHT_KEY)
        assert exported["version_id"] is None
        assert store.latest_version(FLIGHT_KEY) is None
    finally:
        store.close()


def test_reentrant_lock_release_decrements_without_deleting_early(database):
    store = LifecycleStore(database, lock_owner="nested")
    with store.flight_lock(FLIGHT_KEY):
        assert store._held_locks[FLIGHT_KEY] == 1
        with store.flight_lock(FLIGHT_KEY):
            assert store._held_locks[FLIGHT_KEY] == 2
            # The lock row still exists while nested.
            assert store.conn.execute(
                "SELECT COUNT(*) FROM flight_lifecycle_locks WHERE flight_key=?", (FLIGHT_KEY,)
            ).fetchone()[0] == 1
        assert store._held_locks[FLIGHT_KEY] == 1
    assert FLIGHT_KEY not in store._held_locks
    store.close()


def test_stale_lock_after_crash_is_reclaimed(database):
    crashed = LifecycleStore(database, lock_owner="crashed", lock_ttl_seconds=0.05)
    crashed.index_record(normalize(full_trajectory_rows()[0], "airport-a"))
    # Simulate process death: write a lock row that will never be released.
    with crashed._immediate() as conn:
        conn.execute(
            "INSERT INTO flight_lifecycle_locks(flight_key, owner, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (FLIGHT_KEY, "crashed", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00"),
        )
    crashed.close()
    recovered = LifecycleStore(database, lock_owner="restarted")
    try:
        with recovered.flight_lock(FLIGHT_KEY):
            result = recovered.rebuild_flight("CA100", "2026-11-01")
            assert result["published"] is True
        events = [
            row["event"]
            for row in recovered.conn.execute(
                "SELECT event FROM flight_lifecycle_lock_events WHERE flight_key=?",
                (FLIGHT_KEY,),
            ).fetchall()
        ]
        assert "lock_recovered" in events
    finally:
        recovered.close()


def test_half_way_failure_before_commit_leaves_no_partial_version(database):
    store = LifecycleStore(database)
    seed(store, full_trajectory_rows())
    store.set_fault("before_publish")
    with pytest.raises(RuntimeError):
        store.rebuild_flight("CA100", "2026-11-01")
    assert store.list_versions(FLIGHT_KEY) == []
    assert store.conn.execute("SELECT COUNT(*) FROM flight_lifecycle_states").fetchone()[0] == 0
    assert store.conn.execute("SELECT COUNT(*) FROM flight_lifecycle_conflicts").fetchone()[0] == 0
    # Retry succeeds and does not duplicate anything.
    result = store.rebuild_flight("CA100", "2026-11-01")
    assert result["published"] is True
    assert len(store.list_versions(FLIGHT_KEY)) == 1
    store.close()


def test_failure_after_rows_but_before_commit_rolls_back_atomically(database):
    store = LifecycleStore(database)
    seed(store, full_trajectory_rows())
    store.rebuild_flight("CA100", "2026-11-01")
    seed(store, [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T09:30:00Z",
            status="arrived",
            actual_departure="2026-11-01T06:48:00Z",
        )
    ])
    store.set_fault("after_publish_rows")
    with pytest.raises(RuntimeError):
        store.rebuild_flight("CA100", "2026-11-01")
    versions = store.list_versions(FLIGHT_KEY)
    assert len(versions) == 1  # only the first committed version survives
    store.close()


def test_recover_orphans_cleans_half_published_restored_database(database):
    store = LifecycleStore(database)
    seed(store, full_trajectory_rows())
    good = store.rebuild_flight("CA100", "2026-11-01")
    # Simulate a restored file carrying child rows without a header.
    store.conn.execute("PRAGMA foreign_keys = OFF")
    store.conn.execute("BEGIN IMMEDIATE")
    store.conn.execute(
        """INSERT INTO flight_lifecycle_states(
             version_id, flight_key, sequence_no, status, event_family, record_key,
             source, observed_at, effective_from, effective_to, adopted, decision,
             conflict_reason, actual_departure, quality_issues_json, raw_hash)
           VALUES (999, ?, 1, 'scheduled', 'ground_operation', 'rk', 's',
                   '2026-11-01T06:00:00Z', '2026-11-01T06:00:00Z', NULL, 1,
                   'adopted_first_observation', NULL, NULL, '[]', 'h')""",
        (FLIGHT_KEY,),
    )
    store.conn.commit()
    assert store.recover_orphans() == ["999"]
    assert store.load_trajectory(FLIGHT_KEY)["version_id"] == good["version_id"]
    store.close()


def test_resource_limit_surfaces_from_store_without_publishing(database):
    store = LifecycleStore(database)
    rows = [
        raw_observation(source="airport-a", observed_at=f"2026-11-01T06:{m:02d}:00Z", status="scheduled")
        for m in range(3)
    ]
    seed(store, rows)
    with pytest.raises(ResourceLimitExceeded):
        store.rebuild_flight("CA100", "2026-11-01", LifecycleConfig(max_observations=2))
    assert store.list_versions(FLIGHT_KEY) == []
    store.close()


def test_every_fact_in_trajectory_is_traceable_to_source_record(database):
    store = LifecycleStore(database)
    rows = [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
        raw_observation(source="airport-b", observed_at="2026-11-01T06:00:00Z", status="boarding"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:50:00Z",
            status="departed",
            actual_departure="2026-11-01T06:48:00Z",
        ),
    ]
    seed(store, rows)
    store.rebuild_flight(
        "CA100",
        "2026-11-01",
        LifecycleConfig(source_priority={"airport-a": 2}),
    )
    assert store.verify_traceability(FLIGHT_KEY) == []
    exported = store.export_trajectory(FLIGHT_KEY)
    referenced = {s["record_key"] for s in exported["segments"]}
    referenced |= {c["record_key"] for c in exported["conflicts"]}
    assert referenced <= set(exported["provenance"])
    for record_key, provenance in exported["provenance"].items():
        assert provenance["source"]
        assert provenance["raw_hash"]
        assert provenance["payload"]["flight_id"] == "CA100"
    store.close()


def test_legacy_database_migration_backfills_and_keeps_original_tables(tmp_path):
    db_path = tmp_path / "legacy.sqlite3"
    legacy = DataStore(str(db_path))
    batch_id = legacy.start_batch("legacy")
    for row in full_trajectory_rows():
        legacy.upsert(normalize(row, row["source"]))
    legacy.finish_batch(batch_id, "completed", 5, 5, 0)
    legacy.close()

    store = LifecycleStore(str(db_path))
    try:
        assert sorted(store.list_flight_keys()) == [FLIGHT_KEY]
        result = store.rebuild_flight("CA100", "2026-11-01")
        assert result["published"] is True
        # Reopening must not duplicate backfilled observations.
        assert store.conn.execute("SELECT COUNT(*) FROM flight_observations").fetchone()[0] == 5
    finally:
        store.close()

    again = LifecycleStore(str(db_path))
    try:
        assert again.conn.execute("SELECT COUNT(*) FROM flight_observations").fetchone()[0] == 5
        # Original table and columns remain untouched.
        assert again.conn.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 5
        assert again.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 1
        version = again.conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        assert version == 1
    finally:
        again.close()


def test_ingestion_indexes_observations_and_rebuild_is_idempotent(tmp_path):
    db_path = tmp_path / "ingest.sqlite3"
    fixture = Path(__file__).parents[1] / "fixtures" / "lifecycle_synthetic.json"
    first = run_fixture(str(fixture), str(db_path), str(tmp_path / "raw"))
    second = run_fixture(str(fixture), str(db_path), str(tmp_path / "raw"))
    assert first["indexed"] == 8
    assert second["indexed"] == 0  # replays are no-ops in the index too

    store = LifecycleStore(str(db_path))
    try:
        # Airport authority outranks the airline feed, so it may override
        # the earlier cancellation under the source_priority protocol.
        config = LifecycleConfig(
            transition_policy="source_priority",
            source_priority={"airport-a": 5, "airport-b": 3, "airline-feed": 2},
        )
        result = store.rebuild_flight("CA200", "2026-11-02", config)
        # 6 CA200 snapshots, one of which shares an observed_at across sources.
        assert result["observation_count"] == 6
        trajectory = store.load_trajectory("CA200|2026-11-02")
        assert trajectory["current_state"] == "arrived"
        # The cancellation override is carried on the adopting segment itself.
        override = next(
            segment for segment in trajectory["segments"]
            if segment["decision"] == "source_priority_override"
        )
        assert "cancellation" in override["conflict_reason"]
        assert store.verify_traceability("CA200|2026-11-02") == []
    finally:
        store.close()


def test_same_timestamp_different_sources_are_both_preserved(database):
    store = LifecycleStore(database)
    try:
        rows = [
            raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
            raw_observation(source="airport-b", observed_at="2026-11-01T06:00:00Z", status="boarding"),
        ]
        for row in rows:
            assert store.index_record(normalize(row, row["source"])) is True
        # Replaying either source is a no-op.
        assert store.index_record(normalize(rows[0], "airport-a")) is False
        stored = store.conn.execute(
            "SELECT record_key, source FROM flight_observations WHERE flight_key=? ORDER BY source",
            (FLIGHT_KEY,),
        ).fetchall()
        assert [row["source"] for row in stored] == ["airport-a", "airport-b"]
        expected_key = normalize(rows[0], "airport-a").record_key
        assert {row["record_key"] for row in stored} == {expected_key}
        result = store.rebuild_flight(
            "CA100",
            "2026-11-01",
            LifecycleConfig(source_priority={"airport-b": 9}),
        )
        assert result["observation_count"] == 2
        trajectory = store.load_trajectory(FLIGHT_KEY)
        assert trajectory["current_state"] == "boarding"
        assert any(c["conflict_axis"] == "same_timestamp" for c in trajectory["conflicts"])
        assert store.verify_traceability(FLIGHT_KEY) == []
    finally:
        store.close()


def test_two_processes_replaying_same_input_keep_single_version(database):
    store = LifecycleStore(database)
    seed(store, full_trajectory_rows())
    store.close()

    versions: list[int] = []

    def replay() -> None:
        worker = LifecycleStore(database, lock_owner=f"worker-{threading.get_ident()}", busy_timeout_ms=2000)
        try:
            result = worker.rebuild_flight("CA100", "2026-11-01")
            versions.append(result["version_id"])
        finally:
            worker.close()

    threads = [threading.Thread(target=replay) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    auditor = LifecycleStore(database)
    try:
        assert len(auditor.list_versions(FLIGHT_KEY)) == 1
        assert len(set(versions)) == 1
    finally:
        auditor.close()

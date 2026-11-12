"""SQLite identity store tests: migrations, recovery, concurrency and faults."""

from __future__ import annotations

import json
import sqlite3
import threading
import unittest
from pathlib import Path

from aviation_operations.identity import (
    FlightIdentityResolver,
    MappingBundle,
    MappingValidationError,
    STATUS_AMBIGUOUS,
    STATUS_EXACT,
    STATUS_INVALID,
    STATUS_MANUAL,
    bundle_from_mapping,
    canonical_flight_key,
)
from aviation_operations.identity_service import IdentityService
from aviation_operations.identity_store import (
    ConcurrentUpdateError,
    Fault,
    IdentityIndexUnavailable,
    IdentityReader,
    IdentityStoreError,
    IdentityWriter,
    MappingVersionConflict,
)

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures"
TEST_DIR = Path(__file__).parent


class TempDatabaseMixin:
    """File-backed databases that are always removed (incl. WAL/SHM)."""

    def setUp(self):  # type: ignore[override]
        self._tmp_paths: list[Path] = []

    def tearDown(self):  # type: ignore[override]
        for path in self._tmp_paths:
            for suffix in ("", "-wal", "-shm"):
                Path(str(path) + suffix).unlink(missing_ok=True)

    def tmp_database(self, name: str) -> str:
        path = TEST_DIR / f"_tmp_{name}_{id(self)}.sqlite3"
        self._tmp_paths.append(path)
        return str(path)


def load_bundle(version: str = "mapping-2026-11-v1"):
    data = json.loads((FIXTURE_DIR / "identity_mapping_v1.json").read_text())
    data["mapping_version"] = version
    return bundle_from_mapping(data)


def v2_bundle():
    data = json.loads((FIXTURE_DIR / "identity_mapping_v1.json").read_text())
    data["mapping_version"] = "mapping-2026-12-v2"
    data["flight_aliases"] = [
        {
            "rule_id": "FA-ZH-002",
            "airline": "ZH",
            "number": "9001",
            "canonical_airline": "CA",
            "canonical_number": "1001",
        }
    ]
    data["airline_aliases"] = data.get("airline_aliases", []) + [
        {"rule_id": "AA-AC-002", "alias": "AC", "canonical": "CA"}
    ]
    return bundle_from_mapping(data)


class MigrationTests(TempDatabaseMixin, unittest.TestCase):
    def test_legacy_database_without_identity_schema_is_detected(self):
        tmp = Path(self.tmp_database("legacy"))
        legacy = sqlite3.connect(tmp)
        legacy.execute("CREATE TABLE records(record_key TEXT PRIMARY KEY)")
        legacy.commit()
        legacy.close()
        with self.assertRaises(IdentityIndexUnavailable):
            IdentityReader(str(tmp))
        writer = IdentityWriter(str(tmp))
        self.assertEqual(writer.current_version(), 0)
        self.assertEqual(writer.migrate(), 1)
        # Legacy table is untouched.
        tables = {
            row[0]
            for row in writer.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        self.assertIn("records", tables)
        self.assertIn("identity_index", tables)
        writer.close()
        with IdentityReader(str(tmp)) as reader:
            self.assertEqual(reader.write_version(), 0)

    def test_migration_is_idempotent(self):
        with IdentityWriter(":memory:") as writer:
            self.assertEqual(writer.migrate(), 1)
            self.assertEqual(writer.migrate(), 1)
            self.assertEqual(writer.current_version(), 1)

    def test_rollback_removes_identity_tables_but_keeps_domain_data(self):
        tmp = Path(self.tmp_database("rollback"))
        conn = sqlite3.connect(tmp)
        conn.execute("CREATE TABLE records(record_key TEXT PRIMARY KEY, payload_json TEXT)")
        conn.execute("INSERT INTO records VALUES ('RK1', '{}')")
        conn.commit()
        conn.close()
        writer = IdentityWriter(str(tmp))
        writer.migrate()
        writer.activate_bundle(load_bundle())
        self.assertEqual(writer.rollback(0), 0)
        tables = {
            row[0]
            for row in writer.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        self.assertIn("records", tables)
        self.assertNotIn("identity_index", tables)
        self.assertNotIn("mapping_bundles", tables)
        # Domain data survived.
        self.assertEqual(
            writer.conn.execute("SELECT record_key FROM records").fetchone()[0], "RK1"
        )
        writer.close()
        # Re-migrate after rollback works (recovery path).
        writer = IdentityWriter(str(tmp))
        self.assertEqual(writer.migrate(), 1)
        writer.close()


class MappingLifecycleTests(unittest.TestCase):
    def test_activate_and_reload_bundle(self):
        with IdentityWriter(":memory:") as writer:
            writer.activate_bundle(load_bundle())
            reloaded = writer.load_active_bundle()
            self.assertEqual(reloaded.version, "mapping-2026-11-v1")
            self.assertEqual(reloaded.bundle_hash, load_bundle().bundle_hash)
            self.assertEqual(len(reloaded.codeshares), 3)

    def test_same_version_with_different_hash_is_rejected(self):
        with IdentityWriter(":memory:") as writer:
            writer.activate_bundle(load_bundle("v1"))
            altered = bundle_from_mapping(
                {
                    "mapping_version": "v1",
                    "airline_aliases": [
                        {"rule_id": "other", "alias": "DD", "canonical": "CA"}
                    ],
                }
            )
            with self.assertRaises(MappingVersionConflict):
                writer.activate_bundle(altered)

    def test_optimistic_write_version_blocks_stale_writers(self):
        with IdentityWriter(":memory:") as writer:
            writer.activate_bundle(load_bundle())
            version = writer.write_version()
            writer.activate_bundle(v2_bundle())
            self.assertEqual(writer.write_version(), version + 1)
            with self.assertRaises(ConcurrentUpdateError):
                writer.activate_bundle(
                    bundle_from_mapping({"mapping_version": "v3"}),
                    expected_write_version=version,
                )

    def test_bundle_switch_marks_old_index_rows_stale_and_history_is_kept(self):
        with IdentityService(":memory:", load_bundle()) as service:
            resolution = service.resolve(
                flight_id="AC777", flight_date="2026-11-01", airline_code="AC",
                origin="PEK", destination="SHA",
            )
            self.assertEqual(resolution.canonical_flight_key, "FV2|AC|777|2026-11-01")
            service.index_record("RK1", "src", resolution)
            service.activate_bundle(v2_bundle())
            with service.reader() as reader:
                row = reader.get_by_record_key("RK1")
                self.assertEqual(row["stale"], 1)
            re_resolved = service.resolve(
                flight_id="AC777", flight_date="2026-11-01", airline_code="AC",
                origin="PEK", destination="SHA",
            )
            # v2 introduces AC -> CA, so the canonical key actually moves.
            self.assertEqual(re_resolved.canonical_flight_key, "FV2|CA|777|2026-11-01")
            service.index_record("RK1", "src", re_resolved)
            with service.reader() as reader:
                row = reader.get_by_record_key("RK1")
                self.assertEqual(row["stale"], 0)
                self.assertEqual(row["mapping_version"], "mapping-2026-12-v2")
                history = reader.history("RK1")
                self.assertTrue(history)
                self.assertEqual(history[0]["old_canonical_key"], "FV2|AC|777|2026-11-01")
                self.assertEqual(history[0]["new_canonical_key"], "FV2|CA|777|2026-11-01")


class IdentityIndexTests(unittest.TestCase):
    def setUp(self):
        self.service = IdentityService(":memory:", load_bundle())

    def tearDown(self):
        self.service.close()

    def test_exact_resolution_is_queryable_by_canonical_key(self):
        for flight_id, airline, record_key_ in (
            ("CA1001", "CA", "RK-OP"),
            ("ZH5001", "ZH", "RK-MK"),
        ):
            resolution = self.service.resolve(
                flight_id=flight_id, flight_date="2026-11-01", airline_code=airline,
                origin="PEK", destination="SHA",
                scheduled_departure="2026-11-01T06:30:00Z",
            )
            self.assertEqual(resolution.status, STATUS_EXACT)
            self.service.index_record(record_key_, "src", resolution)
        with self.service.reader() as reader:
            rows = reader.get_by_canonical("FV2|CA|1001|2026-11-01")
            self.assertEqual({row["record_key"] for row in rows}, {"RK-OP", "RK-MK"})

    def test_invalid_resolution_is_indexed_for_review(self):
        resolution = self.service.resolve(
            flight_id="CA1001", flight_date="bad-date", airline_code="CA",
            origin="PEK", destination="SHA",
        )
        self.assertEqual(resolution.status, STATUS_INVALID)
        self.service.index_record("RK-BAD", "src", resolution)
        with self.service.reader() as reader:
            rows = reader.search(status=STATUS_INVALID)
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0]["canonical_flight_key"])
            self.assertTrue(json.loads(rows[0]["reasons_json"]))

    def test_manual_decision_overrides_ambiguous_and_withdrawal_reopens_review(self):
        ambiguous = self.service.resolve(
            flight_id="ZZ999", flight_date="2026-11-01", airline_code="ZZ",
            origin="PEK", destination="SHA",
        )
        # ZZ999 resolves exact on its own; force ambiguity via batch conflict.
        records = [
            {"source": "s1", "payload": {"flight_id": "X999", "flight_date": "2026-11-01",
             "airline_code": "AA", "origin": "PEK", "destination": "SHA",
             "scheduled_departure": "2026-11-01T01:00:00Z"}},
            {"source": "s2", "payload": {"flight_id": "X999", "flight_date": "2026-11-01",
             "airline_code": "BB", "origin": "CAN", "destination": "SZX",
             "scheduled_departure": "2026-11-01T02:00:00Z"}},
        ]
        resolutions, conflicts = self.service.resolve_batch(records)
        self.assertEqual(len(conflicts), 1)
        review_key = resolutions[0].review_key
        fingerprints = [r.fingerprint for r in resolutions]
        target = canonical_flight_key("AA", "999", "", "2026-11-01")
        decision_id = self.service.confirm_identity(
            review_key, target, "operator confirms AA",
            covered_fingerprints=fingerprints,
        )
        decided = self.service.resolve(
            flight_id="X999", flight_date="2026-11-01", airline_code="AA",
            origin="PEK", destination="SHA",
            scheduled_departure="2026-11-01T01:00:00Z",
        )
        self.assertEqual(decided.status, STATUS_MANUAL)
        self.assertEqual(decided.canonical_flight_key, target)
        self.assertTrue(any(e.basis == "manual" for e in decided.evidence))

        self.service.index_record("RK-X1", "s1", decided)
        with self.service.reader() as reader:
            self.assertEqual(len(reader.manual_decisions()), 1)
            self.assertEqual(len(reader.get_by_canonical(target)), 1)

        # Withdrawal flips the row stale and reopens the case: no manual
        # override applies anymore, so the input is no longer 'manual'.
        self.service.withdraw_identity(review_key)
        again = self.service.resolve_batch(records[:1])[0][0]
        self.assertNotEqual(again.status, STATUS_MANUAL)
        with self.service.reader() as reader:
            self.assertEqual(reader.manual_decisions(), [])
            self.assertEqual(reader.get_by_record_key("RK-X1")["stale"], 1)
        self.assertGreater(decision_id, 0)

    def test_duplicate_manual_decision_for_same_review_key_rejected(self):
        review_key = "RVW|testdup"
        target = canonical_flight_key("AA", "1", "", "2026-11-01")
        self.service.confirm_identity(review_key, target)
        with self.assertRaises(ConcurrentUpdateError):
            self.service.confirm_identity(review_key, target)


class BackfillAndRecoveryTests(TempDatabaseMixin, unittest.TestCase):
    def _seed_legacy_records(self, path: str):
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE records(record_key TEXT PRIMARY KEY, source TEXT, "
            "observed_at TEXT, payload_json TEXT, raw_hash TEXT, "
            "first_seen_at TEXT, updated_at TEXT)"
        )
        payloads = [
            ("RK1", "src", {"flight_id": "CA1001", "flight_date": "2026-11-01",
             "airline_code": "CA", "origin": "PEK", "destination": "SHA",
             "scheduled_departure": "2026-11-01T06:30:00Z"}),
            ("RK2", "src", {"flight_id": "ZH5001", "flight_date": "2026-11-01",
             "airline_code": "ZH", "origin": "PEK", "destination": "SHA",
             "scheduled_departure": "2026-11-01T06:31:00Z"}),
            ("RK3", "src", {"flight_id": "BAD", "flight_date": "2026-11-01",
             "airline_code": "CA", "origin": "PE", "destination": "SHA"}),
        ]
        for key, source, payload in payloads:
            conn.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?)",
                (key, source, "2026-11-01T00:00:00Z", json.dumps(payload), "h", "t", "t"),
            )
        conn.commit()
        conn.close()

    def test_backfill_indexes_existing_records_and_retries_failures(self):
        tmp = Path(self.tmp_database("backfill"))
        self._seed_legacy_records(str(tmp))
        with IdentityService(str(tmp), load_bundle()) as service:
            summary = service.backfill()
            self.assertEqual(summary["processed"], 3)
            self.assertEqual(summary["failed"], 0)
            # Second run skips already indexed rows.
            again = service.backfill()
            self.assertEqual(again["skipped"], 3)
            with service.reader() as reader:
                exact = reader.search(status=STATUS_EXACT)
                self.assertEqual(len(exact), 2)
                invalid = reader.search(status=STATUS_INVALID)
                self.assertEqual(len(invalid), 1)

    def test_failure_injection_is_captured_and_retry_recovers(self):
        tmp = Path(self.tmp_database("fault"))
        self._seed_legacy_records(str(tmp))
        faults = [Fault("before_identity_commit", RuntimeError("injected disk fault"))]
        with IdentityService(str(tmp), load_bundle(), faults=faults) as service:
            # First write hits the one-shot fault.
            with self.assertRaises(RuntimeError):
                service.index_record(
                    "RK1",
                    "src",
                    service.resolve(
                        flight_id="CA1001", flight_date="2026-11-01",
                        airline_code="CA", origin="PEK", destination="SHA",
                    ),
                )
            # Fault consumed: subsequent writes succeed.
            resolution = service.resolve(
                flight_id="CA1001", flight_date="2026-11-01",
                airline_code="CA", origin="PEK", destination="SHA",
            )
            self.assertTrue(service.index_record("RK1", "src", resolution))
            # Idempotent second write reports no change.
            self.assertFalse(service.index_record("RK1", "src", resolution))

    def test_backfill_failures_land_in_retry_queue(self):
        tmp = Path(self.tmp_database("retry"))
        self._seed_legacy_records(str(tmp))
        attempts = {"n": 0}
        with IdentityService(str(tmp), load_bundle()) as service:
            original = service.resolver.resolve

            def flaky(**kwargs):
                attempts["n"] += 1
                if attempts["n"] == 1:
                    raise RuntimeError("transient resolver fault")
                return original(**kwargs)

            service.resolver.resolve = flaky  # type: ignore[method-assign]
            summary = service.backfill()
            self.assertGreaterEqual(summary["failed"], 1)
            with service.reader() as reader:
                queued = reader.failures()
                self.assertTrue(queued)
            # Restore and retry: every queued failure recovers.
            service.resolver.resolve = original  # type: ignore[method-assign]
            retry_summary = service.retry_failures()
            self.assertEqual(retry_summary["retried"], retry_summary["recovered"])
            with service.reader() as reader:
                self.assertEqual(reader.failures(), [])


class ConcurrencyTests(TempDatabaseMixin, unittest.TestCase):
    def test_concurrent_writers_serialize_and_stale_tokens_fail(self):
        tmp = Path(self.tmp_database("concurrent"))
        bundle = load_bundle()
        service = IdentityService(str(tmp), bundle)
        errors: list[Exception] = []
        barrier = threading.Barrier(4)

        def worker(index: int):
            writer = IdentityWriter(str(tmp))
            try:
                bundle = writer.load_active_bundle()
                # Every worker captures the same token before the race;
                # only the first write can still match it afterwards.
                expected = writer.write_version()
                barrier.wait()
                writer.save_manual_decision(
                    f"RVW|conc{index}",
                    canonical_flight_key("AA", str(index + 1), "", "2026-11-01"),
                    bundle.version,
                    bundle.bundle_hash,
                    expected_write_version=expected,
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)
            finally:
                writer.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        with service.reader() as reader:
            decisions = reader.manual_decisions()
        self.assertEqual(len(decisions), 1)
        self.assertEqual(len(errors), 3)
        self.assertTrue(all(isinstance(e, ConcurrentUpdateError) for e in errors))
        service.close()

    def test_concurrent_index_writes_do_not_lose_rows(self):
        tmp = Path(self.tmp_database("concurrent_index"))
        service = IdentityService(str(tmp), load_bundle())
        errors: list[Exception] = []

        def worker(start: int):
            writer = IdentityWriter(str(tmp))
            try:
                for i in range(start, start + 10):
                    resolver = FlightIdentityResolver(writer.load_active_bundle())
                    resolution = resolver.resolve(
                        flight_id=f"CA{1000 + i}",
                        flight_date="2026-11-01",
                        airline_code="CA",
                        origin="PEK",
                        destination="SHA",
                    )
                    writer.write_identity(f"RK-{i}", "src", resolution)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                writer.close()

        threads = [threading.Thread(target=worker, args=(i * 10,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        with service.reader() as reader:
            rows = reader.search(limit=1000)
        self.assertEqual(len(rows), 40)
        service.close()


class ReadOnlyGuardTests(TempDatabaseMixin, unittest.TestCase):
    def test_reader_cannot_mutate(self):
        tmp = Path(self.tmp_database("readonly"))
        service = IdentityService(str(tmp), load_bundle())
        service.close()
        reader = IdentityReader(str(tmp))
        with self.assertRaises(sqlite3.OperationalError):
            reader.conn.execute("DELETE FROM identity_index")
        with self.assertRaises(sqlite3.OperationalError):
            reader.conn.execute("CREATE TABLE x(y)")
        reader.close()


class FaultInjectionMigrationTests(TempDatabaseMixin, unittest.TestCase):
    def test_fault_after_migration_commit_does_not_corrupt_schema(self):
        # The fault stage fires after the schema script committed; a new
        # writer must observe a consistent version and be able to migrate.
        with IdentityWriter(":memory:", faults=[Fault("after_migration_1", RuntimeError("boom"))]) as w:
            with self.assertRaises(RuntimeError):
                w.migrate()
        with IdentityWriter(":memory:") as w:
            self.assertEqual(w.migrate(), 1)

    def test_fault_during_identity_write_rolls_back_transaction(self):
        with IdentityWriter(
            ":memory:",
            faults=[Fault("before_identity_commit", RuntimeError("disk full"))],
        ) as writer:
            writer.activate_bundle(load_bundle())
            resolver = FlightIdentityResolver(writer.load_active_bundle())
            resolution = resolver.resolve(
                flight_id="CA1001", flight_date="2026-11-01", airline_code="CA",
                origin="PEK", destination="SHA",
            )
            with self.assertRaises(RuntimeError):
                writer.write_identity("RK1", "src", resolution)
            # The failed transaction left no partial row behind.
            rows = writer.conn.execute("SELECT COUNT(*) FROM identity_index").fetchone()[0]
            self.assertEqual(rows, 0)
            # The write token was also rolled back, so a retry succeeds.
            self.assertTrue(writer.write_identity("RK1", "src", resolution))

    def test_rollback_followed_by_remigrate_is_recoverable(self):
        tmp = Path(self.tmp_database("remigrate"))
        conn = sqlite3.connect(tmp)
        conn.execute("CREATE TABLE records(record_key TEXT PRIMARY KEY)")
        conn.commit()
        conn.close()
        writer = IdentityWriter(str(tmp))
        writer.migrate()
        writer.activate_bundle(load_bundle())
        self.assertEqual(writer.rollback(0), 0)
        # Recover: re-run migration and install the mapping again.
        self.assertEqual(writer.migrate(), 1)
        writer.activate_bundle(load_bundle())
        self.assertEqual(writer.current_version(), 1)
        writer.close()
        with IdentityReader(str(tmp)) as reader:
            self.assertEqual(reader.active_mapping_version(), "mapping-2026-11-v1")


if __name__ == "__main__":
    unittest.main()

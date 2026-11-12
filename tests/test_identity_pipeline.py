"""End-to-end tests: ingestion pipeline + identity service integration."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from aviation_operations.application import run_fixture
from aviation_operations.identity import (
    STATUS_EXACT,
    STATUS_INVALID,
    canonical_flight_key,
    record_key,
)
from aviation_operations.identity_service import IdentityService
from aviation_operations.identity_store import (
    Fault,
    IdentityReader,
    IdentityWriter,
)
from aviation_operations.normalize import normalize
from aviation_operations.pipeline import IngestionPipeline
from aviation_operations.storage import DataStore

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures"
MAPPING = str(FIXTURE_DIR / "identity_mapping_v1.json")
BATCH = str(FIXTURE_DIR / "codeshare_batch.json")
SAMPLE = str(FIXTURE_DIR / "sample.json")


class PipelineWithoutIdentityTests(unittest.TestCase):
    def test_default_pipeline_behavior_is_unchanged(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "data.sqlite3")
            result = run_fixture(SAMPLE, db, str(Path(tmp) / "raw"))
            self.assertEqual(result["accepted"], 3)
            self.assertEqual(result["rejected"], 1)
            self.assertNotIn("identity_failed", result)
            # No identity schema was created when no mapping is supplied.
            conn = DataStore(db).conn
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertNotIn("identity_index", tables)
            conn.close()


class PipelineIdentityIntegrationTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.database = str(Path(self.tmp.name) / "data.sqlite3")
        self.raw_dir = str(Path(self.tmp.name) / "raw")

    def tearDown(self):
        self.tmp.cleanup()

    def test_codeshare_batch_ingests_with_one_canonical_operating_identity(self):
        result = run_fixture(BATCH, self.database, self.raw_dir, identity_mapping=MAPPING)
        self.assertEqual(result["processed"], 6)
        # All six records pass domain normalization; identity quality is
        # tracked separately and never changes accept/reject semantics.
        self.assertEqual(result["accepted"], 6)
        with IdentityReader(self.database) as reader:
            rows = reader.get_by_canonical("FV2|CA|1001|2026-11-01")
            # Operating CA1001, marketed ZH5001/NX108 and aliased CCA1001.
            self.assertEqual(len(rows), 4)
            invalid = reader.search(status=STATUS_INVALID)
            self.assertEqual(len(invalid), 1)  # ZZ9 with illegal airport
            # Every indexed row remembers the mapping version it used.
            for row in reader.search(limit=100):
                self.assertEqual(row["mapping_version"], "mapping-2026-11-v1")

    def test_record_key_meaning_is_untouched_by_identity(self):
        raw = json.loads(Path(BATCH).read_text())
        run_fixture(BATCH, self.database, self.raw_dir, identity_mapping=MAPPING)
        store = DataStore(self.database)
        stored = {
            row[0]: json.loads(row[1])
            for row in store.conn.execute("SELECT record_key, payload_json FROM records")
        }
        store.close()
        for item in raw:
            payload = item["payload"]
            expected = record_key(
                payload["flight_id"], payload["flight_date"], item["observed_at"]
            )
            self.assertIn(expected, stored)
            # The stored payload still carries the original flight id.
            self.assertEqual(stored[expected]["flight_id"], payload["flight_id"])

    def test_identity_failure_does_not_reject_domain_record_and_retries(self):
        service = IdentityService(
            self.database,
            faults=[Fault("before_identity_commit", RuntimeError("injected"))],
        )
        service.load_bundle_from_file(MAPPING)
        records = json.loads(Path(BATCH).read_text())
        store = DataStore(self.database)
        batch_id = store.start_batch("test")
        pipeline = IngestionPipeline(
            records, "test", store, batch_id, normalize, identity_service=service
        )
        summary = pipeline.run()
        # First record hit the one-shot identity fault but is still accepted.
        self.assertEqual(summary["accepted"], 6)
        self.assertEqual(summary["identity_failed"], 1)
        store.close()
        with service.reader() as reader:
            self.assertEqual(len(reader.failures()), 1)
        retry = service.retry_failures()
        self.assertEqual(retry["retried"], 1)
        self.assertEqual(retry["recovered"], 1)
        with service.reader() as reader:
            self.assertEqual(reader.failures(), [])
        service.close()

    def test_backfill_after_the_fact_recovers_legacy_database(self):
        # First ingest without identity at all (legacy-style database).
        run_fixture(BATCH, self.database, self.raw_dir)
        with self.assertRaises(Exception):
            IdentityReader(self.database)
        # Then attach identity and backfill the existing records.
        with IdentityService(self.database) as service:
            service.load_bundle_from_file(MAPPING)
            summary = service.backfill()
            self.assertEqual(summary["processed"], 6)
            self.assertEqual(summary["failed"], 0)
            with service.reader() as reader:
                exact = reader.search(status=STATUS_EXACT)
                self.assertEqual(len(exact), 5)

    def test_reindex_after_mapping_switch_and_recovery_via_history(self):
        # Ingest first without identity, then attach the service and backfill.
        run_fixture(BATCH, self.database, self.raw_dir)
        with IdentityService(self.database) as service:
            service.load_bundle_from_file(MAPPING)
            service.backfill()
            v2_path = Path(self.tmp.name) / "mapping_v2.json"
            v2 = json.loads(Path(MAPPING).read_text())
            v2["mapping_version"] = "mapping-2026-12-v2"
            v2["airline_aliases"].append(
                {"rule_id": "AA-NX-009", "alias": "NX", "canonical": "CA"}
            )
            v2_path.write_text(json.dumps(v2))
            service.load_bundle_from_file(str(v2_path))
            # NX108 now canonicalizes onto CA instead of the codeshare op.
            reindex = service.reindex()
            self.assertGreaterEqual(reindex["updated"], 1)
            with service.reader() as reader:
                history = reader.history()
                self.assertTrue(
                    any(row["mapping_version_to"] == "mapping-2026-12-v2" for row in history)
                )

    def test_manual_confirmation_round_trip_through_index(self):
        with IdentityService(self.database) as service:
            service.load_bundle_from_file(MAPPING)
            # Illegal airport -> invalid, then operator confirms identity.
            resolution = service.resolve(
                flight_id="ZZ9", flight_date="2026-11-01", airline_code="ZZ",
                origin="P2K", destination="SHA",
            )
            self.assertEqual(resolution.status, STATUS_INVALID)
            target = canonical_flight_key("ZZ", "9", "", "2026-11-01")
            service.confirm_identity(
                resolution.review_key,
                target,
                rationale="airport code typo accepted after source check",
                covered_fingerprints=[resolution.fingerprint],
            )
            decided = service.resolve(
                flight_id="ZZ9", flight_date="2026-11-01", airline_code="ZZ",
                origin="P2K", destination="SHA",
            )
            self.assertEqual(decided.status, "manual")
            service.index_record("RK-ZZ9", "test", decided)
            with service.reader() as reader:
                self.assertEqual(len(reader.get_by_canonical(target)), 1)
            # Withdrawal returns the row to stale and removes the override.
            service.withdraw_identity(resolution.review_key)
            with service.reader() as reader:
                self.assertEqual(reader.manual_decisions(), [])
                self.assertEqual(reader.get_by_record_key("RK-ZZ9")["stale"], 1)


class IndexBatchServiceTests(unittest.TestCase):
    def test_index_batch_records_exact_invalid_and_conflicts(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            service = IdentityService(str(Path(tmp) / "id.sqlite3"))
            service.load_bundle_from_file(MAPPING)
            records = [
                {"source": "s1", "observed_at": "2026-11-01T01:00:00Z",
                 "payload": {"flight_id": "X999", "flight_date": "2026-11-01",
                              "airline_code": "AA", "origin": "PEK", "destination": "SHA",
                              "scheduled_departure": "2026-11-01T01:00:00Z"}},
                {"source": "s2", "observed_at": "2026-11-01T02:00:00Z",
                 "payload": {"flight_id": "X999", "flight_date": "2026-11-01",
                              "airline_code": "BB", "origin": "CAN", "destination": "SZX",
                              "scheduled_departure": "2026-11-01T02:00:00Z"}},
                {"source": "s3", "observed_at": "2026-11-01T03:00:00Z",
                 "payload": {"flight_id": "CA1001", "flight_date": "2026-11-01",
                              "airline_code": "CA", "origin": "PEK", "destination": "SHA",
                              "scheduled_departure": "2026-11-01T06:30:00Z"}},
            ]

            def key_fn(record, fields):
                return record_key(
                    fields["flight_id"], fields["flight_date"], record["observed_at"]
                )

            summary = service.index_batch(records, record_key_fn=key_fn)
            self.assertEqual(summary["processed"], 3)
            self.assertEqual(summary["exact"], 1)
            self.assertEqual(summary["ambiguous"], 2)
            self.assertEqual(summary["conflicts"], 1)
            self.assertEqual(summary["failed"], 0)
            with service.reader() as reader:
                self.assertEqual(len(reader.conflicts()), 1)
                self.assertEqual(reader.conflicts()[0]["reason"], "shared_flight_id_conflict")
            service.close()


if __name__ == "__main__":
    unittest.main()

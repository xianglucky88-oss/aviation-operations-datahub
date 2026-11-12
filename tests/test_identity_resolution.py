"""Tests for deterministic normalization and versioned identity resolution."""

from __future__ import annotations

import json
import time
import unittest
from pathlib import Path

from aviation_operations.identity import (
    AirlineAlias,
    AliasCycleError,
    CodeshareMembership,
    FlightAlias,
    FlightIdentityResolver,
    MappingBundle,
    MappingValidationError,
    ResolutionCache,
    STATUS_AMBIGUOUS,
    STATUS_EXACT,
    STATUS_INVALID,
    STATUS_MANUAL,
    airport_key,
    bundle_from_mapping,
    canonical_flight_key,
    flight_key,
    load_bundle,
    normalize_airport_code,
    normalize_flight_date,
    normalize_flight_id,
    record_key,
)

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures"


def make_bundle(**overrides):
    data = json.loads((FIXTURE_DIR / "identity_mapping_v1.json").read_text())
    data.update(overrides)
    return bundle_from_mapping(data)


class LegacyKeyCompatibilityTests(unittest.TestCase):
    def test_legacy_functions_keep_historical_behavior(self):
        self.assertEqual(
            record_key(" ca100 ", "2026-11-01", " 2026-11-01T06:00:00Z "),
            "CA100|2026-11-01|2026-11-01T06:00:00Z",
        )
        self.assertEqual(flight_key(" ca100", "2026-11-01"), "CA100|2026-11-01")
        self.assertEqual(airport_key(" pek "), "PEK")
        # flight_key must never gain the FV2 prefix or airline logic.
        self.assertFalse(flight_key("CA100", "2026-11-01").startswith("FV2"))


class DeterministicNormalizationTests(unittest.TestCase):
    def setUp(self):
        self.resolver = FlightIdentityResolver(make_bundle())

    def test_case_whitespace_and_leading_zeros_normalize_to_one_identity(self):
        a = self.resolver.resolve(
            flight_id=" ca0100 ",
            flight_date="2026-11-01",
            airline_code="ca",
            origin="pek",
            destination="sha",
            scheduled_departure="2026-11-01T06:30:00Z",
        )
        b = self.resolver.resolve(
            flight_id="CA100",
            flight_date="2026-11-01",
            airline_code="CA",
            origin="PEK",
            destination="SHA",
            scheduled_departure="2026-11-01T06:30:00Z",
        )
        self.assertEqual(a.status, STATUS_EXACT)
        self.assertEqual(a.canonical_flight_key, b.canonical_flight_key)
        self.assertEqual(a.fingerprint, b.fingerprint)
        self.assertEqual(a.canonical_flight_key, "FV2|CA|100|2026-11-01")

    def test_designator_does_not_swallow_number_digits(self):
        designator, number_text, number, suffix, _ = normalize_flight_id("CA0100", "CA")
        self.assertEqual((designator, number_text, number, suffix), ("CA", "100", 100, ""))

    def test_leading_zero_collapse_can_be_disabled(self):
        bundle = make_bundle(collapse_leading_zeros=False)
        designator, number_text, _, _, _ = normalize_flight_id(
            "CA0100", "CA", collapse_leading_zeros=False
        )
        self.assertEqual((designator, number_text), ("CA", "0100"))
        # Rule numbers keep their padded form too.
        self.assertTrue(all(rule.number_text.isdigit() for rule in bundle.codeshares))

    def test_airport_normalization_and_validation(self):
        self.assertEqual(normalize_airport_code("peK"), ("PEK", True))
        code, valid = normalize_airport_code("P2K")
        self.assertEqual((code, valid), ("P2K", False))
        code, valid = normalize_airport_code(" PE ")
        self.assertEqual((code, valid), ("PE", False))
        code, valid = normalize_airport_code(None)
        self.assertEqual((code, valid), (None, False))

    def test_invalid_airport_makes_identity_invalid_without_guessing(self):
        result = self.resolver.resolve(
            flight_id="CA1001",
            flight_date="2026-11-01",
            airline_code="CA",
            origin="PE",
            destination="SHA",
        )
        self.assertEqual(result.status, STATUS_INVALID)
        self.assertIsNone(result.canonical_flight_key)
        self.assertTrue(any("invalid origin" in reason for reason in result.reasons))

    def test_flight_date_validation_and_boundary(self):
        self.assertEqual(normalize_flight_date("2026-11-01"), ("2026-11-01", True))
        self.assertEqual(normalize_flight_date("2026-02-30"), ("2026-02-30", False))
        result = self.resolver.resolve(
            flight_id="CA1001",
            flight_date="2026-11-01",
            airline_code="CA",
            origin="PEK",
            destination="SHA",
            scheduled_departure="2026-11-02T00:30:00Z",
        )
        self.assertEqual(result.status, STATUS_EXACT)
        self.assertIn("flight_date_boundary", result.reasons)
        # Operational flight_date wins for identity, not the timestamp date.
        self.assertTrue(result.canonical_flight_key.endswith("2026-11-01"))

    def test_missing_airline_is_invalid_not_guessed(self):
        result = self.resolver.resolve(
            flight_id="1001", flight_date="2026-11-01", origin="PEK", destination="SHA"
        )
        self.assertEqual(result.status, STATUS_INVALID)
        self.assertTrue(any("missing airline" in reason for reason in result.reasons))

    def test_unparseable_flight_id_is_invalid(self):
        result = self.resolver.resolve(
            flight_id="!!", flight_date="2026-11-01", airline_code="CA",
            origin="PEK", destination="SHA",
        )
        self.assertEqual(result.status, STATUS_INVALID)

    def test_airline_alias_chain_applies(self):
        result = self.resolver.resolve(
            flight_id="CCA1001",
            flight_date="2026-11-01",
            airline_code="CCA",
            origin="PEK",
            destination="SHA",
        )
        self.assertEqual(result.canonical_flight_key, "FV2|CA|1001|2026-11-01")
        bases = {e.basis for e in result.evidence}
        self.assertIn("airline_alias", bases)

    def test_airline_code_flight_id_mismatch_requires_manual_review(self):
        # Explicit carrier MU but the id carries CA, with no rule linking
        # them: the identity must not be auto-merged.
        result = self.resolver.resolve(
            flight_id="CA1001",
            flight_date="2026-11-01",
            airline_code="MU",
            origin="PEK",
            destination="SHA",
        )
        self.assertEqual(result.status, STATUS_AMBIGUOUS)
        self.assertIsNone(result.canonical_flight_key)
        self.assertIn("airline_code_flight_id_mismatch", result.reasons)

    def test_alias_connected_carrier_difference_stays_exact(self):
        result = self.resolver.resolve(
            flight_id="CCA1001",
            flight_date="2026-11-01",
            airline_code="CCA",
            origin="PEK",
            destination="SHA",
        )
        self.assertEqual(result.status, STATUS_EXACT)
        self.assertNotIn("airline_code_flight_id_mismatch", result.reasons)

    def test_explicit_flight_alias_applies(self):
        result = self.resolver.resolve(
            flight_id="MU7201",
            flight_date="2026-11-01",
            airline_code="MU",
            origin="SHA",
            destination="CAN",
        )
        self.assertEqual(result.canonical_flight_key, "FV2|MU|201|2026-11-01")
        self.assertIn("flight_alias", {e.basis for e in result.evidence})

    def test_canonical_key_includes_version_prefix(self):
        self.assertEqual(
            canonical_flight_key("CA", "100", "", "2026-11-01"),
            "FV2|CA|100|2026-11-01",
        )


class CodeshareResolutionTests(unittest.TestCase):
    def setUp(self):
        self.resolver = FlightIdentityResolver(make_bundle())

    def _resolve(self, flight_id, airline):
        return self.resolver.resolve(
            flight_id=flight_id,
            flight_date="2026-11-01",
            airline_code=airline,
            origin="PEK",
            destination="SHA",
            scheduled_departure="2026-11-01T06:30:00Z",
        )

    def test_marketing_flights_resolve_to_operating_identity(self):
        operating = self._resolve("CA1001", "CA")
        zh = self._resolve("ZH5001", "ZH")
        nx = self._resolve("NX108", "NX")
        self.assertEqual(
            operating.canonical_flight_key, "FV2|CA|1001|2026-11-01"
        )
        self.assertEqual(zh.canonical_flight_key, operating.canonical_flight_key)
        self.assertEqual(nx.canonical_flight_key, operating.canonical_flight_key)
        self.assertIn("codeshare", {e.basis for e in zh.evidence})
        # Evidence identifies the rule and both endpoints of the merge.
        cs_evidence = [e for e in zh.evidence if e.basis == "codeshare"][0]
        self.assertEqual(cs_evidence.rule_id, "CS-PEK-SHA-002")
        self.assertEqual(cs_evidence.output_value, "CA1001")

    def test_no_similarity_merge_without_rule(self):
        # ZH5002 looks similar to ZH5001 but has no rule; it stays distinct.
        no_rule = self._resolve("ZH5002", "ZH")
        with_rule = self._resolve("ZH5001", "ZH")
        self.assertNotEqual(
            no_rule.canonical_flight_key, with_rule.canonical_flight_key
        )
        self.assertEqual(no_rule.canonical_flight_key, "FV2|ZH|5002|2026-11-01")

    def test_same_airports_never_forces_a_merge(self):
        first = self.resolver.resolve(
            flight_id="AA100", flight_date="2026-11-01", airline_code="AA",
            origin="PEK", destination="SHA",
        )
        second = self.resolver.resolve(
            flight_id="BB200", flight_date="2026-11-01", airline_code="BB",
            origin="PEK", destination="SHA",
        )
        self.assertNotEqual(
            first.canonical_flight_key, second.canonical_flight_key
        )


class BatchConflictTests(unittest.TestCase):
    def setUp(self):
        self.resolver = FlightIdentityResolver(make_bundle())

    def test_shared_flight_id_without_mapping_is_ambiguous_conflict(self):
        records = [
            {"payload": {"flight_id": "X999", "flight_date": "2026-11-01",
                         "airline_code": "AA", "origin": "PEK", "destination": "SHA",
                         "scheduled_departure": "2026-11-01T01:00:00Z"}},
            {"payload": {"flight_id": "X999", "flight_date": "2026-11-01",
                         "airline_code": "BB", "origin": "CAN", "destination": "SZX",
                         "scheduled_departure": "2026-11-01T02:00:00Z"}},
        ]
        resolutions, conflicts = self.resolver.resolve_batch(records)
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0].reason, "shared_flight_id_conflict")
        self.assertTrue(all(r.status == STATUS_AMBIGUOUS for r in resolutions))
        self.assertTrue(all(r.canonical_flight_key is None for r in resolutions))

    def test_route_mismatch_downgrades_explicit_merge(self):
        records = [
            {"payload": {"flight_id": "CA1001", "flight_date": "2026-11-01",
                         "airline_code": "CA", "origin": "PEK", "destination": "SHA",
                         "scheduled_departure": "2026-11-01T06:30:00Z"}},
            {"payload": {"flight_id": "ZH5001", "flight_date": "2026-11-01",
                         "airline_code": "ZH", "origin": "CAN", "destination": "SZX",
                         "scheduled_departure": "2026-11-01T06:30:00Z"}},
        ]
        resolutions, conflicts = self.resolver.resolve_batch(records)
        reasons = {c.reason for c in conflicts}
        self.assertIn("route_mismatch", reasons)
        self.assertTrue(all(r.status == STATUS_AMBIGUOUS for r in resolutions))

    def test_consistent_codeshare_batch_stays_exact(self):
        records = [
            {"payload": {"flight_id": "CA1001", "flight_date": "2026-11-01",
                         "airline_code": "CA", "origin": "PEK", "destination": "SHA",
                         "scheduled_departure": "2026-11-01T06:30:00Z"}},
            {"payload": {"flight_id": "ZH5001", "flight_date": "2026-11-01",
                         "airline_code": "ZH", "origin": "PEK", "destination": "SHA",
                         "scheduled_departure": "2026-11-01T06:31:00Z"}},
        ]
        resolutions, conflicts = self.resolver.resolve_batch(records)
        self.assertEqual(conflicts, ())
        self.assertEqual(len({r.canonical_flight_key for r in resolutions}), 1)
        self.assertTrue(all(r.status == STATUS_EXACT for r in resolutions))

    def test_batch_output_is_order_independent(self):
        records = [
            {"payload": {"flight_id": f"AA{100 + i}", "flight_date": "2026-11-01",
                         "airline_code": "AA", "origin": "PEK", "destination": "SHA",
                         "scheduled_departure": "2026-11-01T01:00:00Z"}}
            for i in range(8)
        ]
        forward, conflicts_forward = self.resolver.resolve_batch(records)
        backward, conflicts_backward = self.resolver.resolve_batch(list(reversed(records)))
        self.assertEqual(
            sorted(r.fingerprint for r in forward),
            sorted(r.fingerprint for r in backward),
        )
        self.assertEqual(
            [r.canonical_flight_key for r in forward],
            list(reversed([r.canonical_flight_key for r in backward])),
        )
        self.assertEqual(conflicts_forward, conflicts_backward)


class MappingValidationTests(unittest.TestCase):
    def test_airline_alias_cycle_rejected(self):
        with self.assertRaises(AliasCycleError):
            MappingBundle(
                version="cyc",
                airline_aliases=(
                    AirlineAlias("r1", "AA", "BB"),
                    AirlineAlias("r2", "BB", "AA"),
                ),
            )

    def test_flight_alias_cycle_rejected(self):
        with self.assertRaises(AliasCycleError):
            MappingBundle(
                version="cyc2",
                flight_aliases=(
                    FlightAlias("r1", "AA", "1", "BB", "2"),
                    FlightAlias("r2", "BB", "2", "AA", "1"),
                ),
            )

    def test_codeshare_requires_single_operating_member(self):
        with self.assertRaises(MappingValidationError):
            MappingBundle(
                version="cs",
                codeshares=(CodeshareMembership("r1", "G", "AA", "1", False),),
            )
        with self.assertRaises(MappingValidationError):
            MappingBundle(
                version="cs2",
                codeshares=(
                    CodeshareMembership("r1", "G", "AA", "1", True),
                    CodeshareMembership("r2", "G", "BB", "2", True),
                ),
            )

    def test_duplicate_rule_ids_rejected(self):
        with self.assertRaises(MappingValidationError):
            MappingBundle(
                version="dup",
                airline_aliases=(
                    AirlineAlias("same", "AA", "CC"),
                    AirlineAlias("same", "BB", "CC"),
                ),
            )

    def test_alias_to_two_canonical_carriers_rejected(self):
        with self.assertRaises(MappingValidationError):
            MappingBundle(
                version="split",
                airline_aliases=(
                    AirlineAlias("r1", "AA", "BB"),
                    AirlineAlias("r2", "AA", "CC"),
                ),
            )

    def test_bundle_hash_is_order_independent(self):
        data = {
            "mapping_version": "v",
            "airline_aliases": [
                {"rule_id": f"a{i}", "alias": f"A{i}", "canonical": "CA"} for i in range(6)
            ],
        }
        first = bundle_from_mapping(json.loads(json.dumps(data)))
        shuffled = json.loads(json.dumps(data))
        shuffled["airline_aliases"].reverse()
        second = bundle_from_mapping(shuffled)
        self.assertEqual(first.bundle_hash, second.bundle_hash)

    def test_bundle_hash_changes_when_rules_change(self):
        first = make_bundle()
        second = make_bundle(mapping_version="mapping-2026-11-v2")
        third = bundle_from_mapping(
            {
                "mapping_version": "mapping-2026-11-v2",
                "airline_aliases": [
                    {"rule_id": "X", "alias": "ZZ", "canonical": "CA"}
                ],
            }
        )
        self.assertNotEqual(first.bundle_hash, second.bundle_hash)
        self.assertNotEqual(second.bundle_hash, third.bundle_hash)


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.resolver = FlightIdentityResolver(make_bundle(), cache_ttl_seconds=10)

    def test_repeated_calls_are_cached_and_bundle_switch_invalidates(self):
        kwargs = dict(
            flight_id="CA1001", flight_date="2026-11-01", airline_code="CA",
            origin="PEK", destination="SHA",
        )
        first = self.resolver.resolve(**kwargs)
        second = self.resolver.resolve(**kwargs)
        self.assertIs(first, second)
        switched = self.resolver.with_bundle(make_bundle(mapping_version="mapping-2026-11-v2"))
        third = switched.resolve(**kwargs)
        self.assertIsNot(third, first)
        self.assertEqual(third.mapping_version, "mapping-2026-11-v2")
        self.assertEqual(len(self.resolver.cache), 0)

    def test_ttl_expiry(self):
        cache = ResolutionCache(ttl_seconds=5)
        value = object()
        cache.set("k", value)  # type: ignore[arg-type]
        self.assertIs(cache.get("k", now=time.monotonic()), value)
        self.assertIsNone(cache.get("k", now=time.monotonic() + 6))

    def test_manual_invalidation(self):
        cache = ResolutionCache()
        cache.set("k", object())  # type: ignore[arg-type]
        self.assertEqual(len(cache), 1)
        cache.invalidate()
        self.assertEqual(len(cache), 0)


if __name__ == "__main__":
    unittest.main()

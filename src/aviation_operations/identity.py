"""Flight identity resolution.

This module keeps the historical key functions (``record_key``,
``flight_key``, ``airport_key``) untouched and adds a deterministic,
version-aware flight identity resolver on top of them.

Design rules
------------
* Normalization is purely deterministic (case, whitespace, flight number
  shape, ISO date).  Strings are *never* merged on similarity.
* Airport codes are validated but never participate in matching: two
  records are not considered the same flight just because origin and
  destination look alike.
* Any equivalence beyond deterministic normalization must come from an
  explicit, versioned alias or codeshare rule.
* Everything that is not provably unique becomes ``ambiguous`` or
  ``invalid`` and is routed to manual review instead of being merged.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

# ---------------------------------------------------------------------------
# Legacy key functions — behavior frozen for backwards compatibility.
# ---------------------------------------------------------------------------


def record_key(flight_id: str, flight_date: str, observed_at: str) -> str:
    return "|".join(value.strip().upper() for value in (flight_id, flight_date, observed_at))


def flight_key(flight_id: str, flight_date: str) -> str:
    return f"{flight_id.strip().upper()}|{flight_date.strip()}"


def airport_key(code: str) -> str:
    return code.strip().upper()


# ---------------------------------------------------------------------------
# Constants and errors
# ---------------------------------------------------------------------------

CANONICAL_KEY_VERSION = "FV2"
AIRLINE_RE = re.compile(r"^[A-Z0-9]{2,3}$")
AIRPORT_RE = re.compile(r"^[A-Z]{3}$")
FLIGHT_NUMBER_RE = re.compile(r"^[0-9]{1,4}[A-Z]?$")
DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

# Explicit status vocabulary.
STATUS_EXACT = "exact"
STATUS_AMBIGUOUS = "ambiguous"
STATUS_MANUAL = "manual"
STATUS_INVALID = "invalid"

# Evidence basis vocabulary.
BASIS_DETERMINISTIC = "deterministic"
BASIS_AIRLINE_ALIAS = "airline_alias"
BASIS_FLIGHT_ALIAS = "flight_alias"
BASIS_CODESHARE = "codeshare"
BASIS_MANUAL = "manual"
BASIS_DATE_BOUNDARY = "date_boundary"


class IdentityError(ValueError):
    """Base class for identity resolution failures."""


class MappingValidationError(IdentityError):
    """A versioned mapping bundle is internally inconsistent."""


class AliasCycleError(MappingValidationError):
    """Alias rules form a cycle and therefore cannot be resolved."""


class InvalidIdentityInput(IdentityError):
    """An input cannot be interpreted as a flight identity."""


# ---------------------------------------------------------------------------
# Normalized value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NormalizedFlight:
    airline: str | None
    flight_designator: str | None
    flight_number_text: str | None
    flight_number: int | None
    flight_suffix: str
    flight_date: str | None
    flight_date_valid: bool
    origin: str | None
    origin_valid: bool
    destination: str | None
    destination_valid: bool
    date_boundary: bool
    raw_flight_id: str

    @property
    def airport_issues(self) -> tuple[str, ...]:
        issues: list[str] = []
        if not self.origin_valid:
            issues.append(f"invalid origin airport: {self.origin!r}")
        if not self.destination_valid:
            issues.append(f"invalid destination airport: {self.destination!r}")
        return tuple(sorted(issues))


@dataclass(frozen=True)
class ResolutionEvidence:
    basis: str
    detail: str
    rule_id: str | None = None
    input_value: str | None = None
    output_value: str | None = None


@dataclass(frozen=True)
class CandidateConflict:
    reason: str
    review_key: str
    candidates: tuple[str, ...]
    detail: str


@dataclass(frozen=True)
class IdentityResolution:
    status: str
    canonical_flight_key: str | None
    review_key: str
    fingerprint: str
    normalized: NormalizedFlight
    candidates: tuple[str, ...]
    evidence: tuple[ResolutionEvidence, ...]
    reasons: tuple[str, ...]
    mapping_version: str
    bundle_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "canonical_flight_key": self.canonical_flight_key,
            "review_key": self.review_key,
            "fingerprint": self.fingerprint,
            "candidates": list(self.candidates),
            "reasons": list(self.reasons),
            "mapping_version": self.mapping_version,
            "bundle_hash": self.bundle_hash,
            "evidence": [
                {
                    "basis": e.basis,
                    "detail": e.detail,
                    "rule_id": e.rule_id,
                    "input_value": e.input_value,
                    "output_value": e.output_value,
                }
                for e in self.evidence
            ],
        }


# ---------------------------------------------------------------------------
# Versioned mapping rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AirlineAlias:
    rule_id: str
    alias: str
    canonical: str


@dataclass(frozen=True)
class FlightAlias:
    rule_id: str
    airline: str
    number_text: str
    canonical_airline: str
    canonical_number_text: str


@dataclass(frozen=True)
class CodeshareMembership:
    rule_id: str
    group_id: str
    airline: str
    number_text: str
    operating: bool


@dataclass(frozen=True)
class MappingBundle:
    """An immutable, versioned set of identity rules.

    Construction validates the whole bundle so that a resolver never has
    to reason about a broken mapping.  All internal structures are built
    from sorted inputs, making resolution independent of the iteration
    order of the source mapping.
    """

    version: str
    airline_aliases: tuple[AirlineAlias, ...] = ()
    flight_aliases: tuple[FlightAlias, ...] = ()
    codeshares: tuple[CodeshareMembership, ...] = ()
    collapse_leading_zeros: bool = True
    bundle_hash: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        if self.collapse_leading_zeros:
            flight_aliases = tuple(
                replace(
                    rule,
                    number_text=str(int(rule.number_text)),
                    canonical_number_text=str(int(rule.canonical_number_text)),
                )
                for rule in self.flight_aliases
            )
            codeshares = tuple(
                replace(rule, number_text=str(int(rule.number_text)))
                for rule in self.codeshares
            )
        else:
            flight_aliases = self.flight_aliases
            codeshares = self.codeshares
        airline_aliases = tuple(sorted(self.airline_aliases, key=lambda r: (r.rule_id, r.alias)))
        flight_aliases = tuple(
            sorted(flight_aliases, key=lambda r: (r.rule_id, r.airline, r.number_text))
        )
        codeshares = tuple(
            sorted(codeshares, key=lambda r: (r.group_id, r.rule_id, r.airline, r.number_text))
        )
        object.__setattr__(self, "airline_aliases", airline_aliases)
        object.__setattr__(self, "flight_aliases", flight_aliases)
        object.__setattr__(self, "codeshares", codeshares)
        if not self.version or not isinstance(self.version, str):
            raise MappingValidationError("mapping version is required")
        self._validate_rule_ids()
        self._validate_airline_codes()
        airline_graph = self._build_airline_graph(airline_aliases)
        _detect_cycle(airline_graph, kind="airline alias")
        flight_graph = self._build_flight_graph(flight_aliases)
        _detect_cycle(flight_graph, kind="flight alias")
        self._validate_codeshare_groups(codeshares)
        bundle_hash = hashlib.sha256(
            json.dumps(self._hash_payload(), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        object.__setattr__(self, "bundle_hash", bundle_hash)

    def _hash_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "collapse_leading_zeros": self.collapse_leading_zeros,
            "airline_aliases": [r.__dict__ for r in self.airline_aliases],
            "flight_aliases": [r.__dict__ for r in self.flight_aliases],
            "codeshares": [r.__dict__ for r in self.codeshares],
        }

    def _validate_rule_ids(self) -> None:
        seen: set[str] = set()
        for rule in (*self.airline_aliases, *self.flight_aliases, *self.codeshares):
            if not rule.rule_id:
                raise MappingValidationError("mapping rules require a rule_id")
            if rule.rule_id in seen:
                raise MappingValidationError(f"duplicate rule_id: {rule.rule_id}")
            seen.add(rule.rule_id)

    def _validate_airline_codes(self) -> None:
        values: list[str] = []
        for rule in self.airline_aliases:
            values.extend((rule.alias, rule.canonical))
        for rule in self.flight_aliases:
            values.extend((rule.airline, rule.canonical_airline))
        for rule in self.codeshares:
            values.append(rule.airline)
        for value in values:
            if not AIRLINE_RE.fullmatch(value or ""):
                raise MappingValidationError(f"invalid airline code in mapping: {value!r}")
        number_values: list[str] = []
        for rule in self.flight_aliases:
            number_values.extend((rule.number_text, rule.canonical_number_text))
        for rule in self.codeshares:
            number_values.append(rule.number_text)
        for value in number_values:
            if not re.fullmatch(r"[0-9]{1,4}", value or ""):
                raise MappingValidationError(f"invalid flight number in mapping: {value!r}")

    @staticmethod
    def _build_airline_graph(rules: tuple[AirlineAlias, ...]) -> dict[str, list[str]]:
        graph: dict[str, set[str]] = {}
        targets: dict[str, str] = {}
        for rule in rules:
            graph.setdefault(rule.alias, set()).add(rule.canonical)
            graph.setdefault(rule.canonical, set())
            previous = targets.get(rule.alias)
            if previous is not None and previous != rule.canonical:
                # One alias pointing at two different carriers is an
                # unresolvable conflict, fail closed at load time.
                raise MappingValidationError(
                    f"airline alias {rule.alias} maps to both {previous} and {rule.canonical}"
                )
            targets[rule.alias] = rule.canonical
        return {node: sorted(neighbours) for node, neighbours in sorted(graph.items())}

    @staticmethod
    def _build_flight_graph(rules: tuple[FlightAlias, ...]) -> dict[str, list[str]]:
        graph: dict[str, set[str]] = {}
        targets: dict[str, tuple[str, str]] = {}
        for rule in rules:
            source = f"{rule.airline}|{rule.number_text}"
            target = f"{rule.canonical_airline}|{rule.canonical_number_text}"
            graph.setdefault(source, set()).add(target)
            graph.setdefault(target, set())
            previous = targets.get(source)
            if previous is not None and previous != (rule.canonical_airline, rule.canonical_number_text):
                raise MappingValidationError(
                    f"flight alias {source} maps to multiple canonical flights"
                )
            targets[source] = (rule.canonical_airline, rule.canonical_number_text)
        return {node: sorted(neighbours) for node, neighbours in sorted(graph.items())}

    def _validate_codeshare_groups(self, rules: tuple[CodeshareMembership, ...]) -> None:
        groups: dict[str, list[CodeshareMembership]] = {}
        for rule in rules:
            groups.setdefault(rule.group_id, []).append(rule)
        for group_id in sorted(groups):
            members = groups[group_id]
            operating = [m for m in members if m.operating]
            if len(operating) != 1:
                raise MappingValidationError(
                    f"codeshare group {group_id} must declare exactly one operating flight, "
                    f"found {len(operating)}"
                )
            keys = [(m.airline, m.number_text) for m in members]
            if len(set(keys)) != len(keys):
                raise MappingValidationError(f"duplicate member in codeshare group {group_id}")


def _detect_cycle(graph: Mapping[str, Iterable[str]], *, kind: str) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    colors = {node: WHITE for node in graph}
    stack: list[str] = []

    def visit(node: str) -> None:
        colors[node] = GRAY
        stack.append(node)
        for neighbour in graph.get(node, ()):  # deterministic order from sorted build
            state = colors.get(neighbour, WHITE)
            if state == GRAY:
                start = stack.index(neighbour)
                cycle = stack[start:] + [neighbour]
                raise AliasCycleError(f"{kind} cycle detected: {' -> '.join(cycle)}")
            if state == WHITE:
                visit(neighbour)
        colors[node] = BLACK
        stack.pop()

    for node in sorted(graph):
        if colors[node] == WHITE:
            visit(node)


def bundle_from_mapping(data: dict[str, Any]) -> MappingBundle:
    """Build a validated bundle from a plain JSON-compatible mapping."""

    if not isinstance(data, dict):
        raise MappingValidationError("mapping document must be an object")
    version = str(data.get("mapping_version", "")).strip()
    if not version:
        raise MappingValidationError("mapping_version is required")

    airline_aliases = tuple(
        AirlineAlias(
            rule_id=str(item["rule_id"]),
            alias=_clean_airline(item["alias"]),
            canonical=_clean_airline(item["canonical"]),
        )
        for item in data.get("airline_aliases", [])
    )
    flight_aliases = tuple(
        FlightAlias(
            rule_id=str(item["rule_id"]),
            airline=_clean_airline(item["airline"]),
            number_text=_clean_number_text(item["number"]),
            canonical_airline=_clean_airline(item["canonical_airline"]),
            canonical_number_text=_clean_number_text(item["canonical_number"]),
        )
        for item in data.get("flight_aliases", [])
    )
    codeshares = tuple(
        CodeshareMembership(
            rule_id=str(item["rule_id"]),
            group_id=str(item["group_id"]),
            airline=_clean_airline(item["airline"]),
            number_text=_clean_number_text(item["number"]),
            operating=bool(item.get("operating", False)),
        )
        for item in data.get("codeshare_rules", [])
    )
    return MappingBundle(
        version=version,
        airline_aliases=airline_aliases,
        flight_aliases=flight_aliases,
        codeshares=codeshares,
        collapse_leading_zeros=bool(data.get("collapse_leading_zeros", True)),
    )


def load_bundle(path: str | Path) -> MappingBundle:
    with Path(path).open("r", encoding="utf-8") as handle:
        return bundle_from_mapping(json.load(handle))


def _clean_airline(value: Any) -> str:
    return str(value).strip().upper()


def _clean_number_text(value: Any) -> str:
    return str(value).strip().upper()


# ---------------------------------------------------------------------------
# Deterministic normalization primitives
# ---------------------------------------------------------------------------


def normalize_airline_code(code: str | None) -> str | None:
    if code is None:
        return None
    cleaned = str(code).strip().upper()
    return cleaned or None


def normalize_airport_code(code: str | None) -> tuple[str | None, bool]:
    """Return ``(uppercase code, valid)``.

    Mixed case is a deterministic cleanup.  Empty, wrong-length or
    non-alpha codes are reported invalid instead of being guessed at.
    """

    cleaned = None if code is None else str(code).strip().upper()
    if not cleaned:
        return None, False
    return cleaned, bool(AIRPORT_RE.fullmatch(cleaned))


def normalize_flight_date(value: str | None) -> tuple[str | None, bool]:
    if value is None:
        return None, False
    cleaned = str(value).strip()
    if not DATE_RE.fullmatch(cleaned):
        return cleaned or None, False
    try:
        date.fromisoformat(cleaned)
    except ValueError:
        return cleaned, False
    return cleaned, True


def normalize_flight_id(
    raw_flight_id: str | None,
    airline_code: str | None = None,
    *,
    collapse_leading_zeros: bool = True,
) -> tuple[str | None, str | None, str | None, int | None, str]:
    """Split a flight id into ``(designator, number_text, number, suffix, raw)``.

    The split point is the first digit, so a designator can never swallow
    flight-number digits (``CA01001`` is airline ``CA`` + number ``01001``).
    When an explicit ``airline_code`` is given it must agree with the
    designator found in the flight id; the mismatch is returned to the
    caller via the designator/airline pair rather than silently repaired.
    """

    airline = normalize_airline_code(airline_code)
    raw = "" if raw_flight_id is None else str(raw_flight_id).strip().upper()
    if not raw:
        return airline, None, None, None, ""

    split_at = next((i for i, char in enumerate(raw) if char.isdigit()), len(raw))
    prefix, remainder = raw[:split_at], raw[split_at:]
    if not remainder or not FLIGHT_NUMBER_RE.fullmatch(remainder):
        return airline, None, None, None, raw

    number_match = re.match(r"^([0-9]{1,4})([A-Z]?)$", remainder)
    assert number_match is not None
    number_text, suffix = number_match.group(1), number_match.group(2)
    number = int(number_text)
    if collapse_leading_zeros:
        number_text = str(number)

    designator = prefix or airline
    if prefix and airline and prefix != airline:
        # Keep both facts: the designator embedded in the id drives
        # mapping, while the mismatch itself is surfaced by the resolver.
        designator = prefix
    return designator, number_text, number, suffix, raw


def canonical_flight_key(airline: str, number_text: str, suffix: str, flight_date: str) -> str:
    return f"{CANONICAL_KEY_VERSION}|{airline}|{number_text}{suffix}|{flight_date}"


def _review_key(parts: Iterable[str]) -> str:
    digest = hashlib.sha256("\u001f".join(sorted(parts)).encode("utf-8")).hexdigest()
    return f"RVW|{digest[:16]}"


def _fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


# ---------------------------------------------------------------------------
# TTL cache
# ---------------------------------------------------------------------------


class ResolutionCache:
    """Thread-safe TTL cache for resolution results."""

    def __init__(self, ttl_seconds: float = 300.0, max_entries: int = 10_000) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._lock = threading.RLock()
        self._entries: dict[str, tuple[float, IdentityResolution]] = {}

    def get(self, key: str, *, now: float | None = None) -> IdentityResolution | None:
        moment = time.monotonic() if now is None else now
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, resolution = entry
            if expires_at <= moment:
                self._entries.pop(key, None)
                return None
            return resolution

    def set(self, key: str, resolution: IdentityResolution, *, now: float | None = None) -> None:
        moment = time.monotonic() if now is None else now
        with self._lock:
            self._entries[key] = (moment + self.ttl_seconds, resolution)
            if len(self._entries) > self.max_entries:  # pragma: no cover - capacity guard
                for stale_key in sorted(self._entries)[: len(self._entries) - self.max_entries]:
                    self._entries.pop(stale_key, None)

    def invalidate(self) -> None:
        with self._lock:
            self._entries.clear()

    def purge_expired(self, *, now: float | None = None) -> int:
        moment = time.monotonic() if now is None else now
        with self._lock:
            stale = [key for key, (expires_at, _) in self._entries.items() if expires_at <= moment]
            for key in stale:
                self._entries.pop(key, None)
            return len(stale)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


class FlightIdentityResolver:
    """Pure, deterministic flight identity resolver.

    The resolver holds one immutable :class:`MappingBundle`.  Switching
    mappings means constructing a new resolver (or calling
    :meth:`with_bundle`), which also invalidates the cache.
    """

    def __init__(self, bundle: MappingBundle, cache_ttl_seconds: float = 300.0) -> None:
        self.bundle = bundle
        self.cache = ResolutionCache(ttl_seconds=cache_ttl_seconds)
        self._airline_lookup = self._build_airline_lookup(bundle)
        self._flight_lookup = self._build_flight_lookup(bundle)
        self._codeshare_lookup, self._group_operating = self._build_codeshare_lookup(bundle)

    def with_bundle(self, bundle: MappingBundle) -> "FlightIdentityResolver":
        self.cache.invalidate()
        return FlightIdentityResolver(bundle, cache_ttl_seconds=self.cache.ttl_seconds)

    @staticmethod
    def _build_airline_lookup(bundle: MappingBundle) -> dict[str, AirlineAlias]:
        lookup: dict[str, AirlineAlias] = {}
        for rule in bundle.airline_aliases:  # already sorted deterministically
            lookup.setdefault(rule.alias, rule)
        return lookup

    @staticmethod
    def _build_flight_lookup(bundle: MappingBundle) -> dict[tuple[str, str], FlightAlias]:
        lookup: dict[tuple[str, str], FlightAlias] = {}
        for rule in bundle.flight_aliases:
            lookup.setdefault((rule.airline, rule.number_text), rule)
        return lookup

    @staticmethod
    def _build_codeshare_lookup(
        bundle: MappingBundle,
    ) -> tuple[dict[tuple[str, str], CodeshareMembership], dict[str, CodeshareMembership]]:
        membership: dict[tuple[str, str], CodeshareMembership] = {}
        operating: dict[str, CodeshareMembership] = {}
        for rule in bundle.codeshares:
            membership.setdefault((rule.airline, rule.number_text), rule)
            if rule.operating:
                operating[rule.group_id] = rule
        return membership, operating

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def normalize(
        self,
        *,
        flight_id: str | None,
        flight_date: str | None,
        airline_code: str | None = None,
        origin: str | None = None,
        destination: str | None = None,
        scheduled_departure: str | None = None,
    ) -> NormalizedFlight:
        airline = normalize_airline_code(airline_code)
        designator, number_text, number, suffix, raw = normalize_flight_id(
            flight_id, airline, collapse_leading_zeros=self.bundle.collapse_leading_zeros
        )
        date_text, date_valid = normalize_flight_date(flight_date)
        origin_code, origin_valid = normalize_airport_code(origin)
        destination_code, destination_valid = normalize_airport_code(destination)
        date_boundary = False
        if date_valid and scheduled_departure:
            scheduled_text = str(scheduled_departure).strip()
            if scheduled_text:
                try:
                    scheduled_dt = datetime.fromisoformat(scheduled_text.replace("Z", "+00:00"))
                    date_boundary = scheduled_dt.date().isoformat() != date_text
                except ValueError:
                    date_boundary = False
        return NormalizedFlight(
            airline=airline,
            flight_designator=designator,
            flight_number_text=number_text,
            flight_number=number,
            flight_suffix=suffix,
            flight_date=date_text,
            flight_date_valid=date_valid,
            origin=origin_code,
            origin_valid=origin_valid,
            destination=destination_code,
            destination_valid=destination_valid,
            date_boundary=date_boundary,
            raw_flight_id=raw,
        )

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(
        self,
        *,
        flight_id: str | None,
        flight_date: str | None,
        airline_code: str | None = None,
        origin: str | None = None,
        destination: str | None = None,
        scheduled_departure: str | None = None,
    ) -> IdentityResolution:
        normalized = self.normalize(
            flight_id=flight_id,
            flight_date=flight_date,
            airline_code=airline_code,
            origin=origin,
            destination=destination,
            scheduled_departure=scheduled_departure,
        )
        fingerprint = _fingerprint(
            {
                "airline": normalized.airline,
                "designator": normalized.flight_designator,
                "number_text": normalized.flight_number_text,
                "number": normalized.flight_number,
                "suffix": normalized.flight_suffix,
                "flight_date": normalized.flight_date,
                "origin": normalized.origin,
                "destination": normalized.destination,
                "scheduled_departure": scheduled_departure,
                "collapse_leading_zeros": self.bundle.collapse_leading_zeros,
            }
        )
        cache_key = f"{self.bundle.bundle_hash}|{fingerprint}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        evidence: list[ResolutionEvidence] = [
            ResolutionEvidence(
                basis=BASIS_DETERMINISTIC,
                detail="normalized flight id, airline, airports and flight date",
                input_value=str(flight_id),
                output_value=self._render_normalized(normalized),
            )
        ]
        reasons: set[str] = set()

        invalid_reasons = self._invalid_reasons(normalized)
        if invalid_reasons:
            reasons.update(invalid_reasons)
            resolution = IdentityResolution(
                status=STATUS_INVALID,
                canonical_flight_key=None,
                review_key=_review_key([fingerprint, *sorted(reasons)]),
                fingerprint=fingerprint,
                normalized=normalized,
                candidates=(),
                evidence=tuple(evidence),
                reasons=tuple(sorted(reasons)),
                mapping_version=self.bundle.version,
                bundle_hash=self.bundle.bundle_hash,
            )
            self.cache.set(cache_key, resolution)
            return resolution

        candidates, rule_evidence, match_reasons = self._candidate_identities(normalized)
        evidence.extend(rule_evidence)
        reasons.update(match_reasons)

        if normalized.date_boundary:
            evidence.append(
                ResolutionEvidence(
                    basis=BASIS_DATE_BOUNDARY,
                    detail="scheduled_departure date differs from operational flight_date; "
                    "flight_date retained as identity date",
                    input_value=str(scheduled_departure),
                    output_value=normalized.flight_date,
                )
            )
            reasons.add("flight_date_boundary")

        unique_candidates = tuple(sorted(set(candidates)))
        requires_review = bool(match_reasons)
        if len(unique_candidates) == 1 and not requires_review:
            resolution = IdentityResolution(
                status=STATUS_EXACT,
                canonical_flight_key=unique_candidates[0],
                review_key=_review_key([fingerprint, unique_candidates[0]]),
                fingerprint=fingerprint,
                normalized=normalized,
                candidates=unique_candidates,
                evidence=tuple(evidence),
                reasons=tuple(sorted(reasons)),
                mapping_version=self.bundle.version,
                bundle_hash=self.bundle.bundle_hash,
            )
        else:
            if len(unique_candidates) != 1:
                reasons.add("multiple_candidate_identities")
            resolution = IdentityResolution(
                status=STATUS_AMBIGUOUS,
                canonical_flight_key=None,
                review_key=_review_key([fingerprint, *unique_candidates]),
                fingerprint=fingerprint,
                normalized=normalized,
                candidates=unique_candidates,
                evidence=tuple(evidence),
                reasons=tuple(sorted(reasons)),
                mapping_version=self.bundle.version,
                bundle_hash=self.bundle.bundle_hash,
            )
        self.cache.set(cache_key, resolution)
        return resolution

    @staticmethod
    def _render_normalized(normalized: NormalizedFlight) -> str:
        return "|".join(
            [
                normalized.airline or "",
                normalized.flight_designator or "",
                normalized.flight_number_text or "",
                normalized.flight_suffix or "",
                normalized.flight_date or "",
            ]
        )

    @staticmethod
    def _invalid_reasons(normalized: NormalizedFlight) -> list[str]:
        reasons: list[str] = []
        if not normalized.flight_date_valid:
            reasons.append(f"invalid flight_date: {normalized.flight_date!r}")
        if normalized.airline is None and normalized.flight_designator is None:
            reasons.append("missing airline: neither airline_code nor flight id designator present")
        if normalized.flight_designator is None or normalized.flight_number_text is None:
            reasons.append(f"unparseable flight id: {normalized.raw_flight_id!r}")
        elif not AIRLINE_RE.fullmatch(normalized.flight_designator):
            reasons.append(f"invalid airline designator: {normalized.flight_designator!r}")
        reasons.extend(normalized.airport_issues)
        return sorted(reasons)

    def _candidate_identities(
        self, normalized: NormalizedFlight
    ) -> tuple[list[str], list[ResolutionEvidence], set[str]]:
        evidence: list[ResolutionEvidence] = []
        reasons: set[str] = set()
        assert normalized.flight_designator is not None
        assert normalized.flight_number_text is not None
        assert normalized.flight_date is not None

        designator = normalized.flight_designator
        number_text = normalized.flight_number_text
        suffix = normalized.flight_suffix

        # 1. Airline alias chain (bundle validation guarantees no cycle).
        resolved_airline = designator
        seen_airlines: set[str] = set()
        while resolved_airline in self._airline_lookup and resolved_airline not in seen_airlines:
            seen_airlines.add(resolved_airline)
            rule = self._airline_lookup[resolved_airline]
            evidence.append(
                ResolutionEvidence(
                    basis=BASIS_AIRLINE_ALIAS,
                    detail=f"airline alias {rule.alias} -> {rule.canonical}",
                    rule_id=rule.rule_id,
                    input_value=rule.alias,
                    output_value=rule.canonical,
                )
            )
            resolved_airline = rule.canonical
        if resolved_airline in seen_airlines:  # defensive, loader should have failed
            raise AliasCycleError("airline alias cycle encountered during resolution")

        # Explicit airline_code disagreeing with the flight id designator
        # is only acceptable when an alias rule connects the two.
        if (
            normalized.airline is not None
            and normalized.airline != designator
            and resolved_airline != self._resolve_airline_chain(normalized.airline)
        ):
            reasons.add("airline_code_flight_id_mismatch")

        # 2. Explicit flight number alias chain.
        airline, number = resolved_airline, number_text
        seen_flights: set[tuple[str, str]] = set()
        while (airline, number) in self._flight_lookup and (airline, number) not in seen_flights:
            seen_flights.add((airline, number))
            rule = self._flight_lookup[(airline, number)]
            evidence.append(
                ResolutionEvidence(
                    basis=BASIS_FLIGHT_ALIAS,
                    detail=f"flight alias {rule.airline}{rule.number_text} -> "
                    f"{rule.canonical_airline}{rule.canonical_number_text}",
                    rule_id=rule.rule_id,
                    input_value=f"{rule.airline}{rule.number_text}",
                    output_value=f"{rule.canonical_airline}{rule.canonical_number_text}",
                )
            )
            airline, number = rule.canonical_airline, rule.canonical_number_text
        if (airline, number) in seen_flights:  # defensive
            raise AliasCycleError("flight alias cycle encountered during resolution")

        candidate_keys: list[str] = []
        direct_key = canonical_flight_key(airline, number, suffix, normalized.flight_date)
        candidate_keys.append(direct_key)

        # 3. Optional codeshare membership: only explicit rules merge
        #    marketing and operating flight numbers.
        membership = self._codeshare_lookup.get((airline, number))
        if membership is not None:
            operating = self._group_operating[membership.group_id]
            operating_key = canonical_flight_key(
                operating.airline, operating.number_text, "", normalized.flight_date
            )
            evidence.append(
                ResolutionEvidence(
                    basis=BASIS_CODESHARE,
                    detail=(
                        f"codeshare group {membership.group_id}: "
                        f"{airline}{number} is marketed by {airline}, "
                        f"operating flight {operating.airline}{operating.number_text}"
                    ),
                    rule_id=membership.rule_id,
                    input_value=f"{airline}{number}",
                    output_value=f"{operating.airline}{operating.number_text}",
                )
            )
            # The canonical identity of a codeshare is the operating flight.
            candidate_keys = [operating_key]
        return candidate_keys, evidence, reasons

    def _resolve_airline_chain(self, airline: str) -> str:
        resolved = airline
        seen: set[str] = set()
        while resolved in self._airline_lookup and resolved not in seen:
            seen.add(resolved)
            resolved = self._airline_lookup[resolved].canonical
        return resolved

    # ------------------------------------------------------------------
    # Batch level conflict detection
    # ------------------------------------------------------------------

    def resolve_batch(
        self, records: Iterable[Any], *, attribute_getter: Any = None
    ) -> tuple[list[IdentityResolution], list[CandidateConflict]]:
        """Resolve a batch and flag same-batch identity conflicts.

        Conflicts are derived deterministically from explicit rule output,
        never from string similarity or airport equality:

        * ``shared_flight_id_conflict`` — several airlines claim the same
          raw flight id/date without a mapping connecting them;
        * ``route_mismatch`` — inputs merged by explicit rules into one
          canonical key carry contradictory airport pairs.
        """

        getter = attribute_getter or _default_record_getter
        resolutions = [self.resolve(**getter(record)) for record in records]

        raw_groups: dict[tuple[str, str], list[int]] = {}
        canonical_groups: dict[str, list[int]] = {}
        for index, resolution in enumerate(resolutions):
            normalized = resolution.normalized
            if normalized.raw_flight_id and normalized.flight_date:
                raw_groups.setdefault(
                    (normalized.raw_flight_id, normalized.flight_date), []
                ).append(index)
            if resolution.canonical_flight_key:
                canonical_groups.setdefault(resolution.canonical_flight_key, []).append(index)

        # review_key -> extra reason; applied to resolutions after
        # detection so the frozen value objects stay immutable.
        downgrades: dict[int, list[tuple[str, str]]] = {}
        conflicts: list[CandidateConflict] = []

        for (raw_id, raw_date), indexes in sorted(raw_groups.items()):
            group = [resolutions[i] for i in indexes]
            airlines = {
                r.normalized.airline or r.normalized.flight_designator
                for r in group
                if (r.normalized.airline or r.normalized.flight_designator)
            }
            canonical_keys = {r.canonical_flight_key for r in group if r.canonical_flight_key}
            if len(airlines) > 1 and len(canonical_keys) != 1:
                review = _review_key([raw_id, raw_date, *sorted(a or "" for a in airlines)])
                conflicts.append(
                    CandidateConflict(
                        reason="shared_flight_id_conflict",
                        review_key=review,
                        candidates=tuple(sorted(r.review_key for r in group)),
                        detail=f"flight id {raw_id} on {raw_date} claimed by airlines "
                        f"{sorted(airlines)} without a connecting mapping",
                    )
                )
                for index in indexes:
                    downgrades.setdefault(index, []).append(("shared_flight_id_conflict", review))

        for canonical, indexes in sorted(canonical_groups.items()):
            group = [resolutions[i] for i in indexes]
            routes = {
                (r.normalized.origin, r.normalized.destination)
                for r in group
                if r.normalized.origin_valid and r.normalized.destination_valid
            }
            distinct_inputs = {r.fingerprint for r in group}
            if len(routes) > 1 and len(distinct_inputs) > 1:
                review = _review_key([canonical, *("-".join(r) for r in sorted(routes))])
                conflicts.append(
                    CandidateConflict(
                        reason="route_mismatch",
                        review_key=review,
                        candidates=tuple(sorted(r.fingerprint for r in group)),
                        detail=f"canonical flight {canonical} observed with contradictory "
                        f"airport pairs {sorted(routes)}",
                    )
                )
                for index in indexes:
                    downgrades.setdefault(index, []).append(("route_mismatch", review))

        final_resolutions: list[IdentityResolution] = []
        for index, resolution in enumerate(resolutions):
            markers = downgrades.get(index)
            if not markers:
                final_resolutions.append(resolution)
                continue
            extra_reasons = sorted({reason for reason, _ in markers})
            review_key = _review_key([resolution.review_key, *(key for _, key in markers)])
            final_resolutions.append(
                replace(
                    resolution,
                    status=STATUS_AMBIGUOUS,
                    canonical_flight_key=None,
                    review_key=review_key,
                    reasons=tuple(sorted({*resolution.reasons, *extra_reasons})),
                )
            )

        return final_resolutions, tuple(sorted(conflicts, key=lambda c: (c.reason, c.review_key)))


def _default_record_getter(record: Any) -> dict[str, Any]:
    if isinstance(record, dict):
        payload = record.get("payload", record)
        return {
            "flight_id": payload.get("flight_id"),
            "flight_date": payload.get("flight_date"),
            "airline_code": payload.get("airline_code"),
            "origin": payload.get("origin"),
            "destination": payload.get("destination"),
            "scheduled_departure": payload.get("scheduled_departure"),
        }
    return {
        "flight_id": getattr(record, "flight_id"),
        "flight_date": getattr(record, "flight_date"),
        "airline_code": getattr(record, "airline_code", None),
        "origin": getattr(record, "origin", None),
        "destination": getattr(record, "destination", None),
        "scheduled_departure": getattr(record, "scheduled_departure", None),
    }

"""Pure-domain reconstruction of a single flight's lifecycle trajectory.

The module never touches SQLite, files or clocks.  It turns the (possibly
out-of-order, conflicting and duplicated) :class:`~aviation_operations.models.FlightRecord`
observations already normalised for one ``flight_id``/``flight_date`` into an
explainable state trajectory.

Consistency protocol
--------------------
Two independent conflict axes are handled on top of the existing
:func:`aviation_operations.lifecycle.event_family` semantics:

* ``same_timestamp`` – several sources publish a state at the *same*
  ``observed_at``.  Nothing in the arrival order can break the tie, so the
  configured policy decides.
* ``transition`` – an observation conflicts with the already adopted
  trajectory (state after a terminal state, backwards movement, cancellation
  followed by another state, ...).

For each axis one of three protocols is configurable independently:

``reject``
    The conflicting observation is never adopted.  It is still recorded as an
    auditable conflict with reason ``rejected_*``.

``keep_conflict``
    The previous adoption wins, but the rejected observation is kept as a
    *conflicting state segment* (``adopted=False``) so downstream consumers
    see both stories instead of one silently overwriting the other.

``source_priority``
    The source ranking decides.  Higher priority wins; an equal priority is
    broken deterministically by status rank, record key and source so the
    result never depends on insertion order.  A loser that cannot win is still
    recorded as an auditable conflict.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from .lifecycle import event_family
from .models import FlightRecord

#: Canonical ordering of flight states along a normal lifecycle.
STATUS_RANK: Mapping[str, int] = {
    "scheduled": 10,
    "boarding": 20,
    "delayed": 30,
    "departed": 40,
    "arrived": 50,
    "cancelled": 60,
}

#: States from which no further state may be adopted without a conflict.
TERMINAL_STATES: frozenset[str] = frozenset({"arrived", "cancelled"})

#: Supported consistency protocols.
REJECT = "reject"
KEEP_CONFLICT = "keep_conflict"
SOURCE_PRIORITY = "source_priority"
POLICIES: frozenset[str] = frozenset({REJECT, KEEP_CONFLICT, SOURCE_PRIORITY})


class LifecycleConfigError(ValueError):
    """Raised when a lifecycle consistency configuration is invalid."""


class ResourceLimitExceeded(RuntimeError):
    """Raised when a single flight holds more observations than allowed."""


@dataclass(frozen=True)
class LifecycleConfig:
    """Configurable and auditable consistency protocol for one rebuild.

    The full configuration is hashed (:meth:`fingerprint`) and stored with
    every published version, so every trajectory can be reproduced later.
    """

    same_timestamp_policy: str = SOURCE_PRIORITY
    transition_policy: str = REJECT
    #: Higher rank wins.  Sources absent from the mapping share rank 0.
    source_priority: Mapping[str, int] = field(default_factory=dict)
    #: Maximum number of observations considered for one flight.
    max_observations: int = 10_000

    def __post_init__(self) -> None:
        if self.same_timestamp_policy not in POLICIES:
            raise LifecycleConfigError(f"invalid same_timestamp_policy: {self.same_timestamp_policy!r}")
        if self.transition_policy not in POLICIES:
            raise LifecycleConfigError(f"invalid transition_policy: {self.transition_policy!r}")
        if self.max_observations <= 0:
            raise LifecycleConfigError("max_observations must be positive")
        if any(not isinstance(rank, int) for rank in self.source_priority.values()):
            raise LifecycleConfigError("source_priority ranks must be integers")

    def rank_of(self, source: str) -> int:
        return int(self.source_priority.get(source, 0))

    def fingerprint(self) -> str:
        body = {
            "same_timestamp_policy": self.same_timestamp_policy,
            "transition_policy": self.transition_policy,
            "source_priority": dict(sorted(self.source_priority.items())),
            "max_observations": self.max_observations,
        }
        digest = hashlib.sha256(
            json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return f"cfg:{digest[:16]}"


@dataclass(frozen=True)
class Observation:
    """One normalised record participating in a rebuild."""

    record_key: str
    source: str
    observed_at: str
    status: str
    event_family: str
    actual_departure: str | None
    scheduled_departure: str
    raw_hash: str

    @classmethod
    def from_record(cls, record: FlightRecord) -> "Observation":
        return cls(
            record_key=record.record_key,
            source=record.source,
            observed_at=record.observed_at,
            status=record.status,
            event_family=event_family(record.status),
            actual_departure=record.actual_departure,
            scheduled_departure=record.scheduled_departure,
            raw_hash=record.raw_hash,
        )


@dataclass(frozen=True)
class StateSegment:
    """One state adoption (or explicitly retained conflict) in the trajectory."""

    sequence: int
    status: str
    event_family: str
    record_key: str
    source: str
    observed_at: str
    effective_from: str
    effective_to: str | None
    adopted: bool
    decision: str
    conflict_reason: str | None
    actual_departure: str | None
    quality_issues: tuple[str, ...]
    raw_hash: str


@dataclass(frozen=True)
class Conflict:
    """Audit row explaining why an observation was not cleanly adopted."""

    record_key: str
    source: str
    observed_at: str
    status: str
    event_family: str
    conflict_axis: str  # "same_timestamp" | "transition" | "quality"
    decision: str
    reason: str
    competing_record_key: str | None
    competing_source: str | None
    raw_hash: str


@dataclass(frozen=True)
class Trajectory:
    """Result of rebuilding one flight key."""

    flight_id: str
    flight_date: str
    flight_key: str
    config_fingerprint: str
    input_fingerprint: str
    segments: tuple[StateSegment, ...]
    conflicts: tuple[Conflict, ...]
    observation_count: int
    quality_warning_count: int

    @property
    def current_state(self) -> str | None:
        adopted = [segment for segment in self.segments if segment.adopted]
        return adopted[-1].status if adopted else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "flight_id": self.flight_id,
            "flight_date": self.flight_date,
            "flight_key": self.flight_key,
            "current_state": self.current_state,
            "config_fingerprint": self.config_fingerprint,
            "input_fingerprint": self.input_fingerprint,
            "observation_count": self.observation_count,
            "quality_warning_count": self.quality_warning_count,
            "segments": [segment.__dict__ for segment in self.segments],
            "conflicts": [conflict.__dict__ for conflict in self.conflicts],
        }


def parse_timestamp(value: str) -> datetime | None:
    """Parse the timestamp shapes accepted by the platform.

    Both explicit-offset (``...Z`` / ``+HH:MM``) and naive local timestamps
    are accepted; naive values are assumed UTC.  ``None``/unparseable input
    returns ``None`` so callers can attach an explainable quality issue.
    """

    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _quality_issues(observation: Observation, flight_date: str) -> list[str]:
    issues: list[str] = []
    if parse_timestamp(observation.observed_at) is None:
        issues.append(f"unparseable observed_at timestamp: {observation.observed_at!r}")
    if observation.status in {"departed", "arrived"}:
        if not observation.actual_departure:
            issues.append(f"{observation.status} observation without actual_departure")
        else:
            actual = parse_timestamp(observation.actual_departure)
            if actual is None:
                issues.append(
                    f"actual_departure format is inconsistent: {observation.actual_departure!r}"
                )
            elif actual.date().isoformat() != flight_date:
                issues.append(
                    "actual_departure crosses flight_date "
                    f"({actual.date().isoformat()} != {flight_date})"
                )
    elif observation.actual_departure:
        actual = parse_timestamp(observation.actual_departure)
        if actual is None:
            issues.append(
                f"actual_departure format is inconsistent: {observation.actual_departure!r}"
            )
    return issues


def _sort_key(observation: Observation) -> tuple[Any, ...]:
    parsed = parse_timestamp(observation.observed_at)
    timestamp = parsed or datetime.max.replace(tzinfo=timezone.utc)
    # Fully deterministic, independent of table scan order.
    return (
        timestamp,
        -STATUS_RANK.get(observation.status, -1),
        observation.record_key,
        observation.source,
    )


def _source_tie_key(config: LifecycleConfig, observation: Observation) -> tuple[int, int, str, str]:
    return (
        config.rank_of(observation.source),
        STATUS_RANK.get(observation.status, -1),
        observation.record_key,
        observation.source,
    )


def _input_fingerprint(observations: Sequence[Observation]) -> str:
    body = [
        {
            "record_key": item.record_key,
            "source": item.source,
            "observed_at": item.observed_at,
            "status": item.status,
            "actual_departure": item.actual_departure,
            "raw_hash": item.raw_hash,
        }
        for item in sorted(observations, key=lambda item: (item.record_key, item.source))
    ]
    digest = hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"in:{digest[:16]}"


def rebuild(
    flight_id: str,
    flight_date: str,
    flight_key: str,
    records: Sequence[FlightRecord],
    config: LifecycleConfig,
) -> Trajectory:
    """Rebuild the explainable lifecycle trajectory for one flight.

    Pure function: identical inputs (records + config) always produce an
    identical trajectory and fingerprint.
    """

    if len(records) > config.max_observations:
        raise ResourceLimitExceeded(
            f"flight {flight_key} has {len(records)} observations, "
            f"limit is {config.max_observations}"
        )

    observations = sorted(
        (Observation.from_record(record) for record in records), key=_sort_key
    )
    # Exact replays (same domain record_key and source) collapse to one
    # observation; the duplicate is still surfaced as an auditable conflict.
    deduped: list[Observation] = []
    replay_conflicts: list[Conflict] = []
    seen: set[tuple[str, str]] = set()
    for observation in observations:
        identity = (observation.record_key, observation.source)
        if identity in seen:
            replay_conflicts.append(
                Conflict(
                    record_key=observation.record_key,
                    source=observation.source,
                    observed_at=observation.observed_at,
                    status=observation.status,
                    event_family=observation.event_family,
                    conflict_axis="transition",
                    decision="rejected_duplicate_replay",
                    reason="transition: duplicate replay of an already ingested record",
                    competing_record_key=observation.record_key,
                    competing_source=observation.source,
                    raw_hash=observation.raw_hash,
                )
            )
            continue
        seen.add(identity)
        deduped.append(observation)
    observations = deduped
    input_fingerprint = _input_fingerprint(observations)

    segments: list[StateSegment] = []
    conflicts: list[Conflict] = []
    quality_warnings = 0
    sequence = 0
    # Latest *adopted* segment; transition conflicts are judged against it.
    adopted_head: StateSegment | None = None

    def close_open_segment(observed_at: str) -> None:
        if segments and segments[-1].adopted and segments[-1].effective_to is None:
            index = len(segments) - 1
            segments[index] = StateSegment(
                **{**segments[index].__dict__, "effective_to": observed_at}
            )

    def adopt(observation: Observation, decision: str, reason: str | None, issues: list[str]) -> None:
        nonlocal sequence, adopted_head
        close_open_segment(observation.observed_at)
        sequence += 1
        segment = StateSegment(
            sequence=sequence,
            status=observation.status,
            event_family=observation.event_family,
            record_key=observation.record_key,
            source=observation.source,
            observed_at=observation.observed_at,
            effective_from=observation.observed_at,
            effective_to=None,
            adopted=True,
            decision=decision,
            conflict_reason=reason,
            actual_departure=observation.actual_departure,
            quality_issues=tuple(issues),
            raw_hash=observation.raw_hash,
        )
        segments.append(segment)
        adopted_head = segment

    def retain_conflict(
        observation: Observation,
        axis: str,
        decision: str,
        reason: str,
        competing: StateSegment | Observation | None,
    ) -> None:
        nonlocal sequence
        sequence += 1
        segments.append(
            StateSegment(
                sequence=sequence,
                status=observation.status,
                event_family=observation.event_family,
                record_key=observation.record_key,
                source=observation.source,
                observed_at=observation.observed_at,
                # Conflicting segments never own an effective interval.
                effective_from=observation.observed_at,
                effective_to=observation.observed_at,
                adopted=False,
                decision=decision,
                conflict_reason=reason,
                actual_departure=observation.actual_departure,
                quality_issues=(),
                raw_hash=observation.raw_hash,
            )
        )
        conflicts.append(
            Conflict(
                record_key=observation.record_key,
                source=observation.source,
                observed_at=observation.observed_at,
                status=observation.status,
                event_family=observation.event_family,
                conflict_axis=axis,
                decision=decision,
                reason=reason,
                competing_record_key=getattr(competing, "record_key", None),
                competing_source=getattr(competing, "source", None),
                raw_hash=observation.raw_hash,
            )
        )

    def record_conflict(
        observation: Observation,
        axis: str,
        decision: str,
        reason: str,
        competing: StateSegment | Observation | None,
    ) -> None:
        conflicts.append(
            Conflict(
                record_key=observation.record_key,
                source=observation.source,
                observed_at=observation.observed_at,
                status=observation.status,
                event_family=observation.event_family,
                conflict_axis=axis,
                decision=decision,
                reason=reason,
                competing_record_key=getattr(competing, "record_key", None),
                competing_source=getattr(competing, "source", None),
                raw_hash=observation.raw_hash,
            )
        )

    index = 0
    while index < len(observations):
        observed_at = observations[index].observed_at
        group = [observations[index]]
        index += 1
        while index < len(observations) and observations[index].observed_at == observed_at:
            group.append(observations[index])
            index += 1

        # ---- axis 1: same-timestamp, possibly different-source observations
        winner = group[0]
        if len(group) > 1:
            winner = max(group, key=lambda item: _source_tie_key(config, item))
            for loser in group:
                if loser is winner:
                    continue
                reason = (
                    "same_timestamp: competing state "
                    f"{loser.status} from {loser.source} at {observed_at}"
                )
                if config.same_timestamp_policy == REJECT:
                    decision = "rejected_same_timestamp"
                elif config.same_timestamp_policy == KEEP_CONFLICT:
                    decision = "kept_conflict_same_timestamp"
                else:  # SOURCE_PRIORITY
                    decision = "source_priority_loser_same_timestamp"
                if config.same_timestamp_policy == KEEP_CONFLICT:
                    retain_conflict(loser, "same_timestamp", decision, reason, winner)
                else:
                    record_conflict(loser, "same_timestamp", decision, reason, winner)

        issues = _quality_issues(winner, flight_date)
        if issues:
            quality_warnings += len(issues)
            record_conflict(
                winner,
                "quality",
                "quality_warning",
                "; ".join(issues),
                None,
            )

        # ---- axis 2: transition conflicts against the adopted trajectory
        if adopted_head is None:
            adopt(winner, "adopted_first_observation", None, issues)
            continue

        violation: str | None = None
        if adopted_head.status == "cancelled":
            violation = (
                f"transition: state {winner.status} observed after cancellation "
                f"at {adopted_head.observed_at}"
            )
        elif adopted_head.status in TERMINAL_STATES:
            violation = (
                f"transition: state {winner.status} observed after terminal "
                f"{adopted_head.status} at {adopted_head.observed_at}"
            )
        elif STATUS_RANK.get(winner.status, -1) < STATUS_RANK.get(adopted_head.status, -1):
            violation = (
                f"transition: backwards state {winner.status} after "
                f"{adopted_head.status}"
            )

        if violation is None:
            if winner.status == adopted_head.status:
                decision = "adopted_revision"
            else:
                decision = "adopted_next_state"
            adopt(winner, decision, None, issues)
            continue

        policy = config.transition_policy
        if policy == REJECT:
            record_conflict(winner, "transition", "rejected_transition", violation, adopted_head)
        elif policy == KEEP_CONFLICT:
            retain_conflict(winner, "transition", "kept_conflict_transition", violation, adopted_head)
        else:  # SOURCE_PRIORITY
            winner_rank = config.rank_of(winner.source)
            head_rank = config.rank_of(adopted_head.source)
            if winner_rank > head_rank:
                # A strictly higher-priority source overrides the earlier
                # adoption; equal/lower ranks cannot rewrite history.
                close_open_segment(winner.observed_at)
                adopt(winner, "source_priority_override", violation, issues)
            else:
                record_conflict(
                    winner,
                    "transition",
                    "source_priority_loser_transition",
                    violation,
                    adopted_head,
                )

    return Trajectory(
        flight_id=flight_id,
        flight_date=flight_date,
        flight_key=flight_key,
        config_fingerprint=config.fingerprint(),
        input_fingerprint=input_fingerprint,
        segments=tuple(segments),
        conflicts=tuple(replay_conflicts + conflicts),
        observation_count=len(observations),
        quality_warning_count=quality_warnings,
    )

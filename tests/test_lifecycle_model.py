"""Pure-domain tests for lifecycle state-machine reconstruction."""

from __future__ import annotations

import pytest

from aviation_operations.lifecycle_model import (
    KEEP_CONFLICT,
    REJECT,
    SOURCE_PRIORITY,
    LifecycleConfig,
    LifecycleConfigError,
    ResourceLimitExceeded,
    rebuild,
)
from aviation_operations.models import FlightRecord

from .lifecycle_builders import (
    full_trajectory_rows,
    normalized,
    raw_observation,
    shuffled,
)

FLIGHT_ID = "CA100"
FLIGHT_DATE = "2026-11-01"
FLIGHT_KEY = "CA100|2026-11-01"


def adopted_statuses(trajectory) -> list[str]:
    return [segment.status for segment in trajectory.segments if segment.adopted]


def test_empty_trajectory_is_valid_and_deterministic():
    config = LifecycleConfig()
    first = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, [], config)
    second = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, [], config)
    assert first.segments == ()
    assert first.conflicts == ()
    assert first.current_state is None
    assert first.observation_count == 0
    assert first.input_fingerprint == second.input_fingerprint


def test_full_trajectory_orders_out_of_order_snapshots_by_observed_at():
    rows = shuffled(full_trajectory_rows())
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == [
        "scheduled",
        "boarding",
        "delayed",
        "departed",
        "arrived",
    ]
    # Effective intervals are contiguous and half-open.
    adopted = [segment for segment in trajectory.segments if segment.adopted]
    assert adopted[0].effective_from == "2026-11-01T06:00:00Z"
    for earlier, later in zip(adopted, adopted[1:]):
        assert earlier.effective_to == later.effective_from
        assert later.effective_to is None or later.effective_to > later.effective_from
    assert adopted[-1].effective_to is None
    assert trajectory.current_state == "arrived"


def test_every_segment_carries_record_key_source_and_event_family():
    from aviation_operations.lifecycle import event_family

    trajectory = rebuild(
        FLIGHT_ID,
        FLIGHT_DATE,
        FLIGHT_KEY,
        normalized(full_trajectory_rows()),
        LifecycleConfig(),
    )
    for segment in trajectory.segments:
        assert segment.record_key
        assert segment.source == "airport-a"
        assert segment.event_family == event_family(segment.status)
        assert segment.raw_hash


def test_same_timestamp_conflict_reject_policy():
    rows = [
        raw_observation(source="airport-b", observed_at="2026-11-01T06:00:00Z", status="boarding"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
    ]
    config = LifecycleConfig(same_timestamp_policy=REJECT)
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)
    # Deterministic tie break independent of input order: boarding outranks scheduled.
    assert adopted_statuses(trajectory) == ["boarding"]
    conflict = trajectory.conflicts[0]
    assert conflict.conflict_axis == "same_timestamp"
    assert conflict.decision == "rejected_same_timestamp"
    assert conflict.competing_source == "airport-b"
    assert all(segment.adopted for segment in trajectory.segments)


def test_same_timestamp_conflict_kept_as_visible_conflicting_segment():
    rows = [
        raw_observation(source="airport-b", observed_at="2026-11-01T06:00:00Z", status="boarding"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
    ]
    config = LifecycleConfig(same_timestamp_policy=KEEP_CONFLICT)
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)
    adopted = [segment for segment in trajectory.segments if segment.adopted]
    retained = [segment for segment in trajectory.segments if not segment.adopted]
    assert [segment.status for segment in adopted] == ["boarding"]
    assert len(retained) == 1
    assert retained[0].status == "scheduled"
    assert retained[0].decision == "kept_conflict_same_timestamp"
    # Conflicting segments never own an open effective interval.
    assert retained[0].effective_from == retained[0].effective_to


def test_same_timestamp_source_priority_overrides_tie_break():
    rows = [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
        raw_observation(source="airport-b", observed_at="2026-11-01T06:00:00Z", status="boarding"),
    ]
    config = LifecycleConfig(
        same_timestamp_policy=SOURCE_PRIORITY,
        source_priority={"airport-b": 5, "airport-a": 1},
    )
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)
    assert adopted_statuses(trajectory) == ["boarding"]
    assert trajectory.conflicts[0].decision == "source_priority_loser_same_timestamp"


def test_late_departed_and_arrived_are_adopted_in_observed_order():
    rows = [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T08:30:00Z",
            status="arrived",
            actual_departure="2026-11-01T06:48:00Z",
        ),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:50:00Z",
            status="departed",
            actual_departure="2026-11-01T06:48:00Z",
        ),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["scheduled", "departed", "arrived"]


def test_cancelled_then_later_state_is_rejected_by_default():
    rows = [
        raw_observation(source="airline-feed", observed_at="2026-11-01T06:40:00Z", status="cancelled"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T07:05:00Z",
            status="departed",
            actual_departure="2026-11-01T07:02:00Z",
        ),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["cancelled"]
    conflict = trajectory.conflicts[-1]
    assert conflict.conflict_axis == "transition"
    assert conflict.decision == "rejected_transition"
    assert "cancellation" in conflict.reason


def test_cancelled_then_arrival_can_be_retained_as_conflict():
    rows = [
        raw_observation(source="airline-feed", observed_at="2026-11-01T06:40:00Z", status="cancelled"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T09:40:00Z",
            status="arrived",
            actual_departure="2026-11-01T07:02:00Z",
        ),
    ]
    config = LifecycleConfig(transition_policy=KEEP_CONFLICT)
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)
    assert adopted_statuses(trajectory) == ["cancelled"]
    retained = [segment for segment in trajectory.segments if not segment.adopted]
    assert len(retained) == 1
    assert retained[0].status == "arrived"
    assert retained[0].decision == "kept_conflict_transition"


def test_source_priority_can_override_cancellation():
    rows = [
        raw_observation(source="airline-feed", observed_at="2026-11-01T06:40:00Z", status="cancelled"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T07:05:00Z",
            status="departed",
            actual_departure="2026-11-01T07:02:00Z",
        ),
    ]
    config = LifecycleConfig(
        transition_policy=SOURCE_PRIORITY,
        source_priority={"airport-a": 9, "airline-feed": 1},
    )
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)
    assert adopted_statuses(trajectory) == ["cancelled", "departed"]
    override = trajectory.segments[-1]
    assert override.decision == "source_priority_override"
    assert "cancellation" in (override.conflict_reason or "")


def test_lower_priority_source_cannot_override_cancellation():
    rows = [
        raw_observation(source="airline-feed", observed_at="2026-11-01T06:40:00Z", status="cancelled"),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T07:05:00Z",
            status="departed",
            actual_departure="2026-11-01T07:02:00Z",
        ),
    ]
    config = LifecycleConfig(
        transition_policy=SOURCE_PRIORITY,
        source_priority={"airport-a": 1, "airline-feed": 9},
    )
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)
    assert adopted_statuses(trajectory) == ["cancelled"]
    assert trajectory.conflicts[-1].decision == "source_priority_loser_transition"


def test_backwards_transition_after_terminal_is_conflict():
    rows = [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T08:30:00Z",
            status="arrived",
            actual_departure="2026-11-01T06:48:00Z",
        ),
        raw_observation(source="airport-a", observed_at="2026-11-01T09:00:00Z", status="boarding"),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["arrived"]
    conflict = trajectory.conflicts[-1]
    assert conflict.conflict_axis == "transition"
    assert "terminal" in conflict.reason


def test_duplicate_replay_is_detected_and_does_not_grow_trajectory():
    rows = full_trajectory_rows()
    duplicated = rows + rows  # exact replays
    trajectory = rebuild(
        FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(duplicated), LifecycleConfig()
    )
    assert adopted_statuses(trajectory) == ["scheduled", "boarding", "delayed", "departed", "arrived"]
    replay_conflicts = [c for c in trajectory.conflicts if c.decision == "rejected_duplicate_replay"]
    assert len(replay_conflicts) == 5
    assert trajectory.observation_count == 5  # deduped before the state machine


def test_revision_of_same_status_is_adopted_as_revision():
    rows = [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:50:00Z",
            status="departed",
            actual_departure="2026-11-01T06:48:00Z",
        ),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:55:00Z",
            status="departed",
            actual_departure="2026-11-01T06:49:00Z",
        ),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["departed", "departed"]
    assert trajectory.segments[-1].decision == "adopted_revision"
    # First revision's interval closes when the second is adopted.
    assert trajectory.segments[0].effective_to == "2026-11-01T06:55:00Z"
    assert trajectory.segments[1].effective_to is None


def test_missing_actual_departure_emits_quality_conflict_but_keeps_fact():
    rows = [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:50:00Z", status="departed"),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["departed"]
    quality = [c for c in trajectory.conflicts if c.conflict_axis == "quality"]
    assert quality and "actual_departure" in quality[0].reason
    assert trajectory.quality_warning_count == 1


def test_inconsistent_actual_departure_format_is_flagged():
    rows = [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:50:00Z",
            status="departed",
            actual_departure="06/11/2026 06:48",
        ),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    quality = [c for c in trajectory.conflicts if c.conflict_axis == "quality"]
    assert any("format is inconsistent" in c.reason for c in quality)


def test_cross_day_actual_departure_is_warned_but_flight_date_groups_together():
    rows = [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-02T23:40:00Z",
            status="departed",
            flight_date="2026-11-02",
            actual_departure="2026-11-03T00:05:00Z",
            scheduled_departure="2026-11-02T23:50:00Z",
        ),
        raw_observation(
            source="airport-a",
            observed_at="2026-11-03T01:30:00Z",
            status="arrived",
            flight_date="2026-11-02",
            actual_departure="2026-11-03T00:05:00Z",
            scheduled_departure="2026-11-02T23:50:00Z",
        ),
    ]
    trajectory = rebuild(
        FLIGHT_ID, "2026-11-02", "CA100|2026-11-02", normalized(rows), LifecycleConfig()
    )
    assert adopted_statuses(trajectory) == ["departed", "arrived"]
    cross_day = [
        c
        for c in trajectory.conflicts
        if c.conflict_axis == "quality" and "crosses flight_date" in c.reason
    ]
    assert len(cross_day) == 2


def test_naive_and_offset_timestamps_are_comparable():
    rows = [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:00:00Z", status="scheduled"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:20:00+00:00", status="boarding"),
        raw_observation(source="airport-a", observed_at="2026-11-01T14:25:00+08:00", status="delayed"),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["scheduled", "boarding", "delayed"]


def test_resource_limit_is_enforced_before_trajectory_build():
    rows = [
        raw_observation(source="airport-a", observed_at=f"2026-11-01T06:{minute:02d}:00Z", status="scheduled")
        for minute in range(3)
    ]
    config = LifecycleConfig(max_observations=2)
    with pytest.raises(ResourceLimitExceeded):
        rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), config)


def test_invalid_config_is_rejected():
    with pytest.raises(LifecycleConfigError):
        LifecycleConfig(same_timestamp_policy="last-write-wins")
    with pytest.raises(LifecycleConfigError):
        LifecycleConfig(transition_policy="override-all")
    with pytest.raises(LifecycleConfigError):
        LifecycleConfig(max_observations=0)
    with pytest.raises(LifecycleConfigError):
        LifecycleConfig(source_priority={"airport-a": 1.5})  # type: ignore[dict-item]


def test_parse_timestamp_handles_naive_unparseable_and_empty_values():
    from aviation_operations.lifecycle_model import parse_timestamp

    assert parse_timestamp("2026-11-01T06:00:00").tzinfo is not None  # naive -> UTC
    assert parse_timestamp("") is None
    assert parse_timestamp("not-a-timestamp") is None


def test_unparseable_observed_at_is_a_quality_conflict_not_a_crash():
    rows = [
        raw_observation(source="airport-a", observed_at="bad-time", status="scheduled"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:20:00Z", status="boarding"),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    quality = [c for c in trajectory.conflicts if c.conflict_axis == "quality"]
    assert any("unparseable observed_at" in c.reason for c in quality)
    # The malformed fact is still represented in the audit trail.
    assert any(conflict.observed_at == "bad-time" for conflict in trajectory.conflicts)


def test_non_movement_observation_with_unparseable_actual_departure_is_flagged():
    rows = [
        raw_observation(
            source="airport-a",
            observed_at="2026-11-01T06:20:00Z",
            status="boarding",
            actual_departure="nonsense",
        ),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    quality = [c for c in trajectory.conflicts if c.conflict_axis == "quality"]
    assert any("format is inconsistent" in c.reason for c in quality)


def test_backwards_movement_before_terminal_state_is_a_conflict():
    rows = [
        raw_observation(source="airport-a", observed_at="2026-11-01T06:50:00Z", status="departed",
                        actual_departure="2026-11-01T06:48:00Z"),
        raw_observation(source="airport-a", observed_at="2026-11-01T06:55:00Z", status="boarding"),
    ]
    trajectory = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, normalized(rows), LifecycleConfig())
    assert adopted_statuses(trajectory) == ["departed"]
    conflict = trajectory.conflicts[-1]
    assert conflict.conflict_axis == "transition"
    assert "backwards state" in conflict.reason


def test_configuration_change_changes_fingerprint_and_is_auditable():
    records = normalized(full_trajectory_rows())
    base = LifecycleConfig()
    changed = LifecycleConfig(transition_policy=KEEP_CONFLICT)
    assert base.fingerprint() != changed.fingerprint()
    first = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, records, base)
    second = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, records, changed)
    assert first.config_fingerprint != second.config_fingerprint
    assert first.input_fingerprint == second.input_fingerprint


def test_rebuild_is_a_pure_function_of_input_and_config():
    records = normalized(shuffled(full_trajectory_rows()))
    config = LifecycleConfig(source_priority={"airport-a": 3})
    first = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, records, config)
    second = rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, list(reversed(records)), config)
    assert first.to_dict() == second.to_dict()


def test_flight_records_are_used_without_mutation():
    records = normalized(full_trajectory_rows())
    frozen: FlightRecord = records[0]
    rebuild(FLIGHT_ID, FLIGHT_DATE, FLIGHT_KEY, records, LifecycleConfig())
    assert frozen == records[0]

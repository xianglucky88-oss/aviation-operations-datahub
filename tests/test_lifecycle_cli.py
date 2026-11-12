"""End-to-end tests through the CLI, including legacy invocation compatibility."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from aviation_operations import cli

PROJECT_ROOT = Path(__file__).parents[1]
FIXTURE = PROJECT_ROOT / "fixtures" / "lifecycle_synthetic.json"


def run_cli(tmp_path: Path, *arguments: str) -> dict:
    env = {"PYTHONPATH": str(PROJECT_ROOT / "src"), "PATH": os.environ.get("PATH", "")}
    completed = subprocess.run(
        [sys.executable, "-m", "aviation_operations.cli", *arguments],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout)


def run_inprocess(monkeypatch, capsys, *arguments: str) -> dict:
    monkeypatch.setattr(sys, "argv", ["aviation-operations", *arguments])
    cli.main()
    return json.loads(capsys.readouterr().out)


def test_parser_rejects_unknown_policy():
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["rebuild", "--database", "x.sqlite3", "--flight-id", "CA1", "--flight-date", "2026-11-01",
             "--same-timestamp-policy", "last-write-wins"]
        )


def test_inprocess_rebuild_query_export_and_bad_priority(monkeypatch, capsys, tmp_path):
    database = tmp_path / "data.sqlite3"
    ingest = run_inprocess(
        monkeypatch, capsys,
        "ingest", "--fixture", str(FIXTURE), "--database", str(database),
    )
    assert ingest["accepted"] == 8

    rebuild = run_inprocess(
        monkeypatch, capsys,
        "rebuild", "--database", str(database),
        "--flight-id", "MU300", "--flight-date", "2026-11-02",
        "--source-priority", "airport-a=5",
    )
    assert rebuild["published"] is True

    query = run_inprocess(
        monkeypatch, capsys,
        "query", "--database", str(database),
        "--flight-id", "MU300", "--flight-date", "2026-11-02",
    )
    assert query["current_state"] == "arrived"

    exported = run_inprocess(
        monkeypatch, capsys,
        "export", "--database", str(database),
        "--flight-id", "MU300", "--flight-date", "2026-11-02",
    )
    assert exported["provenance"]

    with pytest.raises(SystemExit):
        run_inprocess(
            monkeypatch, capsys,
            "rebuild", "--database", str(database),
            "--flight-id", "MU300", "--flight-date", "2026-11-02",
            "--source-priority", "airport-a",
        )


def test_legacy_flat_ingestion_still_works(tmp_path):
    database = tmp_path / "data.sqlite3"
    result = run_cli(
        tmp_path,
        "--fixture", str(FIXTURE),
        "--database", str(database),
        "--raw-dir", str(tmp_path / "raw"),
    )
    assert result["processed"] == 8
    assert result["accepted"] == 8
    assert result["indexed"] == 8


def test_rebuild_query_export_lifecycle_end_to_end(tmp_path):
    database = tmp_path / "data.sqlite3"
    run_cli(tmp_path, "ingest", "--fixture", str(FIXTURE), "--database", str(database))

    rebuild = run_cli(
        tmp_path,
        "rebuild",
        "--database", str(database),
        "--flight-id", "CA200",
        "--flight-date", "2026-11-02",
        "--transition-policy", "source_priority",
        "--source-priority", "airport-a=5",
        "--source-priority", "airport-b=3",
        "--source-priority", "airline-feed=2",
    )
    assert rebuild["published"] is True
    assert rebuild["observation_count"] == 6

    query = run_cli(
        tmp_path,
        "query",
        "--database", str(database),
        "--flight-id", "CA200",
        "--flight-date", "2026-11-02",
    )
    assert query["current_state"] == "arrived"
    # The cancellation was overridden by a higher-priority source but the
    # override remains an auditable conflict.
    assert any(
        segment["decision"] == "source_priority_override" for segment in query["segments"]
    )
    # Segments expose every required explainability field.
    for segment in query["segments"]:
        for field in (
            "record_key",
            "source",
            "event_family",
            "effective_from",
            "decision",
            "raw_hash",
        ):
            assert segment[field] is not None or field == "raw_hash"

    export = run_cli(
        tmp_path,
        "export",
        "--database", str(database),
        "--flight-id", "CA200",
        "--flight-date", "2026-11-02",
    )
    assert export["provenance"]
    referenced = {segment["record_key"] for segment in export["segments"]}
    referenced |= {conflict["record_key"] for conflict in export["conflicts"]}
    assert referenced <= set(export["provenance"])

    # Cross-date flight is grouped under its flight_date.
    rebuild_mu = run_cli(
        tmp_path,
        "rebuild",
        "--database", str(database),
        "--flight-id", "MU300",
        "--flight-date", "2026-11-02",
    )
    assert rebuild_mu["observation_count"] == 2
    export_mu = run_cli(
        tmp_path,
        "export",
        "--database", str(database),
        "--flight-id", "MU300",
        "--flight-date", "2026-11-02",
    )
    assert export_mu["current_state"] == "arrived"
    quality_reasons = [c["reason"] for c in export_mu["conflicts"] if c["conflict_axis"] == "quality"]
    assert any("crosses flight_date" in reason for reason in quality_reasons)


def test_repeated_rebuild_is_a_noop_version_wise(tmp_path):
    database = tmp_path / "data.sqlite3"
    run_cli(tmp_path, "ingest", "--fixture", str(FIXTURE), "--database", str(database))
    args = (
        "rebuild",
        "--database", str(database),
        "--flight-id", "CA200",
        "--flight-date", "2026-11-02",
    )
    first = run_cli(tmp_path, *args)
    second = run_cli(tmp_path, *args)
    assert first["published"] is True
    assert second["published"] is False
    assert second["version_id"] == first["version_id"]

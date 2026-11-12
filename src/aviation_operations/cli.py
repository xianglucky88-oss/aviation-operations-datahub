from __future__ import annotations

import argparse
import json
from typing import Any

from .application import (
    export_flight_lifecycle,
    load_flight_lifecycle,
    rebuild_flight_lifecycle,
    run_fixture,
    run_url,
)
from .lifecycle_model import POLICIES, LifecycleConfig


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _load_config(args: argparse.Namespace) -> LifecycleConfig:
    source_priority: dict[str, int] = {}
    if args.source_priority:
        for entry in args.source_priority:
            if "=" not in entry:
                raise SystemExit(f"--source-priority expects SOURCE=RANK, got {entry!r}")
            name, rank = entry.split("=", 1)
            try:
                source_priority[name.strip()] = int(rank)
            except ValueError as exc:
                raise SystemExit(f"source priority rank must be an integer: {entry!r}") from exc
    return LifecycleConfig(
        same_timestamp_policy=args.same_timestamp_policy,
        transition_policy=args.transition_policy,
        source_priority=source_priority,
        max_observations=args.max_observations,
    )


def _add_config_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--same-timestamp-policy",
        choices=sorted(POLICIES),
        default="source_priority",
        help="protocol for observations sharing an observed_at",
    )
    parser.add_argument(
        "--transition-policy",
        choices=sorted(POLICIES),
        default="reject",
        help="protocol for observations conflicting with the adopted trajectory",
    )
    parser.add_argument(
        "--source-priority",
        action="append",
        default=[],
        metavar="SOURCE=RANK",
        help="higher rank wins under the source_priority protocol; repeatable",
    )
    parser.add_argument("--max-observations", type=int, default=10_000)


def _add_legacy_ingestion(parser: argparse.ArgumentParser, *, required: bool) -> None:
    source = parser.add_mutually_exclusive_group(required=required)
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database", required=required)
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="航空航班与机场运行数据平台")
    subparsers = parser.add_subparsers(dest="command")

    ingest = subparsers.add_parser("ingest", help="ingest a fixture or HTTP source (default command)")
    _add_legacy_ingestion(ingest, required=True)

    rebuild = subparsers.add_parser("rebuild", help="rebuild one flight's lifecycle read model")
    rebuild.add_argument("--database", required=True)
    rebuild.add_argument("--flight-id", required=True)
    rebuild.add_argument("--flight-date", required=True)
    rebuild.add_argument("--force", action="store_true", help="publish even when input is unchanged")
    rebuild.add_argument("--lock-owner", default="lifecycle-worker")
    _add_config_arguments(rebuild)

    query = subparsers.add_parser("query", help="print one flight's reconstructed trajectory")
    query.add_argument("--database", required=True)
    query.add_argument("--flight-id", required=True)
    query.add_argument("--flight-date", required=True)
    query.add_argument("--version-id", type=int)

    export = subparsers.add_parser("export", help="export trajectory with full provenance as JSON")
    export.add_argument("--database", required=True)
    export.add_argument("--flight-id", required=True)
    export.add_argument("--flight-date", required=True)
    export.add_argument("--version-id", type=int)

    # Legacy flat form: --fixture/--url at top level, no subcommand.
    _add_legacy_ingestion(parser, required=False)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command is None:
        if args.fixture:
            result = run_fixture(args.fixture, args.database, args.raw_dir)
        else:
            result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
        _print_json(result)
        return
    if args.command == "ingest":
        if args.fixture:
            result = run_fixture(args.fixture, args.database, args.raw_dir)
        else:
            result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
        _print_json(result)
    elif args.command == "rebuild":
        result = rebuild_flight_lifecycle(
            args.database,
            args.flight_id,
            args.flight_date,
            _load_config(args),
            force=args.force,
            lock_owner=args.lock_owner,
        )
        _print_json(result)
    elif args.command == "query":
        _print_json(load_flight_lifecycle(args.database, args.flight_id, args.flight_date, args.version_id))
    elif args.command == "export":
        _print_json(export_flight_lifecycle(args.database, args.flight_id, args.flight_date, args.version_id))


if __name__ == "__main__":
    main()

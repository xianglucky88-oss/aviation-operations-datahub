from __future__ import annotations

import argparse
import json

from .application import run_fixture, run_url


def main() -> None:
    parser = argparse.ArgumentParser(description="Run 航空航班与机场运行数据平台 ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture")
    source.add_argument("--url")
    parser.add_argument("--database", required=True)
    parser.add_argument("--raw-dir")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--records-key")
    args = parser.parse_args()
    if args.fixture:
        result = run_fixture(args.fixture, args.database, args.raw_dir)
    else:
        result = run_url(args.url, args.database, args.raw_dir, args.timeout, args.records_key)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

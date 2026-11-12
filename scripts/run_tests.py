#!/usr/bin/env python3
"""Stdlib-only test runner.

The sandbox image has no pytest/pip available, so this runner discovers
unittest cases under ``tests/`` with isolated mode (``-I``) and an
explicit ``src`` path.  When pytest is installed, the same test modules
also work unchanged under ``python -m pytest``.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))


def main() -> int:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for path in sorted((ROOT / "tests").glob("test_*.py")):
        module_name = path.stem
        tests = loader.loadTestsFromName(module_name)
        suite.addTests(tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())

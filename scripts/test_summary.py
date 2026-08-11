#!/usr/bin/env python3
"""Run the complete suite and print its validation-category summary as JSON."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
MANIFEST = ROOT / "tests/validation_manifest.json"


def main() -> int:
    categories = json.loads(MANIFEST.read_text(encoding="utf-8"))
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=1).run(suite)
    summary = {
        "tests_run": result.testsRun,
        "passed": result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped),
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "required_categories": len(categories),
        "categories": categories,
        "successful": result.wasSuccessful(),
    }
    print(json.dumps(summary, indent=2))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())

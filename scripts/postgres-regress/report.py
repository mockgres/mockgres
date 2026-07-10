#!/usr/bin/env python3

import csv
import json
import sys
from collections import Counter
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: report.py STATUS_TSV SUMMARY_JSON POSTGRES_VERSION", file=sys.stderr)
        return 2

    status_path = Path(sys.argv[1])
    summary_path = Path(sys.argv[2])
    postgres_version = sys.argv[3]

    with status_path.open(newline="", encoding="utf-8") as status_file:
        tests = list(csv.DictReader(status_file, delimiter="\t"))

    counts = Counter(test["status"] for test in tests)
    summary = {
        "postgres_version": postgres_version,
        "total": len(tests),
        "counts": dict(sorted(counts.items())),
        "tests": tests,
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    ordered_statuses = ["PASS", "FAIL", "PSQL_FAIL", "TIMEOUT", "CRASHED"]
    rendered = "  ".join(f"{status}={counts[status]}" for status in ordered_statuses)
    print(f"{rendered}  TOTAL={len(tests)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

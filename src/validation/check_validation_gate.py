from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

READINESS_ORDER = {
    "publish-blocked": 1,
    "not committee-grade": 2,
    "screening-grade only": 3,
    "decision-support only": 4,
    "analytically acceptable": 5,
    "technically valid": 6,
}


def evaluate_gate(payload: Mapping[str, Any], thresholds: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate validation thresholds against a summary payload.

    Pure function: no IO, no process exit. Returns the counts read from the
    payload, the thresholds applied, and the list of human-readable violations.
    An empty ``violations`` list means the gate passes.
    """
    summary = payload.get("summary", {})
    status_counts = summary.get("status_counts", {})
    severity_counts = summary.get("severity_counts", {})

    warn_count = int(status_counts.get("WARN", 0))
    fail_count = int(status_counts.get("FAIL", 0))
    high_count = int(severity_counts.get("High", 0))
    critical_count = int(severity_counts.get("Critical", 0))
    readiness = payload.get("readiness", {}).get("tier", "publish-blocked")

    min_tier = thresholds["min_readiness_tier"]
    violations: list[str] = []
    if warn_count > thresholds["max_warn"]:
        violations.append(f"WARN count {warn_count} > allowed {thresholds['max_warn']}")
    if fail_count > thresholds["max_fail"]:
        violations.append(f"FAIL count {fail_count} > allowed {thresholds['max_fail']}")
    if high_count > thresholds["max_high_severity"]:
        violations.append(f"High severity count {high_count} > allowed {thresholds['max_high_severity']}")
    if critical_count > thresholds["max_critical_severity"]:
        violations.append(f"Critical severity count {critical_count} > allowed {thresholds['max_critical_severity']}")
    if READINESS_ORDER.get(readiness, 0) < READINESS_ORDER[min_tier]:
        violations.append(f"Readiness tier '{readiness}' is below required '{min_tier}'")

    return {
        "warn_count": warn_count,
        "fail_count": fail_count,
        "high_severity_count": high_count,
        "critical_severity_count": critical_count,
        "readiness_tier": readiness,
        "thresholds": dict(thresholds),
        "violations": violations,
    }


def parse_args() -> argparse.Namespace:  # pragma: no cover - CLI plumbing, exercised by `make gate` in CI
    parser = argparse.ArgumentParser(description="Enforce validation gate thresholds from summary JSON.")
    parser.add_argument("--summary-path", type=str, default="reports/formal_validation_summary.json")
    parser.add_argument("--max-warn", type=int, default=0)
    parser.add_argument("--max-fail", type=int, default=0)
    parser.add_argument("--max-high-severity", type=int, default=0)
    parser.add_argument("--max-critical-severity", type=int, default=0)
    parser.add_argument(
        "--min-readiness-tier",
        type=str,
        default="technically valid",
        choices=list(READINESS_ORDER.keys()),
        help="Minimum allowed governance readiness tier.",
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover - CLI entrypoint, exercised by `make gate` in CI
    args = parse_args()
    summary_path = Path(args.summary_path)
    if not summary_path.exists():
        raise FileNotFoundError(f"Validation summary not found: {summary_path}")

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    thresholds = {
        "max_warn": args.max_warn,
        "max_fail": args.max_fail,
        "max_high_severity": args.max_high_severity,
        "max_critical_severity": args.max_critical_severity,
        "min_readiness_tier": args.min_readiness_tier,
    }

    result = evaluate_gate(payload, thresholds)
    print(json.dumps(result, indent=2))

    if result["violations"]:
        sys.exit(1)


if __name__ == "__main__":
    main()

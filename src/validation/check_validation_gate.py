from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

READINESS_ORDER = {
    "publish-blocked": 1,
    "not committee-grade": 2,
    "screening-grade only": 3,
    "decision-support only": 4,
    "analytically acceptable": 5,
    "technically valid": 6,
}


def parse_args() -> argparse.Namespace:
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


def main() -> None:
    args = parse_args()
    summary_path = Path(args.summary_path)
    if not summary_path.exists():
        raise FileNotFoundError(f"Validation summary not found: {summary_path}")

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    status_counts = payload.get("summary", {}).get("status_counts", {})
    severity_counts = payload.get("summary", {}).get("severity_counts", {})

    warn_count = int(status_counts.get("WARN", 0))
    fail_count = int(status_counts.get("FAIL", 0))
    high_count = int(severity_counts.get("High", 0))
    critical_count = int(severity_counts.get("Critical", 0))
    readiness = payload.get("readiness", {}).get("tier", "publish-blocked")

    violations = []
    if warn_count > args.max_warn:
        violations.append(f"WARN count {warn_count} > allowed {args.max_warn}")
    if fail_count > args.max_fail:
        violations.append(f"FAIL count {fail_count} > allowed {args.max_fail}")
    if high_count > args.max_high_severity:
        violations.append(f"High severity count {high_count} > allowed {args.max_high_severity}")
    if critical_count > args.max_critical_severity:
        violations.append(f"Critical severity count {critical_count} > allowed {args.max_critical_severity}")
    if READINESS_ORDER.get(readiness, 0) < READINESS_ORDER[args.min_readiness_tier]:
        violations.append(f"Readiness tier '{readiness}' is below required '{args.min_readiness_tier}'")

    print(
        json.dumps(
            {
                "warn_count": warn_count,
                "fail_count": fail_count,
                "high_severity_count": high_count,
                "critical_severity_count": critical_count,
                "readiness_tier": readiness,
                "thresholds": {
                    "max_warn": args.max_warn,
                    "max_fail": args.max_fail,
                    "max_high_severity": args.max_high_severity,
                    "max_critical_severity": args.max_critical_severity,
                    "min_readiness_tier": args.min_readiness_tier,
                },
                "violations": violations,
            },
            indent=2,
        )
    )

    if violations:
        sys.exit(1)


if __name__ == "__main__":
    main()

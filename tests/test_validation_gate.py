"""Unit tests for src/validation/check_validation_gate.evaluate_gate.

The gate is the last line of defence before a release is declared publishable.
Each threshold branch is pinned here so a loosened comparison cannot pass silently.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.validation.check_validation_gate import evaluate_gate  # noqa: E402

STRICT_THRESHOLDS = {
    "max_warn": 0,
    "max_fail": 0,
    "max_high_severity": 0,
    "max_critical_severity": 0,
    "min_readiness_tier": "technically valid",
}


def _payload(
    *,
    warn: int = 0,
    fail: int = 0,
    high: int = 0,
    critical: int = 0,
    tier: str = "technically valid",
) -> dict:
    return {
        "summary": {
            "status_counts": {"WARN": warn, "FAIL": fail},
            "severity_counts": {"High": high, "Critical": critical},
        },
        "readiness": {"tier": tier},
    }


class TestEvaluateGate(unittest.TestCase):
    def test_clean_payload_has_no_violations(self) -> None:
        result = evaluate_gate(_payload(), STRICT_THRESHOLDS)
        self.assertEqual(result["violations"], [])
        self.assertEqual(result["readiness_tier"], "technically valid")

    def test_each_count_threshold_triggers_its_own_violation(self) -> None:
        cases = [
            ("warn", {"warn": 1}, "WARN count"),
            ("fail", {"fail": 1}, "FAIL count"),
            ("high", {"high": 1}, "High severity count"),
            ("critical", {"critical": 1}, "Critical severity count"),
        ]
        for name, kwargs, marker in cases:
            with self.subTest(name=name):
                result = evaluate_gate(_payload(**kwargs), STRICT_THRESHOLDS)
                self.assertEqual(len(result["violations"]), 1, result["violations"])
                self.assertIn(marker, result["violations"][0])

    def test_readiness_below_minimum_is_a_violation(self) -> None:
        result = evaluate_gate(_payload(tier="decision-support only"), STRICT_THRESHOLDS)
        self.assertTrue(any("Readiness tier" in v for v in result["violations"]))

    def test_readiness_above_minimum_passes(self) -> None:
        relaxed = {**STRICT_THRESHOLDS, "min_readiness_tier": "decision-support only"}
        result = evaluate_gate(_payload(tier="technically valid"), relaxed)
        self.assertEqual(result["violations"], [])

    def test_unknown_readiness_tier_is_treated_as_below_floor(self) -> None:
        result = evaluate_gate(_payload(tier="totally-unknown"), STRICT_THRESHOLDS)
        self.assertTrue(any("Readiness tier" in v for v in result["violations"]))

    def test_thresholds_allow_counts_at_the_limit(self) -> None:
        relaxed = {**STRICT_THRESHOLDS, "max_warn": 2}
        self.assertEqual(evaluate_gate(_payload(warn=2), relaxed)["violations"], [])
        self.assertEqual(len(evaluate_gate(_payload(warn=3), relaxed)["violations"]), 1)

    def test_multiple_breaches_accumulate(self) -> None:
        result = evaluate_gate(
            _payload(warn=1, fail=1, high=1, critical=1, tier="publish-blocked"),
            STRICT_THRESHOLDS,
        )
        self.assertEqual(len(result["violations"]), 5)

    def test_missing_sections_default_to_zero_and_blocked(self) -> None:
        # An empty payload must not crash; readiness defaults to the lowest tier.
        result = evaluate_gate({}, STRICT_THRESHOLDS)
        self.assertEqual(result["warn_count"], 0)
        self.assertEqual(result["readiness_tier"], "publish-blocked")
        self.assertTrue(any("Readiness tier" in v for v in result["violations"]))

    def test_result_echoes_applied_thresholds(self) -> None:
        result = evaluate_gate(_payload(), STRICT_THRESHOLDS)
        self.assertEqual(result["thresholds"], STRICT_THRESHOLDS)


if __name__ == "__main__":
    unittest.main()

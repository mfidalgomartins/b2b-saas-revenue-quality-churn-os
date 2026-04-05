from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUMMARY_PATH = ROOT / "reports" / "formal_validation_summary.json"


class TestValidationGovernance(unittest.TestCase):
    def test_summary_contains_governance_readiness_schema(self) -> None:
        self.assertTrue(SUMMARY_PATH.exists(), "formal_validation_summary.json is missing")
        payload = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))

        self.assertIn("readiness", payload)
        self.assertIn("readiness_scale", payload)

        readiness = payload["readiness"]
        scale = payload["readiness_scale"]

        self.assertIn("tier", readiness)
        self.assertIn("rationale", readiness)
        self.assertIn(readiness["tier"], scale)

        expected_scale = [
            "publish-blocked",
            "not committee-grade",
            "screening-grade only",
            "decision-support only",
            "analytically acceptable",
            "technically valid",
        ]
        self.assertEqual(scale, expected_scale)

    def test_gate_supports_min_readiness_tier(self) -> None:
        payload = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
        current_tier = payload.get("readiness", {}).get("tier", "publish-blocked")

        cmd = [
            sys.executable,
            "src/validation/check_validation_gate.py",
            "--summary-path",
            str(SUMMARY_PATH),
            "--max-warn",
            "100",
            "--max-fail",
            "100",
            "--max-high-severity",
            "100",
            "--max-critical-severity",
            "100",
            "--min-readiness-tier",
            current_tier,
        ]
        result = subprocess.run(cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(result.returncode, 0, f"Validation gate should pass when using current tier. stderr={result.stderr}")


if __name__ == "__main__":
    unittest.main()

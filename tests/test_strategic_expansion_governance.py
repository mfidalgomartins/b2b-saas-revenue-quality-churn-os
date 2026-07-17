"""Direct integration tests for strategic-expansion governance controls."""

from __future__ import annotations

import unittest
from pathlib import Path

from src.validation.expansion_checks import run_strategic_expansion_checks
from src.validation.run_full_project_validation import load_tables

ROOT = Path(__file__).resolve().parents[1]


class TestStrategicExpansionGovernance(unittest.TestCase):
    def test_all_expansion_controls_pass_on_current_release_artifacts(self) -> None:
        findings = []

        run_strategic_expansion_checks(ROOT, load_tables(ROOT), findings)

        observed = {finding.check_id: finding for finding in findings}
        self.assertEqual(set(observed), {"22", "23", "24"})
        for check_id, finding in observed.items():
            self.assertEqual(finding.status, "PASS", f"check {check_id}: {finding.details}")
            self.assertEqual(finding.severity, "None", f"check {check_id}: {finding.details}")


if __name__ == "__main__":
    unittest.main()

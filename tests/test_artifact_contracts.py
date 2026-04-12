from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
OUTPUTS = ROOT / "outputs"
SQL = ROOT / "sql"


class TestArtifactContracts(unittest.TestCase):
    def test_sql_semantic_layer_contract(self) -> None:
        required = [
            SQL / "README.md",
            SQL / "staging" / "stg_subscriptions.sql",
            SQL / "staging" / "stg_monthly_account_metrics.sql",
            SQL / "staging" / "stg_invoices.sql",
            SQL / "marts" / "mart_account_monthly_revenue_quality.sql",
            SQL / "marts" / "mart_retention_monthly.sql",
        ]
        missing = [str(p) for p in required if not p.exists()]
        self.assertEqual(missing, [], f"Missing SQL semantic-layer files: {missing}")

    def test_profiling_and_analysis_artifacts_exist_with_contract(self) -> None:
        profiling_memo_path = REPORTS / "data_profiling_memo.md"
        analysis_metrics_path = REPORTS / "main_business_analysis_metrics.json"
        analysis_memo_path = REPORTS / "main_business_analysis_memo.md"

        self.assertTrue(profiling_memo_path.exists(), "data_profiling_memo.md is missing")
        self.assertTrue(analysis_metrics_path.exists(), "main_business_analysis_metrics.json is missing")
        self.assertTrue(analysis_memo_path.exists(), "main_business_analysis_memo.md is missing")

        analysis_payload = json.loads(analysis_metrics_path.read_text(encoding="utf-8"))
        for key in ["meta", "section1", "section2", "section3", "section4", "section5", "section6"]:
            self.assertIn(key, analysis_payload, f"Missing analysis metrics section: {key}")
        self.assertIn("latest_grr", analysis_payload["section2"])
        self.assertIn("latest_nrr", analysis_payload["section2"])

    def test_dashboard_payload_contract(self) -> None:
        html_path = OUTPUTS / "dashboard" / "executive_dashboard.html"
        self.assertTrue(html_path.exists(), "Dashboard HTML is missing")

        html = html_path.read_text(encoding="utf-8")
        match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, flags=re.S)
        self.assertIsNotNone(match, "Embedded dashboard JSON payload is missing")

        payload = json.loads(match.group(1))  # type: ignore[arg-type]
        required_keys = {
            "meta",
            "official_kpis",
            "filters",
            "accounts",
            "manager_panel",
            "scenario_cards",
            "risk_impact",
            "chart_catalog",
            "methodology",
            "source_map",
        }
        self.assertTrue(required_keys.issubset(set(payload.keys())), f"Missing dashboard payload keys: {required_keys}")
        self.assertGreaterEqual(len(payload.get("chart_catalog", [])), 15, "Dashboard chart catalog is incomplete")
        self.assertGreater(len(payload.get("accounts", [])), 0, "Dashboard account payload is empty")
        self.assertNotEqual(payload.get("meta", {}).get("validation_readiness_tier", ""), "", "Dashboard readiness tier is missing")


if __name__ == "__main__":
    unittest.main()

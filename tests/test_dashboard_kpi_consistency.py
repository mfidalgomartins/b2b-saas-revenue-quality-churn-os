from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
OUTPUTS = ROOT / "outputs"


def load_dashboard_payload() -> dict:
    html_path = OUTPUTS / "dashboard" / "executive_dashboard.html"
    html = html_path.read_text(encoding="utf-8")
    match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, flags=re.S)
    if not match:
        raise AssertionError("Dashboard JSON payload not found")
    return json.loads(match.group(1))


class TestDashboardKpiConsistency(unittest.TestCase):
    def test_official_kpis_match_analysis_metrics(self) -> None:
        payload = load_dashboard_payload()
        analysis = json.loads((REPORTS / "main_business_analysis_metrics.json").read_text(encoding="utf-8"))

        kpis = payload.get("official_kpis", {})
        sec1 = analysis.get("section1", {})
        sec2 = analysis.get("section2", {})
        sec5 = analysis.get("section5", {})

        self.assertAlmostEqual(float(kpis.get("current_mrr", 0.0)), float(sec1.get("mrr_end", 0.0)), places=2)
        self.assertAlmostEqual(float(kpis.get("arr", 0.0)), float(sec1.get("arr_end", 0.0)), places=2)
        self.assertAlmostEqual(float(kpis.get("gross_retention", 0.0)), float(sec2.get("latest_grr", 0.0)), places=6)
        self.assertAlmostEqual(float(kpis.get("net_retention", 0.0)), float(sec2.get("latest_nrr", 0.0)), places=6)
        self.assertAlmostEqual(float(kpis.get("logo_churn", 0.0)), float(sec2.get("logo_churn_rate", 0.0)), places=6)
        self.assertAlmostEqual(float(kpis.get("avg_discount", 0.0)), float(sec1.get("w_discount_end", 0.0)), places=6)
        self.assertAlmostEqual(
            float(kpis.get("discounted_revenue_share", 0.0)),
            float(sec1.get("share_discounted_mrr_latest", 0.0)),
            places=6,
        )
        self.assertAlmostEqual(float(kpis.get("revenue_at_risk_mrr", 0.0)), float(sec5.get("at_risk_mrr_total", 0.0)), places=2)

    def test_dashboard_meta_readiness_matches_validation_summary(self) -> None:
        payload = load_dashboard_payload()
        validation = json.loads((REPORTS / "formal_validation_summary.json").read_text(encoding="utf-8"))

        dashboard_tier = payload.get("meta", {}).get("validation_readiness_tier", "")
        summary_tier = validation.get("readiness", {}).get("tier", "")

        self.assertNotEqual(dashboard_tier, "", "Dashboard readiness tier is missing")
        self.assertEqual(dashboard_tier, summary_tier)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import csv
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class TestProcessedContracts(unittest.TestCase):
    def test_required_processed_files_exist(self) -> None:
        required = [
            "account_monthly_revenue_quality.csv",
            "customer_health_features.csv",
            "cohort_retention_summary.csv",
            "account_risk_base.csv",
            "account_manager_summary.csv",
            "account_scoring_model_output.csv",
            "account_scoring_components.csv",
            "baseline_mrr_forecast.csv",
            "risk_adjusted_mrr_forecast.csv",
            "mrr_scenario_table.csv",
            "scenario_mrr_trajectories.csv",
        ]
        missing = [name for name in required if not (PROCESSED / name).exists()]
        self.assertEqual(missing, [], f"Missing processed files: {missing}")

    def test_account_level_tables_have_consistent_customer_universe(self) -> None:
        customers = read_rows(RAW / "customers.csv")
        customer_set = {r["customer_id"] for r in customers}

        account_level_tables = [
            "customer_health_features.csv",
            "account_risk_base.csv",
            "account_scoring_model_output.csv",
            "account_scoring_components.csv",
        ]
        for table in account_level_tables:
            rows = read_rows(PROCESSED / table)
            ids = [r["customer_id"] for r in rows]
            self.assertEqual(len(ids), len(set(ids)), f"Duplicate customer_id in {table}")
            self.assertEqual(set(ids), customer_set, f"Customer universe mismatch in {table}")

    def test_monthly_table_key_integrity(self) -> None:
        amrq = read_rows(PROCESSED / "account_monthly_revenue_quality.csv")
        monthly = read_rows(RAW / "monthly_account_metrics.csv")

        amrq_keys = [f'{r["customer_id"]}|{r["month"][:10]}' for r in amrq]
        mm_keys = [f'{r["customer_id"]}|{r["month"][:10]}' for r in monthly]
        self.assertEqual(len(amrq_keys), len(set(amrq_keys)), "Duplicate account_monthly_revenue_quality keys")
        self.assertEqual(set(amrq_keys), set(mm_keys), "Monthly key universe mismatch between processed and raw")

    def test_score_ranges_and_tiers(self) -> None:
        rows = read_rows(PROCESSED / "account_scoring_model_output.csv")
        score_cols = [
            "churn_risk_score",
            "revenue_quality_score",
            "discount_dependency_score",
            "expansion_quality_score",
            "governance_priority_score",
        ]
        tier_cols = [
            "churn_risk_tier",
            "revenue_quality_risk_tier",
            "discount_dependency_tier",
            "expansion_quality_risk_tier",
            "governance_priority_tier",
        ]
        allowed_tiers = {"Low", "Moderate", "High", "Critical"}

        bad_scores = []
        bad_tiers = []
        for row in rows:
            for col in score_cols:
                value = float(row[col])
                if value < 0 or value > 100:
                    bad_scores.append((row["customer_id"], col, value))
            for col in tier_cols:
                if row[col] not in allowed_tiers:
                    bad_tiers.append((row["customer_id"], col, row[col]))

        self.assertEqual(bad_scores, [], f"Out-of-range scores: sample={bad_scores[:5]}")
        self.assertEqual(bad_tiers, [], f"Unknown tiers: sample={bad_tiers[:5]}")

    def test_scenario_trajectory_shape(self) -> None:
        scenario_table = read_rows(PROCESSED / "mrr_scenario_table.csv")
        trajectories = read_rows(PROCESSED / "scenario_mrr_trajectories.csv")

        scenarios = {r["scenario"] for r in scenario_table}
        horizon_values = {int(float(r["horizon_months"])) for r in scenario_table}
        self.assertEqual(len(horizon_values), 1, "Multiple horizons found in mrr_scenario_table")
        horizon = next(iter(horizon_values))

        expected_rows = len(scenarios) * horizon
        self.assertEqual(
            len(trajectories),
            expected_rows,
            f"Scenario trajectory row mismatch (expected={expected_rows}, actual={len(trajectories)})",
        )

        trajectory_scenarios = {r["scenario"] for r in trajectories}
        self.assertEqual(trajectory_scenarios, scenarios, "Scenario set mismatch between summary and trajectory tables")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.scoring.backtest_scoring_calibration import build_trailing_panel
from src.scoring.scoring_utils import CHURN_WEIGHTS, compute_churn_components, score_from_components

ROOT = Path(__file__).resolve().parents[1]


class TestScoringTemporalIntegrity(unittest.TestCase):
    def test_current_month_churn_is_not_in_history_feature(self) -> None:
        monthly = pd.read_csv(ROOT / "data/raw/monthly_account_metrics.csv", parse_dates=["month"])
        health = pd.read_csv(ROOT / "data/processed/customer_health_features.csv")
        latest = monthly[monthly["month"] == monthly["month"].max()][["customer_id", "churn_flag"]]
        probe = health[["customer_id", "churn_history_flag"]].merge(
            latest,
            on="customer_id",
            how="left",
            validate="one_to_one",
        )

        leaked = probe[(probe["churn_flag"] == 1) & (probe["churn_history_flag"] == 1)]
        self.assertEqual(len(leaked), 0, "Current-month churn leaked into churn_history_flag")

    def test_latest_active_backtest_scores_match_production(self) -> None:
        panel = build_trailing_panel(ROOT)
        latest_month = panel["month"].max()
        latest_active = panel[(panel["month"] == latest_month) & (panel["active_flag"] == 1)].copy()
        latest_active["backtest_score"] = score_from_components(
            compute_churn_components(latest_active),
            CHURN_WEIGHTS,
        )

        production = pd.read_csv(ROOT / "data/processed/account_scoring_model_output.csv")
        comparison = latest_active[["customer_id", "backtest_score"]].merge(
            production[["customer_id", "churn_risk_score"]],
            on="customer_id",
            how="inner",
            validate="one_to_one",
        )
        max_delta = float((comparison["backtest_score"] - comparison["churn_risk_score"]).abs().max())
        self.assertLessEqual(max_delta, 0.01, f"Backtest/production score drift detected: max_delta={max_delta}")


if __name__ == "__main__":
    unittest.main()

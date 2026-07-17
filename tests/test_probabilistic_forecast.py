"""Tests for probabilistic paths and leakage-safe rolling-origin backtests."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from src.forecasting.probabilistic import (
    ProbabilisticForecastConfig,
    build_forward_forecast,
    rolling_origin_backtest,
    simulate_mrr_paths,
    summarize_backtest,
    summarize_paths,
)


def _company_history() -> pd.DataFrame:
    months = pd.date_range("2023-01-01", periods=24, freq="MS")
    growth = np.linspace(0.04, 0.01, len(months) - 1) + 0.003 * np.sin(np.arange(len(months) - 1))
    mrr = np.r_[1_000_000.0, 1_000_000.0 * np.cumprod(1 + growth)]
    return pd.DataFrame({"month": months, "mrr": mrr, "mrr_growth_rate": np.r_[np.nan, growth]})


class TestProbabilisticPaths(unittest.TestCase):
    def test_path_simulation_is_deterministic_and_quantiles_are_ordered(self) -> None:
        history = np.array([0.01, 0.03, 0.02, -0.01, 0.025, 0.015])
        first = simulate_mrr_paths(100.0, history, 6, 500, 2, np.random.default_rng(9))
        second = simulate_mrr_paths(100.0, history, 6, 500, 2, np.random.default_rng(9))
        np.testing.assert_allclose(first, second)

        quantiles = summarize_paths(first)
        self.assertTrue(np.all(quantiles["p05"] <= quantiles["p10"]))
        self.assertTrue(np.all(quantiles["p10"] <= quantiles["p50"]))
        self.assertTrue(np.all(quantiles["p50"] <= quantiles["p90"]))
        self.assertTrue(np.all(quantiles["p90"] <= quantiles["p95"]))

    def test_forward_forecast_has_declared_horizon_and_metadata(self) -> None:
        config = ProbabilisticForecastConfig(horizon_months=5, simulations=200, lookback_months=12)
        forecast = build_forward_forecast(_company_history(), config)
        self.assertEqual(len(forecast), 5)
        self.assertEqual(forecast["horizon_month"].tolist(), [1, 2, 3, 4, 5])
        self.assertTrue(forecast["p80_interval_width"].ge(0).all())
        self.assertTrue(forecast["method"].eq("local_trend_residual_block_bootstrap").all())


class TestRollingOriginBacktest(unittest.TestCase):
    def test_predictions_at_origin_do_not_change_when_future_changes(self) -> None:
        history = _company_history()
        config = ProbabilisticForecastConfig(
            simulations=200,
            lookback_months=9,
            min_train_months=6,
            backtest_horizon_months=3,
            seed=11,
        )
        first = rolling_origin_backtest(history, config)
        origin = pd.Timestamp("2024-03-01")

        changed = history.copy()
        future_mask = changed["month"].gt(origin)
        changed.loc[future_mask, "mrr"] *= 0.5
        changed.loc[future_mask, "mrr_growth_rate"] = -0.20
        second = rolling_origin_backtest(changed, config)

        prediction_columns = ["horizon_month", "p05", "p10", "p50", "p90", "p95"]
        first_origin = first.loc[first["origin_month"].eq(origin), prediction_columns].reset_index(drop=True)
        second_origin = second.loc[second["origin_month"].eq(origin), prediction_columns].reset_index(drop=True)
        pd.testing.assert_frame_equal(first_origin, second_origin)

    def test_backtest_summary_reports_valid_coverage_and_error_metrics(self) -> None:
        config = ProbabilisticForecastConfig(
            simulations=200,
            lookback_months=9,
            min_train_months=6,
            backtest_horizon_months=4,
        )
        summary = summarize_backtest(rolling_origin_backtest(_company_history(), config))
        overall = summary.loc[summary["scope"].eq("All")].iloc[0]
        self.assertGreater(int(overall["n_forecasts"]), 0)
        self.assertGreaterEqual(float(overall["mae"]), 0)
        self.assertTrue(0 <= float(overall["p80_coverage"]) <= 1)
        self.assertTrue(0 <= float(overall["p90_coverage"]) <= 1)
        self.assertGreaterEqual(float(overall["mean_p80_width"]), 0)


if __name__ == "__main__":
    unittest.main()

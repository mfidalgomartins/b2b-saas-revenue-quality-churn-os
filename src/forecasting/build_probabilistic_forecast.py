"""Build empirical forecast intervals and rolling-origin calibration evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.forecasting.build_forecasting_scenarios import build_company_monthly_frame
from src.forecasting.probabilistic import (
    ProbabilisticForecastConfig,
    build_forward_forecast,
    rolling_origin_backtest,
    summarize_backtest,
)
from src.io.logging_setup import get_logger

log = get_logger(__name__)


def _build_memo(summary: pd.DataFrame, metadata: dict[str, Any]) -> str:
    overall = summary.loc[summary["scope"].eq("All")].iloc[0]
    return f"""# Probabilistic MRR forecast validation

## Forecast contract

- Method: local linear trend with moving-block residual bootstrap. Each path refits the trend on a bootstrapped training history, capturing trend-estimation and process uncertainty while preserving short-run residual dependence.
- Information set: only growth observations available at each forecast origin; latest {metadata["lookback_months"]} observations at most.
- Production horizon: {metadata["forecast_horizon_months"]} months; backtest horizon: {metadata["backtest_horizon_months"]} months.
- Simulation: {metadata["simulations"]:,} paths per origin, block length {metadata["block_length"]} months, deterministic seed {metadata["seed"]}.
- Intervals: P10–P90 is the central 80% range; P05–P95 is the central 90% range.

## Rolling-origin evidence

- Forecast-origin/horizon observations: {int(overall["n_forecasts"]):,}.
- Median-path MAE: ${overall["mae"]:,.0f}; MAPE: {overall["mape"]:.2%}; signed bias: ${overall["bias"]:,.0f}.
- Central 80% empirical coverage: {overall["p80_coverage"]:.1%}; central 90% coverage: {overall["p90_coverage"]:.1%}.
- Mean central-80% interval width: ${overall["mean_p80_width"]:,.0f} ({overall["mean_p80_relative_width"]:.1%} of actual MRR).

## Use boundary

These are empirical operating ranges, not guarantees. Thirty-six monthly observations limit tail estimation and regime-shift detection. Scenario forecasts remain the decision tool for explicit policy or commercial assumptions; probabilistic intervals quantify historical process uncertainty around the current trajectory. Coverage is reported rather than tuned on the final holdout.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--horizon-months", type=int, default=12)
    parser.add_argument("--backtest-horizon-months", type=int, default=6)
    parser.add_argument("--simulations", type=int, default=2000)
    parser.add_argument("--lookback-months", type=int, default=18)
    parser.add_argument("--block-length", type=int, default=3)
    parser.add_argument("--min-train-months", type=int, default=12)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    processed_dir = base_dir / "data/processed"
    reports_dir = base_dir / "reports"
    monthly_quality = pd.read_csv(processed_dir / "account_monthly_revenue_quality.csv", parse_dates=["month"])
    monthly_raw = pd.read_csv(base_dir / "data/raw/monthly_account_metrics.csv", parse_dates=["month"])
    company_monthly = build_company_monthly_frame(monthly_quality, monthly_raw)
    company_monthly["mrr_growth_rate"] = company_monthly["mrr"].pct_change()
    config = ProbabilisticForecastConfig(
        horizon_months=args.horizon_months,
        simulations=args.simulations,
        lookback_months=args.lookback_months,
        block_length=args.block_length,
        min_train_months=args.min_train_months,
        backtest_horizon_months=args.backtest_horizon_months,
        seed=args.seed,
    )
    forecast = build_forward_forecast(company_monthly, config)
    backtest = rolling_origin_backtest(company_monthly, config)
    summary = summarize_backtest(backtest)
    overall = summary.loc[summary["scope"].eq("All")].iloc[0]
    metadata: dict[str, Any] = {
        "method": "local_trend_residual_block_bootstrap",
        "forecast_horizon_months": config.horizon_months,
        "backtest_horizon_months": config.backtest_horizon_months,
        "simulations": config.simulations,
        "lookback_months": config.lookback_months,
        "block_length": config.block_length,
        "min_train_months": config.min_train_months,
        "seed": config.seed,
        "backtest_origins": int(backtest["origin_month"].nunique()),
        "backtest_observations": len(backtest),
        "overall": {
            key: (int(value) if key == "n_forecasts" else float(value))
            for key, value in overall.items()
            if key not in {"scope", "horizon_month"}
        },
    }

    processed_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    forecast.to_csv(processed_dir / "probabilistic_mrr_forecast.csv", index=False)
    backtest.to_csv(processed_dir / "probabilistic_forecast_backtest.csv", index=False)
    summary.to_csv(processed_dir / "probabilistic_forecast_backtest_summary.csv", index=False)
    (reports_dir / "probabilistic_forecast_validation.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (reports_dir / "probabilistic_forecast_validation.md").write_text(_build_memo(summary, metadata), encoding="utf-8")
    log.info("Probabilistic forecast built: %s months", config.horizon_months)
    log.info("Rolling-origin evidence: %s observations", f"{len(backtest):,}")


if __name__ == "__main__":
    main()

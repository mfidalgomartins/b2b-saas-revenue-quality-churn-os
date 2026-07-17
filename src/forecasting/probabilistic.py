"""Empirical block-bootstrap MRR forecasts and rolling-origin evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ProbabilisticForecastConfig:
    horizon_months: int = 12
    simulations: int = 2000
    lookback_months: int = 18
    block_length: int = 3
    min_train_months: int = 12
    backtest_horizon_months: int = 6
    seed: int = 42


def validate_config(config: ProbabilisticForecastConfig) -> None:
    if config.horizon_months < 1 or config.backtest_horizon_months < 1:
        raise ValueError("Forecast horizons must be positive")
    if config.simulations < 100:
        raise ValueError("simulations must be at least 100")
    if config.lookback_months < 3 or config.min_train_months < 3:
        raise ValueError("lookback_months and min_train_months must be at least 3")
    if config.block_length < 1:
        raise ValueError("block_length must be positive")


def simulate_mrr_paths(
    start_mrr: float,
    growth_history: np.ndarray,
    horizon_months: int,
    simulations: int,
    block_length: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Simulate local-trend paths with moving-block residual resampling.

    Each simulation bootstraps the training residuals, refits the local trend,
    and draws future residual blocks. This captures both process noise and trend
    estimation uncertainty.
    """
    history = np.asarray(growth_history, dtype=float)
    history = history[np.isfinite(history)]
    if start_mrr <= 0:
        raise ValueError("start_mrr must be positive")
    if horizon_months < 1 or simulations < 1:
        raise ValueError("horizon_months and simulations must be positive")
    if len(history) < 3:
        raise ValueError("At least three finite growth observations are required")

    safe_history = np.clip(history, -0.25, 0.25)
    training_months = len(safe_history)
    x = np.arange(training_months, dtype=float)
    x_centered = x - x.mean()
    denominator = float(np.square(x_centered).sum())
    slope = float(np.dot(x_centered, safe_history) / denominator)
    intercept = float(safe_history.mean() - slope * x.mean())
    fitted = intercept + slope * x
    residuals = safe_history - fitted

    effective_block = min(block_length, training_months)
    total_draws = training_months + horizon_months
    block_count = int(np.ceil(total_draws / effective_block))
    max_start = training_months - effective_block + 1
    starts = rng.integers(0, max_start, size=(simulations, block_count))
    offsets = np.arange(effective_block)
    indexes = starts[:, :, np.newaxis] + offsets
    sampled_residuals = residuals[indexes].reshape(simulations, -1)[:, :total_draws]

    bootstrapped_training = fitted + sampled_residuals[:, :training_months]
    bootstrap_slopes = (bootstrapped_training @ x_centered) / denominator
    bootstrap_intercepts = bootstrapped_training.mean(axis=1) - bootstrap_slopes * x.mean()
    future_x = training_months + np.arange(horizon_months, dtype=float)
    future_trend = bootstrap_intercepts[:, np.newaxis] + bootstrap_slopes[:, np.newaxis] * future_x
    future_growth = np.clip(future_trend + sampled_residuals[:, training_months:], -0.25, 0.25)
    return start_mrr * np.cumprod(1.0 + future_growth, axis=1)


def summarize_paths(paths: np.ndarray) -> dict[str, np.ndarray]:
    """Return ordered predictive quantiles for every horizon."""
    if paths.ndim != 2 or paths.shape[0] == 0 or paths.shape[1] == 0:
        raise ValueError("paths must be a non-empty two-dimensional array")
    quantiles = np.quantile(paths, [0.05, 0.10, 0.50, 0.90, 0.95], axis=0)
    return {name: quantiles[index] for index, name in enumerate(("p05", "p10", "p50", "p90", "p95"))}


def build_forward_forecast(
    company_monthly: pd.DataFrame,
    config: ProbabilisticForecastConfig = ProbabilisticForecastConfig(),
) -> pd.DataFrame:
    """Build the latest-origin probabilistic forecast."""
    validate_config(config)
    required = {"month", "mrr", "mrr_growth_rate"}
    if missing := required - set(company_monthly):
        raise ValueError(f"company_monthly missing fields: {sorted(missing)}")
    history = company_monthly.sort_values("month").copy()
    growth = history["mrr_growth_rate"].dropna().to_numpy(dtype=float)[-config.lookback_months :]
    start_mrr = float(history.iloc[-1]["mrr"])
    paths = simulate_mrr_paths(
        start_mrr,
        growth,
        config.horizon_months,
        config.simulations,
        config.block_length,
        np.random.default_rng(config.seed),
    )
    quantiles = summarize_paths(paths)
    latest_month = pd.Timestamp(history.iloc[-1]["month"])
    forecast_months = pd.date_range(latest_month + pd.DateOffset(months=1), periods=config.horizon_months, freq="MS")
    output = pd.DataFrame(
        {
            "forecast_month": forecast_months,
            "horizon_month": np.arange(1, config.horizon_months + 1),
            "start_mrr": start_mrr,
            **quantiles,
        }
    )
    output["p80_interval_width"] = output["p90"] - output["p10"]
    output["p90_interval_width"] = output["p95"] - output["p05"]
    output["training_months"] = int(history["mrr_growth_rate"].notna().sum())
    output["lookback_months"] = len(growth)
    output["simulation_count"] = config.simulations
    output["block_length"] = min(config.block_length, len(growth))
    output["method"] = "local_trend_residual_block_bootstrap"
    return output


def rolling_origin_backtest(
    company_monthly: pd.DataFrame,
    config: ProbabilisticForecastConfig = ProbabilisticForecastConfig(),
) -> pd.DataFrame:
    """Evaluate forecasts using only observations available at each origin."""
    validate_config(config)
    required = {"month", "mrr", "mrr_growth_rate"}
    if missing := required - set(company_monthly):
        raise ValueError(f"company_monthly missing fields: {sorted(missing)}")
    history = company_monthly.sort_values("month").reset_index(drop=True).copy()
    rows: list[dict[str, Any]] = []
    for origin_index in range(len(history) - 1):
        training_growth = history.loc[:origin_index, "mrr_growth_rate"].dropna().to_numpy(dtype=float)
        if len(training_growth) < config.min_train_months:
            continue
        training_growth = training_growth[-config.lookback_months :]
        available_horizon = min(config.backtest_horizon_months, len(history) - origin_index - 1)
        seed_sequence = np.random.SeedSequence([config.seed, origin_index])
        paths = simulate_mrr_paths(
            float(history.loc[origin_index, "mrr"]),
            training_growth,
            available_horizon,
            config.simulations,
            config.block_length,
            np.random.default_rng(seed_sequence),
        )
        quantiles = summarize_paths(paths)
        for horizon_index in range(available_horizon):
            actual_mrr = float(history.loc[origin_index + horizon_index + 1, "mrr"])
            point_forecast = float(quantiles["p50"][horizon_index])
            absolute_error = abs(point_forecast - actual_mrr)
            rows.append(
                {
                    "origin_month": pd.Timestamp(history.loc[origin_index, "month"]),
                    "target_month": pd.Timestamp(history.loc[origin_index + horizon_index + 1, "month"]),
                    "horizon_month": horizon_index + 1,
                    "training_months": len(training_growth),
                    "actual_mrr": actual_mrr,
                    "p05": float(quantiles["p05"][horizon_index]),
                    "p10": float(quantiles["p10"][horizon_index]),
                    "p50": point_forecast,
                    "p90": float(quantiles["p90"][horizon_index]),
                    "p95": float(quantiles["p95"][horizon_index]),
                    "error": point_forecast - actual_mrr,
                    "absolute_error": absolute_error,
                    "absolute_percentage_error": absolute_error / actual_mrr if actual_mrr > 0 else np.nan,
                    "covered_80": int(quantiles["p10"][horizon_index] <= actual_mrr <= quantiles["p90"][horizon_index]),
                    "covered_90": int(quantiles["p05"][horizon_index] <= actual_mrr <= quantiles["p95"][horizon_index]),
                }
            )
    if not rows:
        raise ValueError("Insufficient history for rolling-origin backtesting")
    return pd.DataFrame(rows)


def summarize_backtest(backtest: pd.DataFrame) -> pd.DataFrame:
    """Aggregate rolling-origin accuracy and interval calibration by horizon."""
    required = {
        "horizon_month",
        "error",
        "absolute_error",
        "absolute_percentage_error",
        "covered_80",
        "covered_90",
        "p10",
        "p90",
        "actual_mrr",
    }
    if missing := required - set(backtest):
        raise ValueError(f"backtest missing fields: {sorted(missing)}")
    rows: list[dict[str, float | int | str]] = []
    scopes: list[tuple[str, pd.DataFrame]] = [("All", backtest)]
    scopes.extend((str(horizon), group) for horizon, group in backtest.groupby("horizon_month", sort=True))
    for scope, group in scopes:
        rows.append(
            {
                "scope": scope,
                "horizon_month": 0 if scope == "All" else int(scope),
                "n_forecasts": len(group),
                "mae": float(group["absolute_error"].mean()),
                "mape": float(group["absolute_percentage_error"].mean()),
                "bias": float(group["error"].mean()),
                "p80_coverage": float(group["covered_80"].mean()),
                "p90_coverage": float(group["covered_90"].mean()),
                "mean_p80_width": float((group["p90"] - group["p10"]).mean()),
                "mean_p80_relative_width": float(((group["p90"] - group["p10"]) / group["actual_mrr"]).mean()),
            }
        )
    return pd.DataFrame(rows)

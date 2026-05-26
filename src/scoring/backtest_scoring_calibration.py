"""Out-of-sample calibration backtest for the production churn-risk score.

Reconstructs the churn-risk score at every historical month using the SAME
component formulas and weights as `build_scoring_system.py`, then evaluates
whether higher-scored accounts churn at higher rates in the next 3 months.

The result is a like-for-like calibration check on the production model.
Any drift between the production weights and what is evaluated here will
fail the unit test in `tests/test_scoring_utils.py`.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.scoring.scoring_utils import (  # noqa: E402
    CHURN_WEIGHTS,
    compute_churn_components,
    risk_tier,
    score_from_components,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest churn-risk calibration against forward churn outcomes.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--horizon-months", type=int, default=3)
    parser.add_argument(
        "--tier-output-path",
        type=str,
        default="data/processed/scoring_backtest_calibration_by_tier.csv",
    )
    parser.add_argument(
        "--decile-output-path",
        type=str,
        default="data/processed/scoring_backtest_calibration_by_decile.csv",
    )
    parser.add_argument(
        "--summary-json-path",
        type=str,
        default="reports/scoring_backtest_summary.json",
    )
    return parser.parse_args()


def build_trailing_panel(base_dir: Path) -> pd.DataFrame:
    """Construct a customer-month panel with trailing features matching production.

    Produces, for every (customer, month) active row, the same trailing-3M and
    trailing-12M features the production scorer consumes — so the same
    `compute_churn_components` call can run unchanged.
    """
    raw = base_dir / "data" / "raw"
    processed = base_dir / "data" / "processed"

    monthly = pd.read_csv(raw / "monthly_account_metrics.csv", parse_dates=["month"])
    customers = pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"])
    amrq = pd.read_csv(processed / "account_monthly_revenue_quality.csv", parse_dates=["month"])

    panel = monthly.merge(
        amrq[["customer_id", "month", "avg_discount_pct", "renewal_risk_proxy"]],
        on=["customer_id", "month"],
        how="left",
    )
    panel = panel.merge(customers[["customer_id", "signup_date"]], on="customer_id", how="left")
    panel = panel.sort_values(["customer_id", "month"]).reset_index(drop=True)

    grp = panel.groupby("customer_id", group_keys=False)

    def trailing_mean(col: str, window: int) -> pd.Series:
        return grp[col].apply(lambda s: s.rolling(window=window, min_periods=1).mean())

    def trailing_trend(col: str, window: int = 3) -> pd.Series:
        def slope(values: np.ndarray) -> float:
            v = values[~np.isnan(values)]
            if len(v) < 2:
                return 0.0
            x = np.arange(len(v))
            return float(np.polyfit(x, v, 1)[0])

        return grp[col].apply(lambda s: s.rolling(window=window, min_periods=1).apply(slope, raw=True))

    panel["trailing_3m_usage_avg"] = trailing_mean("product_usage_score", 3)
    panel["trailing_3m_usage_trend"] = trailing_trend("product_usage_score", 3)
    panel["trailing_3m_nps_avg"] = trailing_mean("nps_score", 3)
    panel["trailing_3m_support_ticket_avg"] = trailing_mean("support_tickets", 3)
    panel["trailing_3m_payment_delay_avg"] = trailing_mean("payment_delay_days", 3)
    panel["trailing_3m_discount_avg"] = trailing_mean("avg_discount_pct", 3)

    panel["contraction_event"] = (panel["contraction_mrr"].fillna(0) > 0).astype(int)
    panel["contraction_frequency"] = (
        grp["contraction_event"].apply(lambda s: s.rolling(window=12, min_periods=1).mean())
    )

    panel["heavy_discount_event"] = (panel["avg_discount_pct"].fillna(0) >= 0.25).astype(int)
    panel["heavy_discount_frequency_12m"] = (
        grp["heavy_discount_event"].apply(lambda s: s.rolling(window=12, min_periods=1).mean())
    )

    panel["seats_active_lag3"] = grp["seats_active"].shift(3)
    panel["seat_growth_rate"] = np.where(
        panel["seats_active_lag3"].fillna(0) > 0,
        (panel["seats_active"] - panel["seats_active_lag3"]) / panel["seats_active_lag3"],
        0.0,
    )

    panel["churn_history_flag"] = grp["churn_flag"].apply(
        lambda s: s.shift(1).fillna(0).cummax()
    ).astype(int)

    panel["tenure_months"] = (
        (panel["month"].dt.year - panel["signup_date"].dt.year) * 12
        + (panel["month"].dt.month - panel["signup_date"].dt.month)
        + 1
    ).clip(lower=0)

    fill_zero = [
        "trailing_3m_usage_avg", "trailing_3m_usage_trend", "trailing_3m_nps_avg",
        "trailing_3m_support_ticket_avg", "trailing_3m_payment_delay_avg",
        "trailing_3m_discount_avg", "contraction_frequency",
        "heavy_discount_frequency_12m", "seat_growth_rate", "renewal_risk_proxy",
    ]
    for col in fill_zero:
        panel[col] = panel[col].fillna(0.0)
    panel["renewal_due_flag"] = panel["renewal_due_flag"].fillna(0).astype(int)

    return panel


def attach_forward_churn(panel: pd.DataFrame, horizon_months: int) -> pd.DataFrame:
    """For each row, flag if any churn event occurs within horizon_months."""
    panel = panel.sort_values(["customer_id", "month"]).reset_index(drop=True)
    out = panel[["customer_id", "month", "churn_flag"]].copy()
    out["forward_churn_flag"] = 0

    for _, group_idx in panel.groupby("customer_id", sort=False).groups.items():
        rows = panel.loc[group_idx, ["month", "churn_flag"]].sort_values("month")
        months = rows["month"].to_numpy()
        churn = rows["churn_flag"].fillna(0).astype(int).to_numpy()
        flags = np.zeros(len(rows), dtype=int)
        for i in range(len(rows)):
            upper = pd.Timestamp(months[i]) + pd.DateOffset(months=horizon_months)
            mask = (months > months[i]) & (months <= upper)
            flags[i] = int(churn[mask].any()) if mask.any() else 0
        out.loc[rows.index, "forward_churn_flag"] = flags

    panel = panel.merge(
        out[["customer_id", "month", "forward_churn_flag"]],
        on=["customer_id", "month"],
        how="left",
    )
    return panel


def build_calibration_tables(panel: pd.DataFrame) -> dict[str, pd.DataFrame]:
    overall_rate = float(panel["forward_churn_flag"].mean()) if len(panel) else 0.0

    tier_order = ["Low", "Moderate", "High", "Critical"]
    by_tier = (
        panel.groupby("backtest_risk_tier", as_index=False)
        .agg(
            observations=("customer_id", "count"),
            unique_accounts=("customer_id", "nunique"),
            avg_score=("backtest_churn_risk_score", "mean"),
            churn_events=("forward_churn_flag", "sum"),
            forward_churn_rate=("forward_churn_flag", "mean"),
        )
        .rename(columns={"backtest_risk_tier": "risk_tier"})
    )
    by_tier["risk_tier"] = pd.Categorical(by_tier["risk_tier"], categories=tier_order, ordered=True)
    by_tier = by_tier.sort_values("risk_tier").reset_index(drop=True)
    by_tier["lift_vs_overall"] = np.where(
        overall_rate > 0, by_tier["forward_churn_rate"] / overall_rate, 0.0
    )

    panel = panel.copy()
    panel["score_decile"] = pd.qcut(
        panel["backtest_churn_risk_score"], 10, labels=False, duplicates="drop"
    )
    panel["score_decile"] = panel["score_decile"].astype(float) + 1

    by_decile = (
        panel.groupby("score_decile", as_index=False)
        .agg(
            observations=("customer_id", "count"),
            avg_score=("backtest_churn_risk_score", "mean"),
            forward_churn_rate=("forward_churn_flag", "mean"),
        )
        .sort_values("score_decile")
        .reset_index(drop=True)
    )
    by_decile["lift_vs_overall"] = np.where(
        overall_rate > 0, by_decile["forward_churn_rate"] / overall_rate, 0.0
    )

    return {"by_tier": by_tier, "by_decile": by_decile}


def write_summary(
    summary_json_path: Path,
    panel: pd.DataFrame,
    by_tier: pd.DataFrame,
    horizon_months: int,
) -> None:
    summary_json_path.parent.mkdir(parents=True, exist_ok=True)

    overall_rate = float(panel["forward_churn_flag"].mean()) if len(panel) else 0.0
    tier_rates = (
        by_tier.set_index("risk_tier")["forward_churn_rate"].to_dict() if len(by_tier) else {}
    )

    monotonic_pairs = [("Low", "Moderate"), ("Moderate", "High"), ("High", "Critical")]
    monotonic_violations = [
        f"{a}>{b}"
        for a, b in monotonic_pairs
        if a in tier_rates and b in tier_rates and tier_rates[a] > tier_rates[b]
    ]

    summary = {
        "horizon_months": horizon_months,
        "evaluation_rows": int(len(panel)),
        "evaluation_accounts": int(panel["customer_id"].nunique()),
        "overall_forward_churn_rate": round(overall_rate, 6),
        "monotonic_violations": monotonic_violations,
        "tier_churn_rate": {k: round(float(v), 6) for k, v in tier_rates.items()},
        "weights": CHURN_WEIGHTS,
    }
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    base_dir = Path(args.base_dir).resolve()

    panel = build_trailing_panel(base_dir)
    panel = attach_forward_churn(panel, args.horizon_months)

    components = compute_churn_components(panel)
    panel["backtest_churn_risk_score"] = score_from_components(components, CHURN_WEIGHTS)
    panel["backtest_risk_tier"] = panel["backtest_churn_risk_score"].apply(risk_tier)

    max_month = panel["month"].max()
    cutoff_month = max_month - pd.DateOffset(months=args.horizon_months)
    panel = panel[(panel["active_flag"] == 1) & (panel["month"] <= cutoff_month)].copy()

    tables = build_calibration_tables(panel)
    by_tier = tables["by_tier"]
    by_decile = tables["by_decile"]

    tier_path = base_dir / args.tier_output_path
    decile_path = base_dir / args.decile_output_path
    summary_path = base_dir / args.summary_json_path

    tier_path.parent.mkdir(parents=True, exist_ok=True)
    decile_path.parent.mkdir(parents=True, exist_ok=True)
    by_tier.to_csv(tier_path, index=False)
    by_decile.to_csv(decile_path, index=False)

    write_summary(summary_path, panel, by_tier, args.horizon_months)

    logger = logging.getLogger("backtest")
    logger.info("backtest calibration complete")
    logger.info("  tier output    : %s", tier_path)
    logger.info("  decile output  : %s", decile_path)
    logger.info("  summary JSON   : %s", summary_path)


if __name__ == "__main__":
    main()

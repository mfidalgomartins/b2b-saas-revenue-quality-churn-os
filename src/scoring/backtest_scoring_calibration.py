from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest churn-risk calibration using forward churn outcomes.")
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


def risk_tier(score: float) -> str:
    if score < 30:
        return "Low"
    if score < 55:
        return "Moderate"
    if score < 75:
        return "High"
    return "Critical"


def compute_forward_churn_flag(group: pd.DataFrame, horizon_months: int) -> pd.Series:
    g = group.sort_values("month").reset_index(drop=True)
    months = g["month"].to_numpy()
    churn = g["churn_flag"].fillna(0).astype(int).to_numpy()

    out = np.zeros(len(g), dtype=int)
    for i in range(len(g)):
        current = pd.Timestamp(months[i])
        upper = current + pd.DateOffset(months=horizon_months)
        future_mask = (months > current) & (months <= upper)
        out[i] = int(churn[future_mask].any()) if np.any(future_mask) else 0
    return pd.Series(out, index=g.index)


def build_risk_panel(base_dir: Path, horizon_months: int) -> pd.DataFrame:
    raw_dir = base_dir / "data" / "raw"
    processed_dir = base_dir / "data" / "processed"

    mm = pd.read_csv(raw_dir / "monthly_account_metrics.csv", parse_dates=["month"])
    amrq = pd.read_csv(processed_dir / "account_monthly_revenue_quality.csv", parse_dates=["month"])

    panel = mm.merge(
        amrq[
            [
                "customer_id",
                "month",
                "avg_discount_pct",
            ]
        ],
        on=["customer_id", "month"],
        how="left",
    )

    panel = panel.sort_values(["customer_id", "month"]).reset_index(drop=True)
    panel["contraction_event"] = (panel["contraction_mrr"].fillna(0) > 0).astype(int)
    panel["trailing_contraction_freq_3m"] = (
        panel.groupby("customer_id")["contraction_event"]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    usage_risk = np.clip((55 - panel["product_usage_score"].fillna(0)) / 55, 0, 1)
    nps_risk = np.clip((10 - panel["nps_score"].fillna(0)) / 110, 0, 1)
    delay_risk = np.clip(panel["payment_delay_days"].fillna(0) / 60, 0, 1)
    support_risk = np.clip((panel["support_tickets"].fillna(0) - 4) / 20, 0, 1)
    discount_risk = np.clip((panel["avg_discount_pct"].fillna(0) - 0.18) / 0.35, 0, 1)
    contraction_risk = np.clip(panel["trailing_contraction_freq_3m"] / 0.5, 0, 1)

    panel["backtest_churn_risk_score"] = 100 * np.clip(
        0.25 * usage_risk
        + 0.20 * nps_risk
        + 0.20 * delay_risk
        + 0.10 * support_risk
        + 0.15 * discount_risk
        + 0.10 * contraction_risk,
        0,
        1,
    )
    panel["backtest_risk_tier"] = panel["backtest_churn_risk_score"].apply(risk_tier)

    panel["forward_3m_churn_flag"] = 0
    for _, idx in panel.groupby("customer_id").groups.items():
        customer_rows = panel.loc[idx, ["month", "churn_flag"]].sort_values("month")
        forward_flags = compute_forward_churn_flag(customer_rows, horizon_months)
        panel.loc[customer_rows.index, "forward_3m_churn_flag"] = forward_flags.values

    max_month = panel["month"].max()
    cutoff_month = max_month - pd.DateOffset(months=horizon_months)
    panel = panel[(panel["active_flag"] == 1) & (panel["month"] <= cutoff_month)].copy()
    return panel


def build_calibration_tables(panel: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    overall_rate = float(panel["forward_3m_churn_flag"].mean()) if len(panel) > 0 else 0.0

    tier_order = ["Low", "Moderate", "High", "Critical"]
    by_tier = (
        panel.groupby("backtest_risk_tier", as_index=False)
        .agg(
            observations=("customer_id", "count"),
            unique_accounts=("customer_id", "nunique"),
            avg_score=("backtest_churn_risk_score", "mean"),
            churn_events_3m=("forward_3m_churn_flag", "sum"),
            churn_rate_3m=("forward_3m_churn_flag", "mean"),
        )
        .rename(columns={"backtest_risk_tier": "risk_tier"})
    )
    by_tier["risk_tier"] = pd.Categorical(by_tier["risk_tier"], categories=tier_order, ordered=True)
    by_tier = by_tier.sort_values("risk_tier").reset_index(drop=True)
    by_tier["lift_vs_overall"] = np.where(overall_rate > 0, by_tier["churn_rate_3m"] / overall_rate, 0.0)

    panel = panel.copy()
    panel["score_decile"] = pd.qcut(
        panel["backtest_churn_risk_score"],
        10,
        labels=False,
        duplicates="drop",
    )
    panel["score_decile"] = panel["score_decile"].astype(float) + 1

    by_decile = (
        panel.groupby("score_decile", as_index=False)
        .agg(
            observations=("customer_id", "count"),
            avg_score=("backtest_churn_risk_score", "mean"),
            churn_rate_3m=("forward_3m_churn_flag", "mean"),
        )
        .sort_values("score_decile")
        .reset_index(drop=True)
    )
    by_decile["lift_vs_overall"] = np.where(overall_rate > 0, by_decile["churn_rate_3m"] / overall_rate, 0.0)

    return {"by_tier": by_tier, "by_decile": by_decile}


def write_summary(
    summary_json_path: Path,
    panel: pd.DataFrame,
    by_tier: pd.DataFrame,
    horizon_months: int,
) -> None:
    summary_json_path.parent.mkdir(parents=True, exist_ok=True)

    overall_rate = float(panel["forward_3m_churn_flag"].mean()) if len(panel) > 0 else 0.0
    tier_rates = by_tier.set_index("risk_tier")["churn_rate_3m"].to_dict() if len(by_tier) > 0 else {}
    monotonic_pairs = [("Low", "Moderate"), ("Moderate", "High"), ("High", "Critical")]
    monotonic_violations: List[str] = []
    for a, b in monotonic_pairs:
        if a in tier_rates and b in tier_rates and tier_rates[a] > tier_rates[b]:
            monotonic_violations.append(f"{a}>{b}")

    summary = {
        "horizon_months": horizon_months,
        "evaluation_rows": int(len(panel)),
        "evaluation_accounts": int(panel["customer_id"].nunique()),
        "overall_forward_churn_rate": round(overall_rate, 6),
        "monotonic_violations": monotonic_violations,
        "max_tier_churn_rate": float(by_tier["churn_rate_3m"].max()) if len(by_tier) > 0 else 0.0,
        "min_tier_churn_rate": float(by_tier["churn_rate_3m"].min()) if len(by_tier) > 0 else 0.0,
    }
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    tier_output_path = base_dir / args.tier_output_path
    decile_output_path = base_dir / args.decile_output_path
    summary_json_path = base_dir / args.summary_json_path

    panel = build_risk_panel(base_dir, args.horizon_months)
    tables = build_calibration_tables(panel)
    by_tier = tables["by_tier"]
    by_decile = tables["by_decile"]

    tier_output_path.parent.mkdir(parents=True, exist_ok=True)
    decile_output_path.parent.mkdir(parents=True, exist_ok=True)
    by_tier.to_csv(tier_output_path, index=False)
    by_decile.to_csv(decile_output_path, index=False)

    write_summary(summary_json_path, panel, by_tier, args.horizon_months)

    print("Scoring backtest calibration complete.")
    print(f"Tier output: {tier_output_path}")
    print(f"Decile output: {decile_output_path}")
    print(f"Summary JSON: {summary_json_path}")


if __name__ == "__main__":
    main()

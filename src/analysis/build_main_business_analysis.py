from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def load_tables(base_dir: Path) -> dict[str, pd.DataFrame]:
    raw = base_dir / "data" / "raw"
    processed = base_dir / "data" / "processed"
    tables: dict[str, pd.DataFrame] = {
        "customers": pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"]),
        "subscriptions": pd.read_csv(raw / "subscriptions.csv", parse_dates=["subscription_start_date"]),
        "monthly_raw": pd.read_csv(raw / "monthly_account_metrics.csv", parse_dates=["month"]),
        "monthly_quality": pd.read_csv(processed / "account_monthly_revenue_quality.csv", parse_dates=["month"]),
        "health": pd.read_csv(processed / "customer_health_features.csv"),
        "scoring": pd.read_csv(processed / "account_scoring_model_output.csv"),
        "manager_summary": pd.read_csv(processed / "account_manager_summary.csv"),
        "risk_base": pd.read_csv(processed / "account_risk_base.csv"),
    }
    return tables


def _safe_weighted_avg(df: pd.DataFrame, value_col: str, weight_col: str) -> float:
    if len(df) == 0:
        return 0.0
    weights = pd.to_numeric(df[weight_col], errors="coerce").fillna(0.0)
    values = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)
    if float(weights.sum()) <= 0:
        return float(values.mean()) if len(values) else 0.0
    return float(np.average(values, weights=weights))


def build_base_panel(t: dict[str, pd.DataFrame]) -> pd.DataFrame:
    customers = t["customers"][["customer_id", "region", "segment", "industry", "acquisition_channel", "account_manager_id"]]
    subscriptions = t["subscriptions"][["customer_id", "subscription_start_date", "plan_id", "status", "discount_pct"]].rename(
        columns={"subscription_start_date": "month"}
    )
    monthly = t["monthly_quality"].merge(
        t["monthly_raw"][
            [
                "customer_id",
                "month",
                "active_flag",
                "churn_flag",
                "renewal_due_flag",
                "product_usage_score",
                "nps_score",
                "payment_delay_days",
            ]
        ],
        on=["customer_id", "month"],
        how="left",
    )
    monthly = monthly.merge(customers, on="customer_id", how="left")
    monthly = monthly.merge(subscriptions, on=["customer_id", "month"], how="left", suffixes=("", "_sub"))
    return monthly


def forward_churn_flag(monthly: pd.DataFrame, horizon_months: int = 3) -> pd.DataFrame:
    g = monthly[["customer_id", "month", "churn_flag"]].sort_values(["customer_id", "month"]).copy()
    g["forward_churn_flag"] = 0
    for cid, idx in g.groupby("customer_id").groups.items():
        s = g.loc[idx].sort_values("month")
        months = s["month"].to_numpy()
        churn = s["churn_flag"].fillna(0).astype(int).to_numpy()
        out = np.zeros(len(s), dtype=int)
        for i in range(len(s)):
            upper = pd.Timestamp(months[i]) + pd.DateOffset(months=horizon_months)
            mask = (months > months[i]) & (months <= upper)
            out[i] = int(churn[mask].any()) if np.any(mask) else 0
        g.loc[s.index, "forward_churn_flag"] = out
    return g[["customer_id", "month", "forward_churn_flag"]]


def compute_metrics(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    panel = build_base_panel(t)
    panel = panel.sort_values(["customer_id", "month"]).reset_index(drop=True)
    months = panel["month"].dropna().sort_values().unique()
    month_start = pd.Timestamp(months[0]) if len(months) else pd.NaT
    month_end = pd.Timestamp(months[-1]) if len(months) else pd.NaT

    active = panel[panel["active_flag"] == 1].copy()
    monthly_roll = active.groupby("month", as_index=False).agg(
        mrr=("active_mrr", "sum"),
        expansion_mrr=("expansion_mrr", "sum"),
        contraction_mrr=("contraction_mrr", "sum"),
    )
    churn_mrr = (
        active[active["churn_flag"] == 1]
        .groupby("month", as_index=False)["active_mrr"]
        .sum()
        .rename(columns={"active_mrr": "churn_mrr"})
    )
    monthly_roll = monthly_roll.merge(churn_mrr, on="month", how="left").fillna({"churn_mrr": 0.0})
    monthly_roll = monthly_roll.sort_values("month")

    monthly_roll["grr"] = np.where(
        monthly_roll["mrr"] > 0,
        (monthly_roll["mrr"] - monthly_roll["contraction_mrr"] - monthly_roll["churn_mrr"]) / monthly_roll["mrr"],
        np.nan,
    )
    monthly_roll["nrr"] = np.where(
        monthly_roll["mrr"] > 0,
        (monthly_roll["mrr"] + monthly_roll["expansion_mrr"] - monthly_roll["contraction_mrr"] - monthly_roll["churn_mrr"]) / monthly_roll["mrr"],
        np.nan,
    )

    if len(monthly_roll) > 0:
        mrr_start = float(monthly_roll.iloc[0]["mrr"])
        mrr_end = float(monthly_roll.iloc[-1]["mrr"])
        monthly_growth_rate = float((mrr_end / mrr_start) ** (1 / max(len(monthly_roll) - 1, 1)) - 1) if mrr_start > 0 else 0.0
        latest_grr = float(monthly_roll.iloc[-1]["grr"])
        latest_nrr = float(monthly_roll.iloc[-1]["nrr"])
    else:
        mrr_start = 0.0
        mrr_end = 0.0
        monthly_growth_rate = 0.0
        latest_grr = 0.0
        latest_nrr = 0.0

    latest_month = panel["month"].max()
    first_month = panel["month"].min()
    latest = panel[panel["month"] == latest_month].copy()
    first = panel[panel["month"] == first_month].copy()
    latest_active = latest[latest["active_mrr"] > 0].copy()
    first_active = first[first["active_mrr"] > 0].copy()

    top10_share = float(latest_active.nlargest(10, "active_mrr")["active_mrr"].sum() / latest_active["active_mrr"].sum()) if len(latest_active) else 0.0
    top50_share = float(latest_active.nlargest(50, "active_mrr")["active_mrr"].sum() / latest_active["active_mrr"].sum()) if len(latest_active) else 0.0

    discounted_share_latest = float(
        latest_active.loc[latest_active["discount_dependency_flag"] == 1, "active_mrr"].sum() / latest_active["active_mrr"].sum()
    ) if len(latest_active) else 0.0

    scoring = t["scoring"].copy()
    scoring_active = scoring[scoring["current_mrr"] > 0].copy()
    high_risk_active = scoring_active[scoring_active["governance_priority_tier"].isin(["High", "Critical"])].copy()
    at_risk_share_latest = float(high_risk_active["current_mrr"].sum() / scoring_active["current_mrr"].sum()) if len(scoring_active) else 0.0

    segment_churn = (
        active.groupby("segment", as_index=False)
        .agg(active_rows=("customer_id", "count"), churn_events=("churn_flag", "sum"))
        .assign(logo_churn_rate=lambda d: np.where(d["active_rows"] > 0, d["churn_events"] / d["active_rows"], 0.0))
        .sort_values("logo_churn_rate", ascending=False)
    )

    plan_churn = (
        active.groupby("plan_id", as_index=False)
        .agg(active_rows=("customer_id", "count"), churn_events=("churn_flag", "sum"))
        .assign(logo_churn_rate=lambda d: np.where(d["active_rows"] > 0, d["churn_events"] / d["active_rows"], 0.0))
    )

    channel_churn = (
        active.groupby("acquisition_channel", as_index=False)
        .agg(active_rows=("customer_id", "count"), churn_events=("churn_flag", "sum"))
        .assign(logo_churn_rate=lambda d: np.where(d["active_rows"] > 0, d["churn_events"] / d["active_rows"], 0.0))
        .sort_values("logo_churn_rate", ascending=False)
    )

    renewal_churn = (
        active.groupby("renewal_due_flag", as_index=False)
        .agg(active_rows=("customer_id", "count"), churn_events=("churn_flag", "sum"))
        .assign(logo_churn_rate=lambda d: np.where(d["active_rows"] > 0, d["churn_events"] / d["active_rows"], 0.0))
    )

    logo_churn_rate = float(active["churn_flag"].sum() / len(active)) if len(active) else 0.0
    churn_mrr_total = float(active.loc[active["churn_flag"] == 1, "active_mrr"].sum())
    revenue_churn_rate = churn_mrr_total / float(active["active_mrr"].sum()) if len(active) and float(active["active_mrr"].sum()) > 0 else 0.0

    fw = forward_churn_flag(panel, horizon_months=3)
    discount_panel = panel.merge(fw, on=["customer_id", "month"], how="left")
    discount_panel = discount_panel[(discount_panel["active_flag"] == 1) & (discount_panel["active_mrr"] > 0)].copy()
    discount_panel["discount_band"] = pd.cut(
        discount_panel["avg_discount_pct"].fillna(0.0),
        bins=[-1, 0.10, 0.20, 0.30, 1.0],
        labels=["<=10%", "10-20%", "20-30%", ">30%"],
    )
    discount_future_churn = (
        discount_panel.groupby("discount_band", as_index=False, observed=False)
        .agg(rows=("customer_id", "count"), future_churn_3m_rate=("forward_churn_flag", "mean"))
        .sort_values("discount_band")
    )

    expansion_events = panel[(panel["active_mrr"] > 0) & (panel["expansion_mrr"] > 0)].copy()
    expansion_events["expansion_quality"] = np.select(
        [
            (expansion_events["avg_discount_pct"] >= 0.25)
            | (expansion_events["product_usage_score"] < 55)
            | (expansion_events["payment_delay_days"] > 20)
            | (expansion_events["nps_score"] < 10),
            (expansion_events["avg_discount_pct"] <= 0.20)
            & (expansion_events["product_usage_score"] >= 65)
            & (expansion_events["payment_delay_days"] <= 15)
            & (expansion_events["nps_score"] >= 20),
        ],
        ["fragile", "healthy"],
        default="watch",
    )

    exp_quality_summary = expansion_events.groupby("expansion_quality", as_index=False).agg(
        events=("customer_id", "count"),
        expansion_mrr=("expansion_mrr", "sum"),
    )

    at_risk_accounts = high_risk_active.copy()
    at_risk_mrr_total = float(at_risk_accounts["current_mrr"].sum())
    at_risk_count = int(at_risk_accounts["customer_id"].nunique())
    top20_at_risk_share = (
        float(at_risk_accounts.nlargest(20, "current_mrr")["current_mrr"].sum() / at_risk_mrr_total) if at_risk_mrr_total > 0 else 0.0
    )

    manager_summary = t["manager_summary"].copy()
    discount_p90 = float(manager_summary["avg_discount"].quantile(0.90)) if len(manager_summary) else 0.0
    unusual_discount_managers = (
        manager_summary[manager_summary["avg_discount"] >= discount_p90]
        .sort_values("avg_discount", ascending=False)
        .head(8)
    )

    metrics: dict[str, Any] = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "month_start": str(month_start.date()) if pd.notna(month_start) else "",
            "month_end": str(month_end.date()) if pd.notna(month_end) else "",
            "n_months": int(len(monthly_roll)),
        },
        "section1": {
            "mrr_start": round(mrr_start, 2),
            "mrr_end": round(mrr_end, 2),
            "arr_start": round(mrr_start * 12, 2),
            "arr_end": round(mrr_end * 12, 2),
            "monthly_growth_rate": round(monthly_growth_rate, 6),
            "w_realized_price_start": round(_safe_weighted_avg(first_active, "realized_price_index", "active_mrr"), 6),
            "w_realized_price_end": round(_safe_weighted_avg(latest_active, "realized_price_index", "active_mrr"), 6),
            "w_discount_end": round(_safe_weighted_avg(latest_active, "avg_discount_pct", "active_mrr"), 6),
            "expansion_sum": round(float(panel["expansion_mrr"].sum()), 2),
            "contraction_sum": round(float(panel["contraction_mrr"].sum()), 2),
            "top10_share": round(top10_share, 6),
            "top50_share": round(top50_share, 6),
            "share_discounted_mrr_latest": round(discounted_share_latest, 6),
            "share_at_risk_mrr_latest": round(at_risk_share_latest, 6),
        },
        "section2": {
            "logo_churn_rate": round(logo_churn_rate, 6),
            "revenue_churn_rate": round(revenue_churn_rate, 6),
            "overall_grr": round(float(monthly_roll["grr"].mean()), 6) if len(monthly_roll) else 0.0,
            "overall_nrr": round(float(monthly_roll["nrr"].mean()), 6) if len(monthly_roll) else 0.0,
            "latest_grr": round(latest_grr, 6),
            "latest_nrr": round(latest_nrr, 6),
            "churn_segment": segment_churn.to_dict(orient="records"),
            "churn_plan": plan_churn.to_dict(orient="records"),
            "churn_channel": channel_churn.to_dict(orient="records"),
            "renewal_churn": renewal_churn.to_dict(orient="records"),
        },
        "section3": {
            "discount_future_churn_3m": discount_future_churn.to_dict(orient="records"),
        },
        "section4": {
            "expansion_quality_summary": exp_quality_summary.to_dict(orient="records"),
        },
        "section5": {
            "at_risk_accounts_count": at_risk_count,
            "at_risk_mrr_total": round(at_risk_mrr_total, 2),
            "top20_at_risk_share_within_at_risk": round(top20_at_risk_share, 6),
        },
        "section6": {
            "unusual_discount_managers": unusual_discount_managers.to_dict(orient="records"),
        },
    }
    return metrics


def build_memo(metrics: dict[str, Any], output_path: Path) -> None:
    s1 = metrics["section1"]
    s2 = metrics["section2"]
    s3 = metrics["section3"]
    s4 = metrics["section4"]
    s5 = metrics["section5"]
    meta = metrics["meta"]

    discount_rows = s3["discount_future_churn_3m"]
    worst_discount_band = max(discount_rows, key=lambda r: r.get("future_churn_3m_rate", 0)) if discount_rows else None
    exp_rows = s4["expansion_quality_summary"]
    exp_total = sum(float(r.get("expansion_mrr", 0)) for r in exp_rows)
    fragile_exp_share = (
        sum(float(r.get("expansion_mrr", 0)) for r in exp_rows if r.get("expansion_quality") == "fragile") / exp_total if exp_total > 0 else 0.0
    )

    worst_band_name = worst_discount_band["discount_band"] if worst_discount_band else "n/a"
    worst_band_rate = float(worst_discount_band["future_churn_3m_rate"]) if worst_discount_band else 0.0

    memo = f"""# Main Business Analysis Memo

## Scope and Definitions
Analysis window: {meta['month_start']} to {meta['month_end']} ({meta['n_months']} months).

Core metric definitions:
- `MRR`: sum of `active_mrr` in month.
- `ARR`: `12 * MRR`.
- `GRR`: `(starting_mrr - contraction_mrr - churn_mrr) / starting_mrr`.
- `NRR`: `(starting_mrr + expansion_mrr - contraction_mrr - churn_mrr) / starting_mrr`.
- `Logo churn rate`: churn events / active account-month rows.
- `Revenue churn rate`: churned MRR / active MRR.

## 1) Revenue Quality Overview
**Key takeaway:** Strong topline growth with non-trivial quality exposure in discounted and at-risk revenue pockets.

- MRR: `${s1['mrr_start']:,.0f}` -> `${s1['mrr_end']:,.0f}` (`{s1['monthly_growth_rate']:.2%}` implied monthly growth).
- ARR run-rate: `${s1['arr_start']:,.0f}` -> `${s1['arr_end']:,.0f}`.
- Latest weighted realized price index: `{s1['w_realized_price_end']:.3f}`.
- Latest weighted discount: `{s1['w_discount_end']:.1%}`.
- Latest discounted-dependent MRR share: `{s1['share_discounted_mrr_latest']:.1%}`.
- Latest high-risk MRR share: `{s1['share_at_risk_mrr_latest']:.1%}`.

Interpretation:
- Growth quality improved on pricing realization, but downside concentration remains material.

Caveat:
- Realized pricing reflects both commercial pricing and collections quality.

## 2) Retention and Churn Diagnostics
**Key takeaway:** Portfolio retention is stable but expansion buffer remains thin.

- Logo churn: `{s2['logo_churn_rate']:.2%}`.
- Revenue churn: `{s2['revenue_churn_rate']:.2%}`.
- Latest GRR/NRR: `{s2['latest_grr']:.2%}` / `{s2['latest_nrr']:.2%}`.

Interpretation:
- NRR near parity indicates limited cushion if churn or contraction rises.

Caveat:
- Diagnostics are associative; they do not infer causal channel or segment effects.

## 3) Discount and Fragility
**Key takeaway:** Higher discount intensity is associated with higher forward churn in the highest discount bands.

- Worst discount-band forward churn (3m): `{worst_band_name}` at `{worst_band_rate:.2%}`.

Interpretation:
- Extreme discounting should be treated as a governance signal, especially near renewal.

Caveat:
- Correlation does not establish causality.

## 4) Expansion Quality
**Key takeaway:** Expansion remains positive but fragile expansion share is material.

- Fragile expansion MRR share: `{fragile_exp_share:.1%}`.
- Total expansion MRR observed: `${exp_total:,.0f}`.

Interpretation:
- Part of growth is potentially less durable and should be monitored post-expansion.

## 5) Account Health and Risk Concentration
**Key takeaway:** Downside risk is concentrated enough to prioritize with account-level governance.

- At-risk accounts: `{s5['at_risk_accounts_count']}`.
- At-risk MRR: `${s5['at_risk_mrr_total']:,.0f}`.
- Top-20 share within at-risk MRR: `{s5['top20_at_risk_share_within_at_risk']:.1%}`.

## Final Synthesis
- Healthy: strong recurring scale-up and stable gross retention.
- Fragile: meaningful discounted and high-risk revenue exposure.
- Biggest risk: concentrated downside in a small set of accounts with weak forward signals.
- Leadership blind spot if focused only on topline: growth durability can deteriorate before headline MRR does.
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(memo, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build reproducible main business analysis memo and metrics.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--metrics-output-path", type=str, default="reports/main_business_analysis_metrics.json")
    parser.add_argument("--memo-output-path", type=str, default="reports/main_business_analysis_memo.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    tables = load_tables(base_dir)
    metrics = compute_metrics(tables)

    metrics_path = (base_dir / args.metrics_output_path).resolve()
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    build_memo(metrics, (base_dir / args.memo_output_path).resolve())

    print("Main business analysis build complete.")
    print(f"Metrics: {metrics_path}")
    print(f"Memo: {(base_dir / args.memo_output_path).resolve()}")


if __name__ == "__main__":
    main()

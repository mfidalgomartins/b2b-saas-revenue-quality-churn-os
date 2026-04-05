from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


def load_inputs(base_dir: Path) -> Dict[str, pd.DataFrame]:
    processed = base_dir / "data/processed"
    raw = base_dir / "data/raw"

    tables: Dict[str, pd.DataFrame] = {
        "monthly_quality": pd.read_csv(processed / "account_monthly_revenue_quality.csv", parse_dates=["month"]),
        "monthly_raw": pd.read_csv(raw / "monthly_account_metrics.csv", parse_dates=["month"]),
        "scoring": pd.read_csv(processed / "account_scoring_model_output.csv"),
        "health": pd.read_csv(processed / "customer_health_features.csv"),
    }
    return tables


def clip_rate(x: float, low: float, high: float) -> float:
    return float(np.clip(x, low, high))


def build_company_monthly_frame(monthly_quality: pd.DataFrame, monthly_raw: pd.DataFrame) -> pd.DataFrame:
    frame = monthly_quality.merge(
        monthly_raw[["customer_id", "month", "churn_flag"]],
        on=["customer_id", "month"],
        how="left",
    )

    monthly = frame.groupby("month", as_index=False).agg(
        mrr=("active_mrr", "sum"),
        expansion_mrr=("expansion_mrr", "sum"),
        contraction_mrr=("contraction_mrr", "sum"),
    )

    churn = (
        frame[frame["churn_flag"] == 1]
        .groupby("month", as_index=False)["active_mrr"]
        .sum()
        .rename(columns={"active_mrr": "churn_mrr"})
    )
    monthly = monthly.merge(churn, on="month", how="left").fillna({"churn_mrr": 0.0})

    monthly = monthly.sort_values("month").reset_index(drop=True)
    monthly["prev_mrr"] = monthly["mrr"].shift(1)
    monthly["observed_growth_rate"] = np.where(
        monthly["prev_mrr"] > 0,
        (monthly["mrr"] - monthly["prev_mrr"]) / monthly["prev_mrr"],
        np.nan,
    )

    monthly["expansion_rate"] = np.where(monthly["prev_mrr"] > 0, monthly["expansion_mrr"] / monthly["prev_mrr"], np.nan)
    monthly["contraction_rate"] = np.where(monthly["prev_mrr"] > 0, monthly["contraction_mrr"] / monthly["prev_mrr"], np.nan)
    monthly["churn_rate"] = np.where(monthly["prev_mrr"] > 0, monthly["churn_mrr"] / monthly["prev_mrr"], np.nan)

    monthly["net_new_rate"] = (
        monthly["observed_growth_rate"]
        - monthly["expansion_rate"]
        + monthly["contraction_rate"]
        + monthly["churn_rate"]
    )

    return monthly


def weighted_recent_average(series: pd.Series) -> float:
    values = series.dropna().values
    if len(values) == 0:
        return 0.0
    weights = np.arange(1, len(values) + 1)
    return float(np.average(values, weights=weights))


def estimate_baseline_rates(company_monthly: pd.DataFrame, lookback_months: int = 6) -> Dict[str, float]:
    hist = company_monthly.tail(lookback_months).copy()

    rates = {
        "expansion_rate": weighted_recent_average(hist["expansion_rate"]),
        "contraction_rate": weighted_recent_average(hist["contraction_rate"]),
        "churn_rate": weighted_recent_average(hist["churn_rate"]),
        "net_new_rate": weighted_recent_average(hist["net_new_rate"]),
    }

    # Guardrails to keep rate assumptions commercially plausible.
    rates["expansion_rate"] = clip_rate(rates["expansion_rate"], 0.0, 0.08)
    rates["contraction_rate"] = clip_rate(rates["contraction_rate"], 0.0, 0.06)
    rates["churn_rate"] = clip_rate(rates["churn_rate"], 0.0, 0.06)
    rates["net_new_rate"] = clip_rate(rates["net_new_rate"], -0.05, 0.08)

    return rates


def estimate_risk_overlay_rates(
    baseline_rates: Dict[str, float],
    scoring: pd.DataFrame,
) -> Dict[str, float]:
    active = scoring[scoring["current_mrr"] > 0].copy()
    total_mrr = float(active["current_mrr"].sum())

    high_risk = active[active["governance_priority_tier"].isin(["High", "Critical"])].copy()
    high_risk_mrr = float(high_risk["current_mrr"].sum())
    high_risk_share = high_risk_mrr / total_mrr if total_mrr > 0 else 0.0

    if high_risk_mrr > 0:
        top20_share_high_risk = float(high_risk.nlargest(20, "current_mrr")["current_mrr"].sum() / high_risk_mrr)
    else:
        top20_share_high_risk = 0.0

    concentration_multiplier = 1.0 + max(0.0, top20_share_high_risk - 0.50)

    extra_churn = high_risk_share * 0.06 * concentration_multiplier
    extra_contraction = high_risk_share * 0.04 * concentration_multiplier
    expansion_haircut = high_risk_share * 0.03
    net_new_haircut = high_risk_share * 0.02

    adjusted = {
        "expansion_rate": clip_rate(baseline_rates["expansion_rate"] * (1 - expansion_haircut), 0.0, 0.08),
        "contraction_rate": clip_rate(baseline_rates["contraction_rate"] + extra_contraction, 0.0, 0.06),
        "churn_rate": clip_rate(baseline_rates["churn_rate"] + extra_churn, 0.0, 0.06),
        "net_new_rate": clip_rate(baseline_rates["net_new_rate"] - net_new_haircut, -0.05, 0.08),
    }
    return adjusted


def simulate_mrr_trajectory(
    start_month: pd.Timestamp,
    start_mrr: float,
    horizon_months: int,
    rates: Dict[str, float],
    scenario_name: str,
    scenario_type: str,
    realized_price_index_assumption: float,
) -> pd.DataFrame:
    forecast_months = pd.date_range(start=start_month + pd.DateOffset(months=1), periods=horizon_months, freq="MS")

    rows: List[Dict[str, object]] = []
    mrr = float(start_mrr)
    for month in forecast_months:
        expansion_mrr = mrr * rates["expansion_rate"]
        contraction_mrr = mrr * rates["contraction_rate"]
        churn_mrr = mrr * rates["churn_rate"]
        net_new_mrr = mrr * rates["net_new_rate"]

        mrr_next = max(0.0, mrr + expansion_mrr - contraction_mrr - churn_mrr + net_new_mrr)
        start_mrr_rounded = round(mrr, 2)
        expansion_mrr_rounded = round(expansion_mrr, 2)
        contraction_mrr_rounded = round(contraction_mrr, 2)
        churn_mrr_rounded = round(churn_mrr, 2)
        net_new_mrr_rounded = round(net_new_mrr, 2)
        forecast_mrr_rounded = round(mrr_next, 2)
        forecast_arr_rounded = round(forecast_mrr_rounded * 12, 2)
        realized_price_index_rounded = round(realized_price_index_assumption, 4)
        realized_arr_rounded = round(forecast_arr_rounded * realized_price_index_rounded, 2)

        rows.append(
            {
                "scenario": scenario_name,
                "scenario_type": scenario_type,
                "forecast_month": month,
                "start_mrr": start_mrr_rounded,
                "expansion_mrr": expansion_mrr_rounded,
                "contraction_mrr": contraction_mrr_rounded,
                "churn_mrr": churn_mrr_rounded,
                "net_new_mrr": net_new_mrr_rounded,
                "forecast_mrr": forecast_mrr_rounded,
                "forecast_arr": forecast_arr_rounded,
                "realized_price_index_assumption": realized_price_index_rounded,
                "realized_arr_estimate": realized_arr_rounded,
                "assumed_expansion_rate": round(rates["expansion_rate"], 5),
                "assumed_contraction_rate": round(rates["contraction_rate"], 5),
                "assumed_churn_rate": round(rates["churn_rate"], 5),
                "assumed_net_new_rate": round(rates["net_new_rate"], 5),
            }
        )

        mrr = mrr_next

    return pd.DataFrame(rows)


def build_scenarios(
    latest_month: pd.Timestamp,
    start_mrr: float,
    baseline_rates: Dict[str, float],
    risk_adjusted_rates: Dict[str, float],
    latest_realized_price_index: float,
    horizon_months: int,
) -> pd.DataFrame:
    scenario_config = [
        {
            "name": "base_case",
            "type": "base",
            "rates": baseline_rates,
            "realized_price_index": latest_realized_price_index,
        },
        {
            "name": "downside_case",
            "type": "fragile-growth",
            "rates": {
                "expansion_rate": clip_rate(baseline_rates["expansion_rate"] * 0.80, 0.0, 0.08),
                "contraction_rate": clip_rate(baseline_rates["contraction_rate"] * 1.35, 0.0, 0.06),
                "churn_rate": clip_rate(baseline_rates["churn_rate"] * 1.50, 0.0, 0.06),
                "net_new_rate": clip_rate(baseline_rates["net_new_rate"] * 0.70, -0.05, 0.08),
            },
            "realized_price_index": latest_realized_price_index - 0.01,
        },
        {
            "name": "improvement_case",
            "type": "healthy-growth",
            "rates": {
                "expansion_rate": clip_rate(baseline_rates["expansion_rate"] * 1.15, 0.0, 0.08),
                "contraction_rate": clip_rate(baseline_rates["contraction_rate"] * 0.85, 0.0, 0.06),
                "churn_rate": clip_rate(baseline_rates["churn_rate"] * 0.80, 0.0, 0.06),
                "net_new_rate": clip_rate(baseline_rates["net_new_rate"] * 1.15, -0.05, 0.08),
            },
            "realized_price_index": latest_realized_price_index + 0.005,
        },
        {
            "name": "discount_discipline_improvement_case",
            "type": "policy-improvement",
            "rates": {
                "expansion_rate": clip_rate(baseline_rates["expansion_rate"] * 0.94, 0.0, 0.08),
                "contraction_rate": clip_rate(baseline_rates["contraction_rate"] * 0.90, 0.0, 0.06),
                "churn_rate": clip_rate(baseline_rates["churn_rate"] * 0.88, 0.0, 0.06),
                "net_new_rate": clip_rate(baseline_rates["net_new_rate"] * 0.97, -0.05, 0.08),
            },
            "realized_price_index": latest_realized_price_index + 0.020,
        },
        {
            "name": "risk_adjusted_case",
            "type": "risk-adjusted",
            "rates": risk_adjusted_rates,
            "realized_price_index": latest_realized_price_index - 0.005,
        },
    ]

    trajectories = []
    for cfg in scenario_config:
        traj = simulate_mrr_trajectory(
            start_month=latest_month,
            start_mrr=start_mrr,
            horizon_months=horizon_months,
            rates=cfg["rates"],
            scenario_name=cfg["name"],
            scenario_type=cfg["type"],
            realized_price_index_assumption=clip_rate(cfg["realized_price_index"], 0.70, 0.95),
        )
        trajectories.append(traj)

    return pd.concat(trajectories, ignore_index=True)


def summarize_scenarios(trajectories: pd.DataFrame, start_mrr: float) -> pd.DataFrame:
    rows = []
    for scenario, g in trajectories.groupby("scenario"):
        g = g.sort_values("forecast_month")
        end_mrr = float(g.iloc[-1]["forecast_mrr"])
        end_arr = float(g.iloc[-1]["forecast_arr"])
        realized_arr = float(g.iloc[-1]["realized_arr_estimate"])
        delta_mrr = end_mrr - start_mrr
        growth_pct = (delta_mrr / start_mrr) if start_mrr > 0 else 0.0

        rows.append(
            {
                "scenario": scenario,
                "scenario_type": g.iloc[-1]["scenario_type"],
                "horizon_months": int(len(g)),
                "start_mrr": round(start_mrr, 2),
                "end_mrr": round(end_mrr, 2),
                "end_arr": round(end_arr, 2),
                "end_realized_arr_estimate": round(realized_arr, 2),
                "mrr_change": round(delta_mrr, 2),
                "mrr_growth_pct": round(growth_pct, 4),
                "avg_assumed_expansion_rate": round(float(g["assumed_expansion_rate"].mean()), 5),
                "avg_assumed_contraction_rate": round(float(g["assumed_contraction_rate"].mean()), 5),
                "avg_assumed_churn_rate": round(float(g["assumed_churn_rate"].mean()), 5),
                "avg_assumed_net_new_rate": round(float(g["assumed_net_new_rate"].mean()), 5),
            }
        )

    out = pd.DataFrame(rows)
    base_end = float(out.loc[out["scenario"] == "base_case", "end_mrr"].iloc[0])
    base_arr = float(out.loc[out["scenario"] == "base_case", "end_arr"].iloc[0])
    out["mrr_vs_base"] = (out["end_mrr"] - base_end).round(2)
    out["arr_vs_base"] = (out["end_arr"] - base_arr).round(2)

    return out.sort_values("scenario")


def compute_business_impacts(
    scoring: pd.DataFrame,
    health: pd.DataFrame,
    monthly_quality: pd.DataFrame,
    scenario_summary: pd.DataFrame,
) -> pd.DataFrame:
    active = scoring[scoring["current_mrr"] > 0].copy()
    total_mrr = float(active["current_mrr"].sum())

    high_risk = active[active["governance_priority_tier"].isin(["High", "Critical"])].copy()
    high_risk_mrr = float(high_risk["current_mrr"].sum())

    arr_at_risk = high_risk_mrr * 12

    # Expected contraction exposure from account-level contraction frequency.
    severity_df = monthly_quality[(monthly_quality["active_mrr"] > 0) & (monthly_quality["contraction_mrr"] > 0)].copy()
    if len(severity_df) > 0:
        contraction_severity = float((severity_df["contraction_mrr"] / severity_df["active_mrr"]).clip(0, 1).mean())
    else:
        contraction_severity = 0.12

    exposure_df = health[["customer_id", "current_mrr", "contraction_frequency"]].merge(
        active[["customer_id", "churn_risk_score"]], on="customer_id", how="left"
    )
    exposure_df["prob_contraction_6m"] = 1 - (1 - exposure_df["contraction_frequency"].clip(0, 1)) ** 6
    exposure_df["expected_contraction_loss_mrr_6m"] = (
        exposure_df["current_mrr"] * exposure_df["prob_contraction_6m"] * contraction_severity
    )
    expected_contraction_exposure_mrr_6m = float(exposure_df["expected_contraction_loss_mrr_6m"].sum())

    # Concentration-adjusted downside in high-risk cohort.
    high_risk_exp = exposure_df[exposure_df["customer_id"].isin(high_risk["customer_id"])].copy()
    high_risk_exp["p_churn_6m"] = clip_rate(0.0, 0.0, 1.0)
    high_risk_exp["p_churn_6m"] = (high_risk_exp["churn_risk_score"] / 100.0 * 0.40).clip(0, 0.60)

    high_risk_exp["expected_loss_mrr_6m"] = high_risk_exp["current_mrr"] * (
        high_risk_exp["p_churn_6m"]
        + (1 - high_risk_exp["p_churn_6m"]) * high_risk_exp["prob_contraction_6m"] * contraction_severity
    )

    base_expected_loss_highrisk_6m = float(high_risk_exp["expected_loss_mrr_6m"].sum())

    if high_risk_mrr > 0:
        top20_share_highrisk = float(high_risk.nlargest(20, "current_mrr")["current_mrr"].sum() / high_risk_mrr)
    else:
        top20_share_highrisk = 0.0
    concentration_multiplier = 1.0 + max(0.0, top20_share_highrisk - 0.50)
    concentration_adjusted_downside_mrr_6m = base_expected_loss_highrisk_6m * concentration_multiplier

    # Stress test on highest-priority accounts.
    top20_high_risk_mrr = float(high_risk.nlargest(20, "current_mrr")["current_mrr"].sum())
    top20_full_churn_arr_impact = top20_high_risk_mrr * 12
    top20_20pct_contraction_arr_impact = top20_high_risk_mrr * 0.20 * 12

    # Retention improvement opportunity from improvement case vs base case.
    base_arr = float(scenario_summary.loc[scenario_summary["scenario"] == "base_case", "end_arr"].iloc[0])
    improvement_arr = float(scenario_summary.loc[scenario_summary["scenario"] == "improvement_case", "end_arr"].iloc[0])
    retention_improvement_opportunity_arr = improvement_arr - base_arr

    impact_rows = [
        {
            "metric": "arr_at_risk",
            "value": round(arr_at_risk, 2),
            "unit": "ARR",
            "definition": "Current ARR associated with High/Critical governance priority accounts.",
        },
        {
            "metric": "expected_contraction_exposure_mrr_6m",
            "value": round(expected_contraction_exposure_mrr_6m, 2),
            "unit": "MRR",
            "definition": "Expected 6-month MRR loss from contraction frequency and historical contraction severity.",
        },
        {
            "metric": "concentration_adjusted_downside_mrr_6m",
            "value": round(concentration_adjusted_downside_mrr_6m, 2),
            "unit": "MRR",
            "definition": "Expected 6-month downside on high-risk cohort adjusted for concentration in top accounts.",
        },
        {
            "metric": "top20_high_risk_full_churn_arr_impact",
            "value": round(top20_full_churn_arr_impact, 2),
            "unit": "ARR",
            "definition": "Stress test: annualized ARR impact if top-20 high-risk accounts fully churn.",
        },
        {
            "metric": "top20_high_risk_20pct_contraction_arr_impact",
            "value": round(top20_20pct_contraction_arr_impact, 2),
            "unit": "ARR",
            "definition": "Stress test: annualized ARR impact if top-20 high-risk accounts contract by 20%.",
        },
        {
            "metric": "retention_improvement_opportunity_arr",
            "value": round(retention_improvement_opportunity_arr, 2),
            "unit": "ARR",
            "definition": "ARR uplift opportunity from improvement scenario vs base case at forecast horizon.",
        },
        {
            "metric": "high_risk_mrr_share",
            "value": round((high_risk_mrr / total_mrr) if total_mrr > 0 else 0.0, 4),
            "unit": "share",
            "definition": "Share of current MRR in High/Critical governance priority tier.",
        },
    ]

    return pd.DataFrame(impact_rows)


def write_narrative_report(
    output_path: Path,
    company_monthly: pd.DataFrame,
    baseline_rates: Dict[str, float],
    risk_adjusted_rates: Dict[str, float],
    scenario_summary: pd.DataFrame,
    impacts: pd.DataFrame,
    horizon_months: int,
) -> None:
    latest_month = company_monthly["month"].max()
    start_mrr = float(company_monthly.loc[company_monthly["month"] == latest_month, "mrr"].iloc[0])

    base_row = scenario_summary[scenario_summary["scenario"] == "base_case"].iloc[0]
    downside_row = scenario_summary[scenario_summary["scenario"] == "downside_case"].iloc[0]
    improvement_row = scenario_summary[scenario_summary["scenario"] == "improvement_case"].iloc[0]
    discount_row = scenario_summary[scenario_summary["scenario"] == "discount_discipline_improvement_case"].iloc[0]
    risk_row = scenario_summary[scenario_summary["scenario"] == "risk_adjusted_case"].iloc[0]

    def impact_val(metric: str) -> float:
        return float(impacts.loc[impacts["metric"] == metric, "value"].iloc[0])

    text = f"""# Forecasting and Scenario Analysis Memo

## Objective
Provide near-term, decision-useful commercial intelligence for MRR trajectory and downside exposure.

## Modeling Style
- Interpretable monthly rate-based model.
- Baseline rates derived from recency-weighted averages of the last 6 observed months.
- Forecast horizon: {horizon_months} months forward from {latest_month.date()}.
- No black-box machine learning; assumptions are explicit and scenario-adjustable.

## Baseline MRR Forecast
- Starting MRR: ${start_mrr:,.0f}
- Baseline forecast end-MRR ({horizon_months}m): ${base_row['end_mrr']:,.0f}
- Baseline MRR growth over horizon: {base_row['mrr_growth_pct']:.1%}

Baseline assumptions (monthly rates):
- Expansion rate: {baseline_rates['expansion_rate']:.2%}
- Contraction rate: {baseline_rates['contraction_rate']:.2%}
- Churn rate: {baseline_rates['churn_rate']:.2%}
- Net-new rate (residual): {baseline_rates['net_new_rate']:.2%}

## Risk-Adjusted Forecast
- Risk-adjusted end-MRR: ${risk_row['end_mrr']:,.0f}
- Difference vs base case: ${risk_row['mrr_vs_base']:,.0f} MRR

Risk-adjusted assumptions incorporate:
- Higher churn/contraction from high-risk concentration.
- Lower expansion and net-new rates due to fragility drag.

Risk-adjusted rates (monthly):
- Expansion rate: {risk_adjusted_rates['expansion_rate']:.2%}
- Contraction rate: {risk_adjusted_rates['contraction_rate']:.2%}
- Churn rate: {risk_adjusted_rates['churn_rate']:.2%}
- Net-new rate: {risk_adjusted_rates['net_new_rate']:.2%}

## Scenario Comparison
- Base case (reference): end-MRR ${base_row['end_mrr']:,.0f}
- Downside / fragile-growth: end-MRR ${downside_row['end_mrr']:,.0f} ({downside_row['mrr_vs_base']:,.0f} vs base)
- Improvement / healthy-growth: end-MRR ${improvement_row['end_mrr']:,.0f} ({improvement_row['mrr_vs_base']:,.0f} vs base)
- Discount-discipline improvement: end-MRR ${discount_row['end_mrr']:,.0f} ({discount_row['mrr_vs_base']:,.0f} vs base)

Interpretation:
- The fragile-growth downside quantifies sensitivity to churn/contraction concentration.
- The healthy-growth improvement quantifies value from retention and expansion-quality execution.
- Discount-discipline improvement may slightly moderate short-term expansion but improves realized ARR quality.

## Business Impact Estimates
- ARR at risk: ${impact_val('arr_at_risk'):,.0f}
- Expected contraction exposure (6m): ${impact_val('expected_contraction_exposure_mrr_6m'):,.0f} MRR
- Concentration-adjusted downside (6m): ${impact_val('concentration_adjusted_downside_mrr_6m'):,.0f} MRR
- Stress test: top-20 high-risk full churn impact: ${impact_val('top20_high_risk_full_churn_arr_impact'):,.0f} ARR
- Stress test: top-20 high-risk 20% contraction impact: ${impact_val('top20_high_risk_20pct_contraction_arr_impact'):,.0f} ARR
- Retention improvement opportunity (improvement vs base): ${impact_val('retention_improvement_opportunity_arr'):,.0f} ARR

## Assumptions by Scenario
- Base case: continuation of recent rate regime.
- Downside case: churn +50%, contraction +35%, expansion -20%, net-new -30%.
- Improvement case: churn -20%, contraction -15%, expansion +15%, net-new +15%.
- Discount-discipline improvement: churn -12%, contraction -10%, expansion -6%, net-new -3%, realized price index +2pts.

## Caveats
- This is an operating forecast, not a statistical confidence-interval model.
- Net-new rate is a residual term and can absorb unobserved commercial drivers.
- Scenario outputs are assumption-sensitive and should be reviewed monthly.
- Use this layer for decision support and prioritization, not single-point budgeting certainty.
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build interpretable forecasting and scenario analysis layer.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--output-dir", type=str, default="data/processed")
    parser.add_argument("--report-path", type=str, default="reports/forecasting_scenario_analysis.md")
    parser.add_argument("--horizon-months", type=int, default=6)
    parser.add_argument("--lookback-months", type=int, default=6)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    output_dir = (base_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = load_inputs(base_dir)

    company_monthly = build_company_monthly_frame(tables["monthly_quality"], tables["monthly_raw"])
    baseline_rates = estimate_baseline_rates(company_monthly, lookback_months=args.lookback_months)
    risk_adjusted_rates = estimate_risk_overlay_rates(baseline_rates, tables["scoring"])

    latest_month = company_monthly["month"].max()
    start_mrr = float(company_monthly.loc[company_monthly["month"] == latest_month, "mrr"].iloc[0])

    latest_quality_slice = tables["monthly_quality"][tables["monthly_quality"]["month"] == latest_month].copy()
    latest_quality_slice = latest_quality_slice[latest_quality_slice["active_mrr"] > 0]
    if len(latest_quality_slice) > 0 and latest_quality_slice["active_mrr"].sum() > 0:
        latest_realized_price_index = float(
            np.average(latest_quality_slice["realized_price_index"], weights=latest_quality_slice["active_mrr"])
        )
    else:
        latest_realized_price_index = float(
            tables["monthly_quality"].loc[tables["monthly_quality"]["month"] == latest_month, "realized_price_index"].mean()
        )

    trajectories = build_scenarios(
        latest_month=latest_month,
        start_mrr=start_mrr,
        baseline_rates=baseline_rates,
        risk_adjusted_rates=risk_adjusted_rates,
        latest_realized_price_index=latest_realized_price_index,
        horizon_months=args.horizon_months,
    )

    scenario_summary = summarize_scenarios(trajectories, start_mrr=start_mrr)
    impacts = compute_business_impacts(
        scoring=tables["scoring"],
        health=tables["health"],
        monthly_quality=tables["monthly_quality"],
        scenario_summary=scenario_summary,
    )

    # Required outputs
    trajectories[trajectories["scenario"] == "base_case"].to_csv(output_dir / "baseline_mrr_forecast.csv", index=False)
    trajectories[trajectories["scenario"] == "risk_adjusted_case"].to_csv(output_dir / "risk_adjusted_mrr_forecast.csv", index=False)
    trajectories.to_csv(output_dir / "scenario_mrr_trajectories.csv", index=False)
    scenario_summary.to_csv(output_dir / "mrr_scenario_table.csv", index=False)
    impacts.to_csv(output_dir / "commercial_risk_impact_estimates.csv", index=False)

    write_narrative_report(
        output_path=(base_dir / args.report_path).resolve(),
        company_monthly=company_monthly,
        baseline_rates=baseline_rates,
        risk_adjusted_rates=risk_adjusted_rates,
        scenario_summary=scenario_summary,
        impacts=impacts,
        horizon_months=args.horizon_months,
    )

    print("Forecasting/scenario layer build complete.")
    print(f"baseline_mrr_forecast: {len(trajectories[trajectories['scenario']=='base_case']):,} rows")
    print(f"risk_adjusted_mrr_forecast: {len(trajectories[trajectories['scenario']=='risk_adjusted_case']):,} rows")
    print(f"scenario_mrr_trajectories: {len(trajectories):,} rows")
    print(f"mrr_scenario_table: {len(scenario_summary):,} rows")
    print(f"commercial_risk_impact_estimates: {len(impacts):,} rows")


if __name__ == "__main__":
    main()

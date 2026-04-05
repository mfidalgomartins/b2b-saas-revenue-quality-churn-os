from __future__ import annotations

import argparse
import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DASHBOARD_VERSION = "2.0.0"
DATA_CONTRACT_VERSION = "dashboard_payload_v2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build executive offline dashboard with governed payload.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--output", type=str, default="outputs/dashboard/executive_dashboard.html")
    return parser.parse_args()


def _to_month(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m")


def _safe_float(value: Any, digits: int = 4) -> float:
    if pd.isna(value):
        return 0.0
    return round(float(value), digits)


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _encode_png_data_uri(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{data}"


def _load_latest_plan(customers: pd.DataFrame, subscriptions: pd.DataFrame, plans: pd.DataFrame) -> pd.DataFrame:
    latest_sub = (
        subscriptions.sort_values(["customer_id", "subscription_start_date"]).drop_duplicates("customer_id", keep="last")
    )
    latest_sub = latest_sub.merge(plans[["plan_id", "plan_name", "plan_tier", "billing_cycle"]], on="plan_id", how="left")
    latest_sub = latest_sub[
        [
            "customer_id",
            "plan_tier",
            "plan_name",
            "billing_cycle",
            "discount_pct",
            "seats_purchased",
            "contracted_mrr",
            "realized_mrr",
        ]
    ].copy()
    for col in ["plan_tier", "plan_name", "billing_cycle"]:
        latest_sub[col] = latest_sub[col].fillna("Unknown")
    return customers[["customer_id"]].merge(latest_sub, on="customer_id", how="left")


def _build_chart_catalog(base_dir: Path) -> list[dict[str, str]]:
    chart_specs = [
        (
            "chart_01_mrr_arr",
            "Recurring Revenue Is Growing, But Quality Guardrails Matter",
            "MRR/ARR trend over time",
            "01_mrr_arr_growth_trend.png",
            "executive_overview",
        ),
        (
            "chart_02_grr_nrr",
            "Retention Quality Holds Near Parity, With Limited Expansion Cushion",
            "Gross vs net retention trend",
            "02_grr_nrr_retention_trend.png",
            "retention_churn",
        ),
        (
            "chart_03_churn_segment",
            "Churn Burden Is Uneven Across Segments",
            "Logo churn by segment",
            "03_logo_churn_by_segment.png",
            "retention_churn",
        ),
        (
            "chart_04_concentration",
            "A Small Account Core Concentrates Disproportionate Revenue Exposure",
            "Revenue concentration curve",
            "04_revenue_concentration_curve.png",
            "revenue_quality",
        ),
        (
            "chart_05_discount_mix",
            "Discount Behavior Differs Materially by Segment, Channel, and Manager",
            "Average discount by segment/channel/manager",
            "05_average_discount_segment_channel_manager.png",
            "revenue_quality",
        ),
        (
            "chart_06_discount_share",
            "Discount-Dependent Revenue Share Stays Material",
            "Discounted revenue share trend",
            "06_discounted_revenue_share_trend.png",
            "revenue_quality",
        ),
        (
            "chart_07_churn_risk_dist",
            "Risk Distribution Is Skewed to Low, With a High-Impact Tail",
            "Churn risk score distribution",
            "07_churn_risk_score_distribution.png",
            "account_risk",
        ),
        (
            "chart_08_revenue_quality_dist",
            "Revenue Quality Scores Reveal Meaningful Fragility Pockets",
            "Revenue quality score distribution",
            "08_revenue_quality_score_distribution.png",
            "account_risk",
        ),
        (
            "chart_09_expansion_quality",
            "Expansion Quality Is Strongest in Specific Segments Only",
            "Expansion quality by segment",
            "09_expansion_quality_by_segment.png",
            "revenue_quality",
        ),
        (
            "chart_10_governance_priority_accounts",
            "Priority Queue Is Concentrated in a Small Set of Accounts",
            "Top accounts by governance priority",
            "10_top_accounts_governance_priority.png",
            "account_risk",
        ),
        (
            "chart_11_cohort_heatmap",
            "Cohort Retention Heatmap Highlights Uneven Durability",
            "Cohort retention heatmap",
            "11_cohort_retention_heatmap.png",
            "retention_churn",
        ),
        (
            "chart_12_discount_vs_risk",
            "Higher Discount Intensity Is Associated with Higher Risk",
            "Discount vs churn risk",
            "12_discount_vs_churn_risk.png",
            "account_risk",
        ),
        (
            "chart_13_payment_vs_risk",
            "Payment Delay Is a Leading Commercial Risk Signal",
            "Payment delay vs churn risk",
            "13_payment_delay_vs_churn_risk.png",
            "account_risk",
        ),
        (
            "chart_14_usage_vs_risk",
            "Usage Deterioration Aligns with Elevated Churn Risk",
            "Usage decline vs churn risk",
            "14_usage_decline_vs_churn_risk.png",
            "account_risk",
        ),
        (
            "chart_15_scenarios",
            "Fragile-Growth Case Compresses Near-Term MRR Trajectory",
            "Scenario comparison",
            "15_scenario_mrr_comparison.png",
            "scenario_forecast",
        ),
    ]

    charts_dir = base_dir / "outputs" / "charts"
    catalog: list[dict[str, str]] = []
    for chart_id, title, subtitle, filename, section in chart_specs:
        path = charts_dir / filename
        if not path.exists():
            continue
        catalog.append(
            {
                "chart_id": chart_id,
                "title": title,
                "subtitle": subtitle,
                "section": section,
                "filename": filename,
                "image_data_uri": _encode_png_data_uri(path),
            }
        )
    return catalog


def build_payload(base_dir: Path) -> dict[str, Any]:
    raw_dir = base_dir / "data" / "raw"
    processed_dir = base_dir / "data" / "processed"
    reports_dir = base_dir / "reports"

    customers = pd.read_csv(raw_dir / "customers.csv", parse_dates=["signup_date"])
    subscriptions = pd.read_csv(raw_dir / "subscriptions.csv", parse_dates=["subscription_start_date"])
    plans = pd.read_csv(raw_dir / "plans.csv")
    account_managers = pd.read_csv(raw_dir / "account_managers.csv")
    monthly_metrics = pd.read_csv(raw_dir / "monthly_account_metrics.csv", parse_dates=["month"])
    monthly_quality = pd.read_csv(processed_dir / "account_monthly_revenue_quality.csv", parse_dates=["month"])

    scoring = pd.read_csv(processed_dir / "account_scoring_model_output.csv")
    health = pd.read_csv(processed_dir / "customer_health_features.csv")
    manager_summary = pd.read_csv(processed_dir / "account_manager_summary.csv")
    scenario_table = pd.read_csv(processed_dir / "mrr_scenario_table.csv")
    scenario_traj = pd.read_csv(processed_dir / "scenario_mrr_trajectories.csv", parse_dates=["forecast_month"])
    risk_impact = pd.read_csv(processed_dir / "commercial_risk_impact_estimates.csv")
    cohort = pd.read_csv(processed_dir / "cohort_retention_summary.csv", parse_dates=["cohort_month"])

    analysis_metrics = json.loads((reports_dir / "main_business_analysis_metrics.json").read_text(encoding="utf-8"))
    validation_summary = json.loads((reports_dir / "formal_validation_summary.json").read_text(encoding="utf-8"))

    release_manifest_path = reports_dir / "release_manifest.json"
    release_manifest = {}
    if release_manifest_path.exists():
        release_manifest = json.loads(release_manifest_path.read_text(encoding="utf-8"))

    latest_plan = _load_latest_plan(customers, subscriptions, plans)

    accounts = customers.merge(
        scoring[
            [
                "customer_id",
                "current_mrr",
                "churn_risk_score",
                "churn_risk_tier",
                "churn_risk_main_driver",
                "revenue_quality_score",
                "discount_dependency_score",
                "expansion_quality_score",
                "governance_priority_score",
                "governance_priority_tier",
                "governance_main_driver",
                "recommended_action",
                "recommended_action_reason",
            ]
        ],
        on="customer_id",
        how="left",
    )
    accounts = accounts.merge(
        health[["customer_id", "trailing_3m_usage_avg", "trailing_3m_payment_delay_avg", "trailing_3m_discount_avg"]],
        on="customer_id",
        how="left",
    )
    accounts = accounts.merge(latest_plan, on="customer_id", how="left")

    accounts["signup_month"] = accounts["signup_date"].dt.strftime("%Y-%m")
    accounts["current_mrr"] = pd.to_numeric(accounts["current_mrr"], errors="coerce").fillna(0.0)

    for col in [
        "trailing_3m_usage_avg",
        "trailing_3m_payment_delay_avg",
        "trailing_3m_discount_avg",
        "churn_risk_score",
        "revenue_quality_score",
        "discount_dependency_score",
        "expansion_quality_score",
        "governance_priority_score",
        "discount_pct",
        "seats_purchased",
    ]:
        accounts[col] = pd.to_numeric(accounts[col], errors="coerce").fillna(0.0)

    for col in [
        "plan_tier",
        "plan_name",
        "billing_cycle",
        "churn_risk_tier",
        "governance_priority_tier",
        "churn_risk_main_driver",
        "governance_main_driver",
        "recommended_action",
    ]:
        accounts[col] = accounts[col].fillna("Unknown")

    manager_panel = manager_summary.merge(account_managers, on="account_manager_id", how="left").fillna("Unknown")

    sec1 = analysis_metrics.get("section1", {})
    sec2 = analysis_metrics.get("section2", {})
    sec5 = analysis_metrics.get("section5", {})

    critical_count = int((accounts["governance_priority_tier"] == "Critical").sum())

    official_kpis = {
        "current_mrr": _safe_float(sec1.get("mrr_end", accounts["current_mrr"].sum()), 2),
        "arr": _safe_float(sec1.get("arr_end", accounts["current_mrr"].sum() * 12.0), 2),
        "gross_retention": _safe_float(sec2.get("latest_grr", 0.0), 6),
        "net_retention": _safe_float(sec2.get("latest_nrr", 0.0), 6),
        "logo_churn": _safe_float(sec2.get("logo_churn_rate", 0.0), 6),
        "avg_discount": _safe_float(sec1.get("w_discount_end", 0.0), 6),
        "discounted_revenue_share": _safe_float(sec1.get("share_discounted_mrr_latest", 0.0), 6),
        "revenue_at_risk_mrr": _safe_float(sec5.get("at_risk_mrr_total", 0.0), 2),
        "critical_risk_account_count": critical_count,
    }

    alerts: list[dict[str, str]] = []
    if official_kpis["net_retention"] < 1.0:
        alerts.append(
            {
                "severity": "high",
                "label": "NRR Below 100%",
                "detail": f"Latest NRR is {_fmt_pct(official_kpis['net_retention'])}; expansion is not fully outpacing losses.",
            }
        )
    if official_kpis["discounted_revenue_share"] > 0.15:
        alerts.append(
            {
                "severity": "medium",
                "label": "High Discount Reliance",
                "detail": (
                    f"Discount-dependent revenue share is {_fmt_pct(official_kpis['discounted_revenue_share'])}; "
                    "review renewal pricing discipline."
                ),
            }
        )
    if official_kpis["critical_risk_account_count"] > 0:
        alerts.append(
            {
                "severity": "high",
                "label": "Critical Accounts In Portfolio",
                "detail": (
                    f"{official_kpis['critical_risk_account_count']} accounts are marked Critical governance priority "
                    "and require intervention ownership."
                ),
            }
        )
    if not alerts:
        alerts.append(
            {
                "severity": "low",
                "label": "No Immediate Alert Threshold Breaches",
                "detail": "Current governed thresholds show no critical breach, continue weekly monitoring discipline.",
            }
        )

    scenario_cards: list[dict[str, Any]] = []
    for _, row in scenario_table.sort_values("scenario").iterrows():
        scenario_cards.append(
            {
                "scenario": str(row["scenario"]),
                "scenario_type": str(row["scenario_type"]),
                "end_mrr": _safe_float(row["end_mrr"], 2),
                "end_arr": _safe_float(row["end_arr"], 2),
                "mrr_growth_pct": _safe_float(row["mrr_growth_pct"], 4),
                "mrr_vs_base": _safe_float(row["mrr_vs_base"], 2),
                "arr_vs_base": _safe_float(row["arr_vs_base"], 2),
            }
        )

    scenario_traj = scenario_traj.copy()
    scenario_traj["forecast_month"] = _to_month(scenario_traj["forecast_month"])
    scenario_trajectory = scenario_traj[
        ["scenario", "scenario_type", "forecast_month", "forecast_mrr"]
    ].to_dict(orient="records")

    risk_impact_rows = risk_impact[["metric", "value", "unit", "definition"]].copy()
    risk_impact_rows["value"] = risk_impact_rows["value"].apply(lambda x: _safe_float(x, 2))

    cohort_slice = cohort[["cohort_month", "segment", "region", "month_number", "net_retention_rate"]].copy()
    cohort_slice["cohort_month"] = _to_month(cohort_slice["cohort_month"])

    monthly_min = monthly_metrics["month"].min()
    monthly_max = monthly_metrics["month"].max()

    monthly_panel = monthly_quality.merge(
        monthly_metrics[["customer_id", "month", "churn_flag"]],
        on=["customer_id", "month"],
        how="left",
    )
    monthly_rollup = (
        monthly_panel.groupby("month", as_index=False)
        .agg(
            active_mrr=("active_mrr", "sum"),
            expansion_mrr=("expansion_mrr", "sum"),
            contraction_mrr=("contraction_mrr", "sum"),
            churned_mrr=("active_mrr", lambda s: float(s[monthly_panel.loc[s.index, "churn_flag"].fillna(0).astype(int) == 1].sum())),
            churn_events=("churn_flag", "sum"),
            account_rows=("customer_id", "size"),
            discounted_mrr=("active_mrr", lambda s: float(s[monthly_panel.loc[s.index, "discount_dependency_flag"].fillna(0).astype(int) == 1].sum())),
        )
        .sort_values("month")
        .reset_index(drop=True)
    )
    monthly_rollup["month_label"] = _to_month(monthly_rollup["month"])
    monthly_rollup["arr"] = monthly_rollup["active_mrr"] * 12.0
    monthly_rollup["logo_churn_rate"] = monthly_rollup["churn_events"] / monthly_rollup["account_rows"].clip(lower=1)
    monthly_rollup["discounted_share"] = monthly_rollup["discounted_mrr"] / monthly_rollup["active_mrr"].clip(lower=1)
    monthly_rollup["starting_mrr"] = monthly_rollup["active_mrr"].shift(1)
    valid_base = monthly_rollup["starting_mrr"] > 0
    monthly_rollup["grr"] = 0.0
    monthly_rollup["nrr"] = 0.0
    monthly_rollup.loc[valid_base, "grr"] = (
        (monthly_rollup.loc[valid_base, "starting_mrr"] - monthly_rollup.loc[valid_base, "contraction_mrr"] - monthly_rollup.loc[valid_base, "churned_mrr"])
        / monthly_rollup.loc[valid_base, "starting_mrr"]
    )
    monthly_rollup.loc[valid_base, "nrr"] = (
        (
            monthly_rollup.loc[valid_base, "starting_mrr"]
            + monthly_rollup.loc[valid_base, "expansion_mrr"]
            - monthly_rollup.loc[valid_base, "contraction_mrr"]
            - monthly_rollup.loc[valid_base, "churned_mrr"]
        )
        / monthly_rollup.loc[valid_base, "starting_mrr"]
    )
    monthly_rollup = monthly_rollup.fillna(0.0)
    monthly_summary = [
        {
            "month": str(r.month_label),
            "mrr": _safe_float(r.active_mrr, 2),
            "arr": _safe_float(r.arr, 2),
            "logo_churn_rate": _safe_float(r.logo_churn_rate, 6),
            "discounted_share": _safe_float(r.discounted_share, 6),
            "grr": _safe_float(r.grr, 6),
            "nrr": _safe_float(r.nrr, 6),
        }
        for r in monthly_rollup.itertuples(index=False)
    ]
    monthly_compact_index = {
        "customer_id": 0,
        "month": 1,
        "active_mrr": 2,
        "expansion_mrr": 3,
        "contraction_mrr": 4,
        "discount_dependency_flag": 5,
        "churn_flag": 6,
    }
    monthly_compact_rows: list[list[Any]] = []
    monthly_panel_copy = monthly_panel.copy()
    monthly_panel_copy["month_label"] = _to_month(monthly_panel_copy["month"])
    for row in monthly_panel_copy[
        [
            "customer_id",
            "month_label",
            "active_mrr",
            "expansion_mrr",
            "contraction_mrr",
            "discount_dependency_flag",
            "churn_flag",
        ]
    ].itertuples(index=False, name=None):
        (
            customer_id,
            month_label,
            active_mrr,
            expansion_mrr,
            contraction_mrr,
            discount_dependency_flag,
            churn_flag,
        ) = row
        monthly_compact_rows.append(
            [
                str(customer_id),
                str(month_label),
                _safe_float(active_mrr, 2),
                _safe_float(expansion_mrr, 2),
                _safe_float(contraction_mrr, 2),
                int(discount_dependency_flag) if not pd.isna(discount_dependency_flag) else 0,
                int(churn_flag) if not pd.isna(churn_flag) else 0,
            ]
        )

    filter_options = {
        "regions": sorted(accounts["region"].dropna().astype(str).unique().tolist()),
        "segments": sorted(accounts["segment"].dropna().astype(str).unique().tolist()),
        "industries": sorted(accounts["industry"].dropna().astype(str).unique().tolist()),
        "plan_tiers": sorted(accounts["plan_tier"].dropna().astype(str).unique().tolist()),
        "channels": sorted(accounts["acquisition_channel"].dropna().astype(str).unique().tolist()),
        "account_managers": sorted(accounts["account_manager_id"].dropna().astype(str).unique().tolist()),
        "risk_tiers": ["Low", "Moderate", "High", "Critical", "Unknown"],
        "signup_months": sorted(accounts["signup_month"].dropna().astype(str).unique().tolist()),
    }

    methodology = {
        "glossary": [
            {
                "term": "MRR",
                "definition": "Monthly recurring revenue recognized from active subscriptions in each month.",
            },
            {
                "term": "ARR",
                "definition": "Annualized recurring run-rate, computed as MRR multiplied by 12.",
            },
            {
                "term": "Gross Retention (GRR)",
                "definition": "Retention excluding expansion impact.",
            },
            {
                "term": "Net Retention (NRR)",
                "definition": "Retention including expansion and contraction impact.",
            },
            {
                "term": "Governance Priority",
                "definition": "Composite urgency signal blending churn risk, quality weakness, and exposure concentration.",
            },
        ],
        "scoring_logic": [
            "churn_risk_score (0-100): higher means higher forward churn exposure.",
            "revenue_quality_score (0-100): higher means healthier pricing/retention quality.",
            "discount_dependency_score (0-100): higher means greater discount-driven fragility.",
            "expansion_quality_score (0-100): higher means more sustainable expansion pattern.",
            "governance_priority_score (0-100): higher means stronger intervention urgency.",
        ],
        "assumptions": [
            "Trend visuals are refreshed from the latest monthly pipeline run.",
            "Interactive filters update current account diagnostics, not historical restatements.",
            "Scenario outputs are decision-support ranges and should not be interpreted as deterministic forecasts.",
        ],
        "validation_notes": [
            "Quality checks reconcile revenue, retention, discount, scoring, and scenario outputs before publication.",
            "Results are intended for prioritization and decision support, not causal proof.",
        ],
        "caveats": [
            "Associations shown are correlational diagnostics, not causal proof.",
            "Manager comparisons can reflect portfolio mix effects.",
            "Data is synthetic and intended to emulate commercial behavior patterns.",
        ],
    }

    source_map = {
        "executive_overview": [
            "reports/main_business_analysis_metrics.json",
            "data/processed/account_scoring_model_output.csv",
            "reports/formal_validation_summary.json",
        ],
        "revenue_quality": [
            "outputs/charts/01_mrr_arr_growth_trend.png",
            "outputs/charts/04_revenue_concentration_curve.png",
            "outputs/charts/05_average_discount_segment_channel_manager.png",
            "outputs/charts/06_discounted_revenue_share_trend.png",
            "outputs/charts/09_expansion_quality_by_segment.png",
        ],
        "retention_churn": [
            "outputs/charts/02_grr_nrr_retention_trend.png",
            "outputs/charts/03_logo_churn_by_segment.png",
            "outputs/charts/11_cohort_retention_heatmap.png",
        ],
        "account_risk": [
            "data/processed/account_scoring_model_output.csv",
            "outputs/charts/07_churn_risk_score_distribution.png",
            "outputs/charts/08_revenue_quality_score_distribution.png",
            "outputs/charts/10_top_accounts_governance_priority.png",
            "outputs/charts/12_discount_vs_churn_risk.png",
            "outputs/charts/13_payment_delay_vs_churn_risk.png",
            "outputs/charts/14_usage_decline_vs_churn_risk.png",
        ],
        "portfolio_manager": [
            "data/processed/account_manager_summary.csv",
            "data/raw/account_managers.csv",
        ],
        "scenario_forecast": [
            "data/processed/mrr_scenario_table.csv",
            "data/processed/scenario_mrr_trajectories.csv",
            "data/processed/commercial_risk_impact_estimates.csv",
            "outputs/charts/15_scenario_mrr_comparison.png",
        ],
    }

    chart_catalog = _build_chart_catalog(base_dir)

    executive_narrative = (
        "Topline recurring revenue is growing, but governance-relevant quality signals indicate selective fragility. "
        "Discount-reliant expansion and concentrated high-risk exposure should be managed as first-order risks, "
        "not secondary analytics concerns."
    )

    data_coverage = {
        "month_start": monthly_min.strftime("%Y-%m") if pd.notna(monthly_min) else "",
        "month_end": monthly_max.strftime("%Y-%m") if pd.notna(monthly_max) else "",
        "signup_start": accounts["signup_month"].min(),
        "signup_end": accounts["signup_month"].max(),
    }

    payload = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "dashboard_version": DASHBOARD_VERSION,
            "data_contract_version": DATA_CONTRACT_VERSION,
            "validation_overall": validation_summary.get("overall_assessment", ""),
            "validation_readiness_tier": validation_summary.get("readiness", {}).get("tier", ""),
            "release_timestamp_utc": release_manifest.get("release_timestamp_utc", ""),
            "data_coverage": data_coverage,
            "row_counts": {
                "accounts": int(accounts.shape[0]),
                "manager_rows": int(manager_panel.shape[0]),
                "scenario_rows": int(scenario_table.shape[0]),
                "charts_embedded": int(len(chart_catalog)),
            },
        },
        "official_kpis": official_kpis,
        "alerts": alerts,
        "executive_narrative": executive_narrative,
        "filters": filter_options,
        "accounts": accounts[
            [
                "customer_id",
                "signup_month",
                "region",
                "segment",
                "industry",
                "acquisition_channel",
                "account_manager_id",
                "plan_tier",
                "plan_name",
                "billing_cycle",
                "current_mrr",
                "churn_risk_score",
                "churn_risk_tier",
                "churn_risk_main_driver",
                "revenue_quality_score",
                "discount_dependency_score",
                "expansion_quality_score",
                "governance_priority_score",
                "governance_priority_tier",
                "governance_main_driver",
                "recommended_action",
                "recommended_action_reason",
                "trailing_3m_usage_avg",
                "trailing_3m_payment_delay_avg",
                "trailing_3m_discount_avg",
                "discount_pct",
                "seats_purchased",
            ]
        ].to_dict(orient="records"),
        "manager_panel": manager_panel.to_dict(orient="records"),
        "scenario_cards": scenario_cards,
        "scenario_trajectory": scenario_trajectory,
        "risk_impact": risk_impact_rows.to_dict(orient="records"),
        "cohort_slice": cohort_slice.to_dict(orient="records"),
        "monthly_summary": monthly_summary,
        "monthly_compact_index": monthly_compact_index,
        "monthly_compact_rows": monthly_compact_rows,
        "chart_catalog": chart_catalog,
        "methodology": methodology,
        "source_map": source_map,
    }
    return payload


def build_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")

    html_template = """<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"UTF-8\" />
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
<title>B2B SaaS Revenue Quality & Churn Early Warning Operating Dashboard</title>
<style>
:root {{
  --bg: #f4f7fb;
  --panel: #ffffff;
  --panel-soft: #f8fbff;
  --ink: #1a2633;
  --muted: #5a6778;
  --line: #d8e2ee;
  --accent: #1f5db8;
  --accent-soft: #e8f0ff;
  --green: #1f7a53;
  --orange: #be6b0d;
  --red: #b3342b;
  --header-bg: rgba(244, 247, 251, 0.95);
  --chip-bg: #ffffff;
  --chip-ink: #314256;
  --input-bg: #ffffff;
  --input-border: #c9d6e5;
  --btn-bg: #ffffff;
  --btn-border: #c2d1e2;
  --btn-ink: #1e2b3d;
  --btn-hover: #f2f6fc;
  --tab-bg: #eaf1fa;
  --tab-ink: #2e4057;
  --tab-active-bg: #ffffff;
  --tab-active-ink: #193a66;
  --kpi-bg: #fbfdff;
  --chart-card-bg: #ffffff;
  --chart-image-bg: #f8fbff;
  --chart-image-border: #e0e8f3;
  --chart-frame-bg: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
  --tooltip-bg: rgba(18, 33, 56, 0.95);
  --tooltip-ink: #ffffff;
  --insight-bg: #f3f8ff;
  --insight-border: #d7e5f6;
  --insight-ink: #34485f;
  --table-head-bg: #f5f9ff;
  --table-row-hover: #f8fbff;
  --modal-overlay: rgba(11, 18, 30, 0.76);
  --modal-bg: #ffffff;
  --modal-line: #d4deea;
  --modal-body-bg: #f7fbff;
  --empty-bg: #f7faff;
  --empty-line: #c9d5e4;
  --empty-ink: #456;
  --shadow: 0 10px 28px rgba(16, 24, 40, 0.08);
}}
[data-theme="dark"] {{
  --bg: #0f1724;
  --panel: #152133;
  --panel-soft: #18273b;
  --ink: #e6edf7;
  --muted: #a8b7cb;
  --line: #30425b;
  --accent: #74a8ff;
  --accent-soft: #213858;
  --header-bg: rgba(15, 23, 36, 0.95);
  --chip-bg: #1a2a3f;
  --chip-ink: #d4e1f2;
  --input-bg: #162436;
  --input-border: #3a4f6c;
  --btn-bg: #1a2a3f;
  --btn-border: #3a4f6c;
  --btn-ink: #e4edf9;
  --btn-hover: #22344d;
  --tab-bg: #1a2b42;
  --tab-ink: #c7d7ec;
  --tab-active-bg: #2a4161;
  --tab-active-ink: #f1f6ff;
  --kpi-bg: #182638;
  --chart-card-bg: #162436;
  --chart-image-bg: #f8fbff;
  --chart-image-border: #cfdae9;
  --chart-frame-bg: linear-gradient(180deg, #fdfefe 0%, #f6faff 100%);
  --tooltip-bg: rgba(6, 11, 18, 0.96);
  --tooltip-ink: #f4f7fb;
  --insight-bg: #1c2f47;
  --insight-border: #355170;
  --insight-ink: #d8e5f4;
  --table-head-bg: #1d2f47;
  --table-row-hover: #21344f;
  --modal-overlay: rgba(5, 10, 16, 0.85);
  --modal-bg: #122033;
  --modal-line: #2f4664;
  --modal-body-bg: #0e1928;
  --empty-bg: #162437;
  --empty-line: #3a5272;
  --empty-ink: #ccd9ea;
  --shadow: 0 12px 30px rgba(5, 10, 18, 0.42);
}}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; background: var(--bg); color: var(--ink); font-family: "IBM Plex Sans", "Segoe UI", Arial, sans-serif; }}
body {{ line-height: 1.35; transition: background-color 0.25s ease, color 0.25s ease; }}
body[data-theme="light"] {{ color-scheme: light; }}
body[data-theme="dark"] {{ color-scheme: dark; }}

header {{
  position: sticky;
  top: 0;
  z-index: 60;
  background: var(--header-bg);
  backdrop-filter: blur(6px);
  border-bottom: 1px solid var(--line);
}}
.header-inner {{
  max-width: 1500px;
  margin: 0 auto;
  padding: 14px 20px 12px;
  display: grid;
  gap: 10px;
}}
.title-row {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 14px;
  flex-wrap: wrap;
}}
.title-block h1 {{ margin: 0; font-size: 1.25rem; font-weight: 700; }}
.subtitle {{ margin-top: 4px; color: var(--muted); font-size: 0.88rem; }}
.meta-row {{ display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }}
.chip {{
  border: 1px solid var(--line);
  background: var(--chip-bg);
  border-radius: 999px;
  font-size: 0.75rem;
  color: var(--chip-ink);
  padding: 3px 10px;
  white-space: nowrap;
}}
.chip.alert-high {{ border-color: #f4b8b2; background: #fff3f2; color: #8e2f29; }}
.chip.alert-medium {{ border-color: #f5d6b3; background: #fff8ef; color: #8a4f08; }}
.theme-btn {{
  border-radius: 999px;
  padding: 5px 12px;
  font-size: 0.76rem;
  font-weight: 700;
  border: 1px solid var(--btn-border);
  background: var(--btn-bg);
  color: var(--btn-ink);
}}
.theme-btn:hover {{ background: var(--btn-hover); }}

.filters {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
  gap: 8px;
}}
.filter {{ display: grid; gap: 4px; }}
.filter label {{ font-size: 0.7rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.03em; font-weight: 700; }}
.filter select, .filter input {{
  width: 100%;
  border: 1px solid var(--input-border);
  border-radius: 8px;
  padding: 7px 8px;
  font-size: 0.85rem;
  background: var(--input-bg);
  color: var(--ink);
}}
.filter input::placeholder {{ color: var(--muted); }}
.filter-actions {{ display: flex; gap: 8px; align-items: end; }}
button {{
  border: 1px solid var(--btn-border);
  background: var(--btn-bg);
  color: var(--btn-ink);
  border-radius: 8px;
  padding: 8px 11px;
  font-size: 0.82rem;
  cursor: pointer;
}}
button.primary {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
button:hover {{ background: var(--btn-hover); }}
.button-link {{
  border: 1px solid var(--btn-border);
  background: var(--btn-bg);
  color: var(--btn-ink);
  border-radius: 8px;
  padding: 8px 11px;
  font-size: 0.82rem;
  cursor: pointer;
  text-decoration: none;
  display: inline-flex;
  align-items: center;
}}
.button-link:hover {{ background: var(--btn-hover); }}

main {{
  max-width: 1500px;
  margin: 0 auto;
  padding: 16px 20px 28px;
}}
.tab-nav {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 10px;
}}
.tab-btn {{
  border: 1px solid var(--line);
  background: var(--tab-bg);
  color: var(--tab-ink);
  border-radius: 8px;
  padding: 8px 12px;
  font-size: 0.84rem;
  font-weight: 600;
}}
.tab-btn.active {{ background: var(--tab-active-bg); color: var(--tab-active-ink); border-color: var(--line); }}
.section {{
  display: none;
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 12px;
  box-shadow: var(--shadow);
  padding: 14px;
}}
.section.active {{ display: block; }}
.section-head {{ display: flex; justify-content: space-between; gap: 10px; align-items: baseline; flex-wrap: wrap; margin-bottom: 10px; }}
.section-head h2 {{ margin: 0; font-size: 1.08rem; }}
.section-note {{ color: var(--muted); font-size: 0.84rem; }}

.chart-toolbar {{
  margin-bottom: 10px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}}
.chart-density-controls {{ display: flex; gap: 6px; flex-wrap: wrap; }}
.chart-density-btn.active {{
  background: var(--accent);
  color: #fff;
  border-color: var(--accent);
}}

.kpi-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 9px;
}}
.kpi-card {{
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--kpi-bg);
  padding: 10px;
}}
.kpi-label {{ font-size: 0.73rem; text-transform: uppercase; letter-spacing: 0.03em; color: var(--muted); font-weight: 700; }}
.kpi-value {{ margin-top: 5px; font-size: 1.15rem; font-weight: 700; color: var(--ink); }}
.kpi-foot {{ margin-top: 3px; font-size: 0.75rem; color: var(--muted); }}

.panel-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 10px;
}}
.panel {{
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 10px;
  background: var(--panel-soft);
  min-width: 0;
}}
.panel h3 {{ margin: 0 0 8px; font-size: 0.93rem; }}
.panel p {{ margin: 0; color: var(--ink); font-size: 0.86rem; }}
.alert-list {{ display: grid; gap: 7px; }}
.alert-item {{ border: 1px solid var(--line); border-radius: 8px; padding: 8px; background: var(--panel); }}
.alert-item.high {{ border-color: #efb6b2; background: #fff4f3; }}
.alert-item.medium {{ border-color: #f0d4b6; background: #fff8ef; }}
.alert-item.low {{ border-color: #cfe2f5; background: #f4f8ff; }}
.alert-title {{ font-size: 0.82rem; font-weight: 700; }}
.alert-detail {{ margin-top: 2px; font-size: 0.82rem; color: #44556a; }}
[data-theme="dark"] .alert-detail {{ color: #dbe6f6; }}
[data-theme="dark"] .alert-item.high {{ border-color: #9a4e4a; background: #442724; color: #ffd9d4; }}
[data-theme="dark"] .alert-item.medium {{ border-color: #8f6c3e; background: #43331e; color: #ffe6c2; }}
[data-theme="dark"] .alert-item.low {{ border-color: #4b6489; background: #25374f; color: #dbe9ff; }}

.chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(620px, 1fr)); gap: 12px; }}
.chart-card {{ border: 1px solid var(--line); border-radius: 10px; background: var(--chart-card-bg); padding: 12px; min-width: 0; }}
.chart-title {{ font-size: 0.89rem; font-weight: 700; margin-bottom: 2px; }}
.chart-subtitle {{ font-size: 0.78rem; color: var(--muted); margin-bottom: 7px; }}
.chart-actions {{ display: flex; justify-content: flex-end; gap: 6px; margin-bottom: 8px; }}
.chart-card img {{
  width: 100%;
  height: auto;
  min-height: 420px;
  display: block;
  border-radius: 8px;
  border: 1px solid var(--chart-image-border);
  background: var(--chart-image-bg);
  cursor: zoom-in;
}}
body.density-compact .chart-grid {{ grid-template-columns: repeat(auto-fit, minmax(430px, 1fr)); }}
body.density-compact .chart-card img {{ min-height: 340px; }}
body.density-presentation .chart-grid {{ grid-template-columns: 1fr; }}
body.density-presentation .chart-card img {{ min-height: 620px; }}

.chart-modal {{
  position: fixed;
  inset: 0;
  background: var(--modal-overlay);
  display: none;
  align-items: center;
  justify-content: center;
  z-index: 90;
  padding: 16px;
}}
.chart-modal.active {{ display: flex; }}
.chart-modal-dialog {{
  width: min(1600px, 98vw);
  height: min(94vh, 980px);
  background: var(--modal-bg);
  border-radius: 12px;
  border: 1px solid var(--modal-line);
  box-shadow: 0 14px 50px rgba(15, 23, 42, 0.34);
  display: grid;
  grid-template-rows: auto 1fr auto;
  overflow: hidden;
}}
.chart-modal-head {{
  border-bottom: 1px solid var(--modal-line);
  padding: 10px 12px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
}}
.chart-modal-title {{ font-size: 0.94rem; font-weight: 700; color: var(--ink); }}
.chart-modal-controls {{ display: flex; gap: 6px; flex-wrap: wrap; }}
.chart-modal-body {{
  background: var(--modal-body-bg);
  overflow: hidden;
  position: relative;
}}
.chart-modal-stage {{
  width: 100%;
  height: 100%;
  overflow: hidden;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: grab;
}}
.chart-modal-stage.dragging {{ cursor: grabbing; }}
.chart-modal-image {{
  max-width: none;
  max-height: none;
  transform-origin: center center;
  user-select: none;
  -webkit-user-drag: none;
}
.chart-modal-foot {{
  border-top: 1px solid var(--modal-line);
  padding: 8px 12px;
  font-size: 0.81rem;
  color: var(--muted);
  display: flex;
  justify-content: space-between;
  gap: 8px;
  flex-wrap: wrap;
}

.interactive-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
  gap: 12px;
  margin-top: 10px;
}}
.svg-chart-card {{
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 12px;
  background: var(--panel-soft);
  min-width: 0;
}}
.svg-chart-head {{
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 8px;
  margin-bottom: 8px;
}}
.svg-chart-title {{ font-size: 0.9rem; font-weight: 700; }}
.svg-chart-subtitle {{ font-size: 0.76rem; color: var(--muted); }}
.chart-svg-wrap {{
  position: relative;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--chart-frame-bg);
  overflow: hidden;
}}
.chart-svg {{
  width: 100%;
  height: 360px;
  display: block;
}}
.svg-tooltip {{
  position: absolute;
  pointer-events: none;
  background: var(--tooltip-bg);
  color: var(--tooltip-ink);
  padding: 6px 8px;
  border-radius: 7px;
  font-size: 0.74rem;
  max-width: 240px;
  display: none;
  z-index: 4;
}}
.chart-insight {{
  margin-top: 8px;
  font-size: 0.8rem;
  color: var(--insight-ink);
  background: var(--insight-bg);
  border: 1px solid var(--insight-border);
  border-radius: 8px;
  padding: 7px 8px;
}

.table-wrap {{ border: 1px solid var(--line); border-radius: 10px; overflow: auto; background: var(--panel); }}
table {{ border-collapse: collapse; width: 100%; min-width: 900px; }}
th, td {{ border-bottom: 1px solid var(--line); padding: 8px 9px; text-align: left; font-size: 0.8rem; white-space: nowrap; }}
th {{ background: var(--table-head-bg); position: sticky; top: 0; z-index: 1; cursor: pointer; }}
tr:hover td {{ background: var(--table-row-hover); }}
.table-meta {{ margin-top: 6px; color: var(--muted); font-size: 0.78rem; display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; }}
.pager {{ display: flex; gap: 6px; align-items: center; }}

.risk-stack {{ display: grid; gap: 6px; }}
.risk-bar {{ display: grid; grid-template-columns: 110px 1fr 60px; gap: 8px; align-items: center; font-size: 0.8rem; }}
.risk-track {{ height: 10px; border-radius: 999px; background: var(--accent-soft); overflow: hidden; }}
.risk-fill {{ height: 10px; }}
.fill-low {{ background: #4f8dd9; }}
.fill-moderate {{ background: #32a07f; }}
.fill-high {{ background: #d99124; }}
.fill-critical {{ background: #c74b40; }}

.drawer-btn {{ margin-left: auto; }}
.methodology-drawer {{ display: none; border: 1px solid var(--line); border-radius: 10px; background: var(--panel-soft); padding: 10px; margin-top: 10px; }}
.methodology-drawer.active {{ display: block; }}
.methodology-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 10px; }}
.methodology-grid ul {{ margin: 0; padding-left: 18px; }}
.methodology-grid li {{ margin: 4px 0; font-size: 0.84rem; color: var(--ink); }}
.methodology-grid h4 {{ margin: 0 0 6px; font-size: 0.86rem; }}
.technical-details {{ border: 1px solid var(--line); border-radius: 10px; background: var(--panel-soft); padding: 8px 10px; }}
.technical-details > summary {{ cursor: pointer; font-size: 0.84rem; color: var(--muted); font-weight: 600; }}
.technical-details[open] > summary {{ margin-bottom: 8px; }}

.empty-state {{
  border: 1px dashed var(--empty-line);
  border-radius: 9px;
  background: var(--empty-bg);
  padding: 12px;
  color: var(--empty-ink);
  font-size: 0.84rem;
}}

@media (max-width: 980px) {{
  .header-inner, main {{ padding-left: 12px; padding-right: 12px; }}
  .tab-btn {{ flex: 1 1 auto; text-align: center; }}
  table {{ min-width: 760px; }}
  .chart-grid {{ grid-template-columns: 1fr; }}
  .interactive-grid {{ grid-template-columns: 1fr; }}
  .chart-svg {{ height: 280px; }}
  .chart-card img {{ min-height: 300px; }}
  .chart-modal-dialog {{ width: 100%; height: 96vh; }}
}}
</style>
</head>
<body>
<header>
  <div class=\"header-inner\">
    <div class=\"title-row\">
      <div class=\"title-block\">
        <h1>B2B SaaS Revenue Quality & Churn Early Warning Operating Dashboard</h1>
        <div class=\"subtitle\">Executive operating view for commercial leadership, finance, RevOps, CS, and strategy.</div>
      </div>
      <div class=\"meta-row\">
        <span class=\"chip info\" id=\"filterStateChip\">Filtered: all accounts</span>
        <button id=\"btn_theme\" class=\"theme-btn\" type=\"button\" aria-pressed=\"false\">Dark mode</button>
      </div>
    </div>

    <div class=\"filters\">
      <div class=\"filter\"><label>Region</label><select id=\"f_region\"></select></div>
      <div class=\"filter\"><label>Segment</label><select id=\"f_segment\"></select></div>
      <div class=\"filter\"><label>Industry</label><select id=\"f_industry\"></select></div>
      <div class=\"filter\"><label>Plan Tier</label><select id=\"f_plan\"></select></div>
      <div class=\"filter\"><label>Acquisition Channel</label><select id=\"f_channel\"></select></div>
      <div class=\"filter\"><label>Account Manager</label><select id=\"f_manager\"></select></div>
      <div class=\"filter\"><label>Risk Tier</label><select id=\"f_risk\"></select></div>
      <div class=\"filter\"><label>Signup Start</label><select id=\"f_start\"></select></div>
      <div class=\"filter\"><label>Signup End</label><select id=\"f_end\"></select></div>
      <div class=\"filter\"><label>Account Search</label><input id=\"f_search\" type=\"text\" placeholder=\"customer id / industry / action\" /></div>
      <div class=\"filter-actions\">
        <button id=\"btn_reset\">Reset Filters</button>
        <button id=\"btn_method\" class=\"primary\">Methodology</button>
      </div>
    </div>
  </div>
</header>

<main>
  <nav class=\"tab-nav\" id=\"tabs\">
    <button class=\"tab-btn active\" data-tab=\"executive\">Executive Overview</button>
    <button class=\"tab-btn\" data-tab=\"revenue\">Revenue Quality</button>
    <button class=\"tab-btn\" data-tab=\"retention\">Retention & Churn</button>
    <button class=\"tab-btn\" data-tab=\"risk\">Account Risk</button>
    <button class=\"tab-btn\" data-tab=\"portfolio\">Portfolio / Manager</button>
    <button class=\"tab-btn\" data-tab=\"scenario\">Scenario & Forecast</button>
    <button class=\"tab-btn\" data-tab=\"method\">Methodology & Definitions</button>
  </nav>

  <div class=\"panel chart-toolbar\">
    <div class=\"section-note\">Interactive view: click charts to expand, zoom, and navigate presentation mode.</div>
    <div class=\"chart-density-controls\">
      <button class=\"chart-density-btn\" data-density=\"compact\" id=\"densityCompact\">Compact</button>
      <button class=\"chart-density-btn active\" data-density=\"comfortable\" id=\"densityComfortable\">Comfortable</button>
      <button class=\"chart-density-btn\" data-density=\"presentation\" id=\"densityPresentation\">Presentation</button>
    </div>
  </div>

  <section class=\"section active\" id=\"section_executive\">
    <div class=\"section-head\">
      <h2>Executive Overview</h2>
      <div class=\"section-note\">Executive cards and insights update instantly with filters.</div>
    </div>
    <div class=\"kpi-grid\" id=\"officialKpis\"></div>

    <div class=\"panel-grid\" style=\"margin-top:10px;\">
      <div class=\"panel\">
        <h3>Executive Narrative</h3>
        <p id=\"executiveNarrative\"></p>
      </div>
      <div class=\"panel\">
        <h3>Key Alerts</h3>
        <div id=\"alerts\" class=\"alert-list\"></div>
      </div>
    </div>

    <div class=\"interactive-grid\">
      <article class=\"svg-chart-card\">
        <div class=\"svg-chart-head\">
          <div class=\"svg-chart-title\">Interactive MRR / ARR Momentum</div>
          <div class=\"svg-chart-subtitle\">Click a point to inspect exact values</div>
        </div>
        <div class=\"chart-svg-wrap\">
          <svg id=\"chartInteractiveMrrArr\" class=\"chart-svg\"></svg>
          <div id=\"tooltipInteractiveMrrArr\" class=\"svg-tooltip\"></div>
        </div>
        <div id=\"insightInteractiveMrrArr\" class=\"chart-insight\">Select a point to inspect month-level detail.</div>
      </article>
      <article class=\"svg-chart-card\">
        <div class=\"svg-chart-head\">
          <div class=\"svg-chart-title\">Interactive Retention Quality</div>
          <div class=\"svg-chart-subtitle\">GRR vs NRR, month by month</div>
        </div>
        <div class=\"chart-svg-wrap\">
          <svg id=\"chartInteractiveRetention\" class=\"chart-svg\"></svg>
          <div id=\"tooltipInteractiveRetention\" class=\"svg-tooltip\"></div>
        </div>
        <div id=\"insightInteractiveRetention\" class=\"chart-insight\">Click any month to inspect retention spread.</div>
      </article>
      <article class=\"svg-chart-card\">
        <div class=\"svg-chart-head\">
          <div class=\"svg-chart-title\">Interactive Discount vs Churn Lens</div>
          <div class=\"svg-chart-subtitle\">Discounted share and logo churn trend</div>
        </div>
        <div class=\"chart-svg-wrap\">
          <svg id=\"chartInteractiveDiscountChurn\" class=\"chart-svg\"></svg>
          <div id=\"tooltipInteractiveDiscountChurn\" class=\"svg-tooltip\"></div>
        </div>
        <div id=\"insightInteractiveDiscountChurn\" class=\"chart-insight\">Use this to spot months with simultaneous pricing pressure and churn stress.</div>
      </article>
      <article class=\"svg-chart-card\">
        <div class=\"svg-chart-head\">
          <div class=\"svg-chart-title\">Interactive Slice Drilldown</div>
          <div class=\"svg-chart-subtitle\">Click bars to apply filters instantly</div>
        </div>
        <div class=\"chart-svg-wrap\" style=\"margin-bottom:8px;\">
          <svg id=\"chartInteractiveTierDrill\" class=\"chart-svg\" style=\"height:160px;\"></svg>
          <div id=\"tooltipInteractiveTierDrill\" class=\"svg-tooltip\"></div>
        </div>
        <div class=\"chart-svg-wrap\">
          <svg id=\"chartInteractiveSegmentDrill\" class=\"chart-svg\" style=\"height:160px;\"></svg>
          <div id=\"tooltipInteractiveSegmentDrill\" class=\"svg-tooltip\"></div>
        </div>
        <div id=\"insightInteractiveDrill\" class=\"chart-insight\">Click a risk tier or segment bar to auto-apply filter and refresh the account table.</div>
      </article>
    </div>

    <div class=\"chart-grid\" style=\"margin-top:10px;\" id=\"execCharts\"></div>
  </section>

  <section class=\"section\" id=\"section_revenue\">
    <div class=\"section-head\">
      <h2>Revenue Quality</h2>
      <div class=\"section-note\">Pricing discipline, concentration, and expansion balance over time.</div>
    </div>
    <div class=\"chart-grid\" id=\"revenueCharts\"></div>
  </section>

  <section class=\"section\" id=\"section_retention\">
    <div class=\"section-head\">
      <h2>Retention & Churn</h2>
      <div class=\"section-note\">Where retention is resilient and where churn pressure is rising.</div>
    </div>
    <div class=\"chart-grid\" id=\"retentionCharts\"></div>
  </section>

  <section class=\"section\" id=\"section_risk\">
    <div class=\"section-head\">
      <h2>Account Risk</h2>
      <div class=\"section-note\">Risk concentration and intervention queue by current commercial exposure.</div>
    </div>

    <div class=\"kpi-grid\" id=\"sliceKpis\"></div>

    <div class=\"panel-grid\" style=\"margin-top:10px;\">
      <div class=\"panel\">
        <h3>Risk Tier Split (Filtered Accounts)</h3>
        <div class=\"risk-stack\" id=\"riskBars\"></div>
      </div>
      <div class=\"panel\">
        <h3>Slice Notes</h3>
        <p id=\"sliceNotes\"></p>
      </div>
    </div>

    <div class=\"chart-grid\" style=\"margin-top:10px;\" id=\"riskCharts\"></div>

    <div style=\"margin-top:10px;\" class=\"panel\">
      <h3>Highest-Risk Accounts (Filtered)</h3>
      <div class=\"table-wrap\">
        <table id=\"accountsTable\">
          <thead>
            <tr>
              <th data-sort=\"customer_id\">Customer</th>
              <th data-sort=\"segment\">Segment</th>
              <th data-sort=\"region\">Region</th>
              <th data-sort=\"plan_tier\">Plan</th>
              <th data-sort=\"current_mrr\">Current MRR</th>
              <th data-sort=\"churn_risk_score\">Churn Risk</th>
              <th data-sort=\"governance_priority_score\">Governance Priority</th>
              <th data-sort=\"governance_priority_tier\">Priority Tier</th>
              <th data-sort=\"recommended_action\">Recommended Action</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div class=\"table-meta\">
        <div id=\"accountsMeta\"></div>
        <div class=\"pager\">
          <button id=\"acctPrev\">Prev</button>
          <span id=\"acctPage\"></span>
          <button id=\"acctNext\">Next</button>
        </div>
      </div>
    </div>
  </section>

  <section class=\"section\" id=\"section_portfolio\">
    <div class=\"section-head\">
      <h2>Portfolio / Manager View</h2>
      <div class=\"section-note\">Portfolio performance and risk profile by manager and team.</div>
    </div>
    <div class=\"panel\">
      <h3>Manager Comparison (Filtered Accounts)</h3>
      <div class=\"table-wrap\">
        <table id=\"managerTable\">
          <thead>
            <tr>
              <th data-sort=\"account_manager_id\">Manager</th>
              <th data-sort=\"team\">Team</th>
              <th data-sort=\"portfolio_accounts\">Accounts</th>
              <th data-sort=\"portfolio_mrr\">Portfolio MRR</th>
              <th data-sort=\"avg_governance\">Avg Governance Priority</th>
              <th data-sort=\"critical_accounts\">Critical Accounts</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div class=\"table-meta\" id=\"managerMeta\"></div>
    </div>
    <div class=\"chart-grid\" style=\"margin-top:10px;\" id=\"portfolioCharts\"></div>
  </section>

  <section class=\"section\" id=\"section_scenario\">
    <div class=\"section-head\">
      <h2>Scenario & Forecast</h2>
      <div class=\"section-note\">Expected trajectory under base, downside, and improvement paths.</div>
    </div>

    <div class=\"kpi-grid\" id=\"scenarioCards\"></div>

    <div class=\"panel\" style=\"margin-top:10px;\">
      <h3>Business Impact Estimates</h3>
      <div class=\"table-wrap\">
        <table id=\"riskImpactTable\">
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
              <th>Unit</th>
              <th>Definition</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <div class=\"chart-grid\" style=\"margin-top:10px;\" id=\"scenarioCharts\"></div>
  </section>

  <section class=\"section\" id=\"section_method\">
    <div class=\"section-head\">
      <h2>Methodology & Definitions</h2>
      <div class=\"section-note\">Definitions, assumptions, and caveats for interpretation.</div>
    </div>

    <button id=\"methodDrawerBtn\" class=\"drawer-btn\">Toggle Methodology Drawer</button>
    <div class=\"methodology-drawer\" id=\"methodDrawer\">
      <div class=\"methodology-grid\" id=\"methodGrid\"></div>
    </div>

    <details class=\"technical-details\" style=\"margin-top:10px;\">
      <summary>Technical references (optional)</summary>
      <div class=\"panel\" style=\"margin-top:8px;\">
        <h3>Source Map</h3>
        <div id=\"sourceMap\"></div>
      </div>
    </details>
  </section>
</main>

<div class=\"chart-modal\" id=\"chartModal\" aria-hidden=\"true\">
  <div class=\"chart-modal-dialog\">
    <div class=\"chart-modal-head\">
      <div class=\"chart-modal-title\" id=\"chartModalTitle\">Chart</div>
      <div class=\"chart-modal-controls\">
        <button id=\"chartPrev\">Prev</button>
        <button id=\"chartNext\">Next</button>
        <button id=\"chartZoomOut\">Zoom -</button>
        <button id=\"chartZoomIn\">Zoom +</button>
        <button id=\"chartZoomReset\">Reset</button>
        <button id=\"chartClose\" class=\"primary\">Close</button>
      </div>
    </div>
    <div class=\"chart-modal-body\">
      <div class=\"chart-modal-stage\" id=\"chartModalStage\">
        <img class=\"chart-modal-image\" id=\"chartModalImage\" alt=\"Expanded chart\" />
      </div>
    </div>
    <div class=\"chart-modal-foot\">
      <div id=\"chartModalSubtitle\"></div>
      <div id=\"chartModalPosition\"></div>
    </div>
  </div>
</div>

<script id=\"dashboard-data\" type=\"application/json\">__PAYLOAD_JSON__</script>
<script>
const payload = JSON.parse(document.getElementById('dashboard-data').textContent);
const accounts = payload.accounts || [];
const managerPanel = payload.manager_panel || [];
const filters = payload.filters || {};
const monthlyCompactIndex = payload.monthly_compact_index || {};
const monthlyCompactRows = payload.monthly_compact_rows || [];
const MIN_MODAL_ZOOM = 0.45;
const MAX_MODAL_ZOOM = 4;
const THEME_STORAGE_KEY = 'executive_dashboard_theme';

const state = {{
  page: 1,
  perPage: 20,
  sortBy: 'governance_priority_score',
  sortDir: 'desc',
  managerSortBy: 'portfolio_mrr',
  managerSortDir: 'desc',
  chartDensity: 'comfortable',
  theme: 'light',
}};

const chartContext = {{}};
const modalState = {{
  charts: [],
  index: 0,
  zoom: MIN_MODAL_ZOOM,
  tx: 0,
  ty: 0,
  dragging: false,
  dragStartX: 0,
  dragStartY: 0,
}};

function esc(value) {{
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}}

function fmtMoney(value) {{
  return Number(value || 0).toLocaleString('en-US', {{ style: 'currency', currency: 'USD', maximumFractionDigits: 0 }});
}}

function fmtPct(value) {{
  return `${{(Number(value || 0) * 100).toFixed(2)}}%`;
}}

function fmtNum(value, d = 1) {{
  return Number(value || 0).toLocaleString('en-US', {{ maximumFractionDigits: d, minimumFractionDigits: d }});
}}

function loadThemePreference() {{
  try {{
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === 'dark' || stored === 'light') return stored;
  }} catch (err) {{
    return 'light';
  }}
  return 'light';
}}

function saveThemePreference(theme) {{
  try {{
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  }} catch (err) {{
    return;
  }}
}}

function applyTheme(theme) {{
  const resolved = theme === 'dark' ? 'dark' : 'light';
  state.theme = resolved;
  document.body.setAttribute('data-theme', resolved);
  const btn = document.getElementById('btn_theme');
  if (!btn) return;
  const isDark = resolved === 'dark';
  btn.textContent = isDark ? 'Light mode' : 'Dark mode';
  btn.setAttribute('aria-pressed', isDark ? 'true' : 'false');
}}

function bySection(section) {{
  return (payload.chart_catalog || []).filter(c => c.section === section);
}}

function renderCharts(targetId, section) {{
  const el = document.getElementById(targetId);
  const charts = bySection(section);
  chartContext[targetId] = charts;
  if (!charts.length) {{
    el.innerHTML = '<div class="empty-state">No chart artifacts available for this section.</div>';
    return;
  }}
  el.innerHTML = charts.map((c, idx) => `
    <article class="chart-card">
      <div class="chart-title">${{esc(c.title)}}</div>
      <div class="chart-subtitle">${{esc(c.subtitle)}}</div>
      <div class="chart-actions">
        <button class="chart-expand-btn" data-target="${{esc(targetId)}}" data-index="${{idx}}">Expand</button>
        <a class="button-link" href="${{c.image_data_uri}}" download="${{esc(c.filename)}}" aria-label="Download chart">Download</a>
      </div>
      <img
        class="chart-preview"
        data-target="${{esc(targetId)}}"
        data-index="${{idx}}"
        src="${{c.image_data_uri}}"
        alt="${{esc(c.title)}}"
        loading="lazy"
      />
    </article>
  `).join('');

  el.querySelectorAll('.chart-expand-btn, .chart-preview').forEach(node => {{
    node.addEventListener('click', () => {{
      const target = node.getAttribute('data-target');
      const idx = Number(node.getAttribute('data-index') || 0);
      openChartModal(target, idx);
    }});
  }});
}}

function applyDensity() {{
  document.body.classList.remove('density-compact', 'density-comfortable', 'density-presentation');
  document.body.classList.add(`density-${{state.chartDensity}}`);
  document.querySelectorAll('.chart-density-btn').forEach(btn => {{
    const active = btn.getAttribute('data-density') === state.chartDensity;
    btn.classList.toggle('active', active);
  }});
}}

function updateModalTransform() {{
  const img = document.getElementById('chartModalImage');
  img.style.transform = `translate(${{modalState.tx}}px, ${{modalState.ty}}px) scale(${{modalState.zoom}})`;
}}

function updateModalContent() {{
  if (!modalState.charts.length) return;
  const idx = Math.max(0, Math.min(modalState.index, modalState.charts.length - 1));
  modalState.index = idx;
  const chart = modalState.charts[idx];
  document.getElementById('chartModalTitle').textContent = chart.title || 'Chart';
  document.getElementById('chartModalSubtitle').textContent = chart.subtitle || '';
  document.getElementById('chartModalPosition').textContent = `${{idx + 1}} / ${{modalState.charts.length}}`;
  const img = document.getElementById('chartModalImage');
  img.src = chart.image_data_uri;
  img.alt = chart.title || 'Chart';
  modalState.zoom = MIN_MODAL_ZOOM;
  modalState.tx = 0;
  modalState.ty = 0;
  updateModalTransform();
}}

function openChartModal(targetId, index) {{
  modalState.charts = chartContext[targetId] || [];
  modalState.index = index || 0;
  if (!modalState.charts.length) return;
  document.getElementById('chartModal').classList.add('active');
  document.getElementById('chartModal').setAttribute('aria-hidden', 'false');
  updateModalContent();
}}

function closeChartModal() {{
  document.getElementById('chartModal').classList.remove('active');
  document.getElementById('chartModal').setAttribute('aria-hidden', 'true');
}}

function moveChart(step) {{
  if (!modalState.charts.length) return;
  modalState.index = (modalState.index + step + modalState.charts.length) % modalState.charts.length;
  updateModalContent();
}}

function svgEl(tag, attrs = {{}}) {{
  const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
  Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, String(v)));
  return el;
}}

function clearSvg(svgId) {{
  const svg = document.getElementById(svgId);
  svg.innerHTML = '';
  const width = svg.clientWidth || 800;
  const height = svg.clientHeight || 320;
  svg.setAttribute('viewBox', `0 0 ${{width}} ${{height}}`);
  return {{ svg, width, height }};
}}

function showTooltip(tooltipId, html, x, y) {{
  const tip = document.getElementById(tooltipId);
  if (!tip) return;
  tip.style.display = 'block';
  tip.innerHTML = html;
  tip.style.left = `${{x + 10}}px`;
  tip.style.top = `${{y + 10}}px`;
}}

function hideTooltip(tooltipId) {{
  const tip = document.getElementById(tooltipId);
  if (!tip) return;
  tip.style.display = 'none';
}}

function drawLineChart(svgId, tooltipId, data, config) {{
  const {{ svg, width, height }} = clearSvg(svgId);
  if (!data.length) {{
    svg.appendChild(svgEl('text', {{ x: 12, y: 22, fill: '#64748b', 'font-size': 12 }})).textContent = 'No data';
    return;
  }}
  const m = {{ l: 54, r: 18, t: 14, b: 34 }};
  const plotW = width - m.l - m.r;
  const plotH = height - m.t - m.b;
  const labels = data.map(d => d.month);
  const values = config.series.flatMap(s => data.map(d => Number(d[s.key] || 0)));
  let yMin = Math.min(...values);
  let yMax = Math.max(...values);
  if (config.zeroBaseline) yMin = Math.min(0, yMin);
  if (yMax <= yMin) yMax = yMin + 1;
  const pad = (yMax - yMin) * 0.1;
  yMin -= pad;
  yMax += pad;

  const x = i => m.l + (labels.length === 1 ? plotW / 2 : (i * plotW) / (labels.length - 1));
  const y = v => m.t + ((yMax - v) / (yMax - yMin)) * plotH;

  svg.appendChild(svgEl('rect', {{ x: m.l, y: m.t, width: plotW, height: plotH, fill: '#ffffff' }}));
  svg.appendChild(svgEl('line', {{ x1: m.l, y1: m.t + plotH, x2: m.l + plotW, y2: m.t + plotH, stroke: '#c9d8ea', 'stroke-width': 1 }}));
  svg.appendChild(svgEl('line', {{ x1: m.l, y1: m.t, x2: m.l, y2: m.t + plotH, stroke: '#c9d8ea', 'stroke-width': 1 }}));

  const tickCount = 4;
  for (let i = 0; i <= tickCount; i += 1) {{
    const v = yMin + ((yMax - yMin) * i) / tickCount;
    const ty = y(v);
    svg.appendChild(svgEl('line', {{ x1: m.l, y1: ty, x2: m.l + plotW, y2: ty, stroke: '#edf2f8', 'stroke-width': 1 }}));
    const t = svgEl('text', {{ x: m.l - 6, y: ty + 4, fill: '#64748b', 'font-size': 11, 'text-anchor': 'end' }});
    t.textContent = config.yTickFormat ? config.yTickFormat(v) : Number(v).toFixed(2);
    svg.appendChild(t);
  }}

  const labelStep = Math.max(1, Math.floor(labels.length / 8));
  labels.forEach((label, i) => {{
    if (i % labelStep !== 0 && i !== labels.length - 1) return;
    const tx = svgEl('text', {{ x: x(i), y: height - 8, fill: '#64748b', 'font-size': 11, 'text-anchor': 'middle' }});
    tx.textContent = label;
    svg.appendChild(tx);
  }});

  config.series.forEach(s => {{
    const points = data.map((d, i) => `${{x(i)}},${{y(Number(d[s.key] || 0))}}`).join(' ');
    svg.appendChild(svgEl('polyline', {{
      points,
      fill: 'none',
      stroke: s.color,
      'stroke-width': 2.2,
      'stroke-linejoin': 'round',
      'stroke-linecap': 'round',
    }}));

    data.forEach((d, i) => {{
      const vx = x(i);
      const vy = y(Number(d[s.key] || 0));
      const c = svgEl('circle', {{ cx: vx, cy: vy, r: 3.7, fill: s.color, 'data-month': d.month }});
      c.addEventListener('mouseenter', e => {{
        const value = Number(d[s.key] || 0);
        const valTxt = s.valueFormat ? s.valueFormat(value) : value.toFixed(2);
        showTooltip(
          tooltipId,
          `<strong>${{esc(d.month)}}</strong><br/>${{esc(s.label)}}: ${{esc(valTxt)}}`,
          e.offsetX,
          e.offsetY
        );
      }});
      c.addEventListener('mouseleave', () => hideTooltip(tooltipId));
      c.addEventListener('click', () => {{
        if (config.onPointClick) config.onPointClick(d, s);
      }});
      svg.appendChild(c);
    }});
  }});

  const legendStart = m.l;
  config.series.forEach((s, i) => {{
    const lx = legendStart + i * 140;
    svg.appendChild(svgEl('rect', {{ x: lx, y: 4, width: 12, height: 12, rx: 2, fill: s.color }}));
    const lt = svgEl('text', {{ x: lx + 16, y: 14, fill: '#334155', 'font-size': 11 }});
    lt.textContent = s.label;
    svg.appendChild(lt);
  }});
}}

function drawBarChart(svgId, tooltipId, rows, opts) {{
  const {{ svg, width, height }} = clearSvg(svgId);
  if (!rows.length) {{
    svg.appendChild(svgEl('text', {{ x: 12, y: 22, fill: '#64748b', 'font-size': 12 }})).textContent = 'No data';
    return;
  }}
  const m = {{ l: 42, r: 12, t: 14, b: 38 }};
  const plotW = width - m.l - m.r;
  const plotH = height - m.t - m.b;
  const maxVal = Math.max(...rows.map(r => Number(r.value || 0)), 1);
  const barW = plotW / rows.length * 0.62;
  const gap = plotW / rows.length;
  const y = v => m.t + plotH - (Number(v || 0) / maxVal) * plotH;

  svg.appendChild(svgEl('line', {{ x1: m.l, y1: m.t + plotH, x2: m.l + plotW, y2: m.t + plotH, stroke: '#c9d8ea', 'stroke-width': 1 }}));
  rows.forEach((r, i) => {{
    const x = m.l + i * gap + (gap - barW) / 2;
    const h = m.t + plotH - y(r.value);
    const rect = svgEl('rect', {{
      x,
      y: y(r.value),
      width: barW,
      height: Math.max(1, h),
      rx: 5,
      fill: r.color || opts.color || '#1f5fbf',
      cursor: opts.onClick ? 'pointer' : 'default',
    }});
    rect.addEventListener('mouseenter', e => {{
      const valueText = opts.valueFormat ? opts.valueFormat(Number(r.value || 0)) : String(r.value);
      showTooltip(tooltipId, `<strong>${{esc(r.label)}}</strong><br/>${{esc(valueText)}}`, e.offsetX, e.offsetY);
    }});
    rect.addEventListener('mouseleave', () => hideTooltip(tooltipId));
    if (opts.onClick) rect.addEventListener('click', () => opts.onClick(r));
    svg.appendChild(rect);

    const tx = svgEl('text', {{ x: x + barW / 2, y: m.t + plotH + 14, fill: '#64748b', 'font-size': 11, 'text-anchor': 'middle' }});
    tx.textContent = r.label;
    svg.appendChild(tx);
  }});
}}

function aggregateMonthlyForFiltered(filteredIds) {{
  if (!filteredIds.size) return [];
  const idx = monthlyCompactIndex;
  const months = (payload.monthly_summary || []).map(r => r.month);
  const bucketMap = new Map(months.map(m => [m, {{
    month: m,
    mrr: 0,
    expansion_mrr: 0,
    contraction_mrr: 0,
    discounted_mrr: 0,
    churned_mrr: 0,
    churn_events: 0,
    account_rows: 0,
  }}]));

  monthlyCompactRows.forEach(row => {{
    const cid = String(row[idx.customer_id] || '');
    if (!filteredIds.has(cid)) return;
    const month = String(row[idx.month] || '');
    if (!month) return;
    if (!bucketMap.has(month)) {{
      bucketMap.set(month, {{
        month,
        mrr: 0,
        expansion_mrr: 0,
        contraction_mrr: 0,
        discounted_mrr: 0,
        churned_mrr: 0,
        churn_events: 0,
        account_rows: 0,
      }});
    }}
    const b = bucketMap.get(month);
    const mrr = Number(row[idx.active_mrr] || 0);
    const expansion = Number(row[idx.expansion_mrr] || 0);
    const contraction = Number(row[idx.contraction_mrr] || 0);
    const discountDep = Number(row[idx.discount_dependency_flag] || 0);
    const churnFlag = Number(row[idx.churn_flag] || 0);

    b.mrr += mrr;
    b.expansion_mrr += expansion;
    b.contraction_mrr += contraction;
    if (discountDep === 1) b.discounted_mrr += mrr;
    if (churnFlag === 1) {{
      b.churn_events += 1;
      b.churned_mrr += mrr;
    }}
    if (mrr > 0) b.account_rows += 1;
  }});

  const ordered = [...bucketMap.values()].sort((a, b) => String(a.month).localeCompare(String(b.month)));
  let prevMrr = 0;
  return ordered.map(r => {{
    const starting = prevMrr;
    const grr = starting > 0 ? (starting - r.contraction_mrr - r.churned_mrr) / starting : 0;
    const nrr = starting > 0 ? (starting + r.expansion_mrr - r.contraction_mrr - r.churned_mrr) / starting : 0;
    const out = {{
      month: r.month,
      mrr: r.mrr,
      arr: r.mrr * 12,
      logo_churn_rate: r.account_rows > 0 ? r.churn_events / r.account_rows : 0,
      discounted_share: r.mrr > 0 ? r.discounted_mrr / r.mrr : 0,
      grr,
      nrr,
      account_rows: r.account_rows,
    }};
    prevMrr = r.mrr;
    return out;
  }});
}}

function computeFilteredKpis(filtered, monthly) {{
  if (!filtered.length) {{
    return {{
      current_mrr: 0,
      arr: 0,
      gross_retention: 0,
      net_retention: 0,
      logo_churn: 0,
      avg_discount: 0,
      discounted_revenue_share: 0,
      revenue_at_risk_mrr: 0,
      critical_risk_account_count: 0,
    }};
  }}
  const currentMrr = filtered.reduce((acc, r) => acc + Number(r.current_mrr || 0), 0);
  const discountWeightedNum = filtered.reduce((acc, r) => acc + Number(r.current_mrr || 0) * Number(r.discount_pct || 0), 0);
  const avgDiscount = currentMrr > 0 ? discountWeightedNum / currentMrr : 0;
  const atRiskMrr = filtered
    .filter(r => ['High', 'Critical'].includes(r.governance_priority_tier))
    .reduce((acc, r) => acc + Number(r.current_mrr || 0), 0);
  const latest = [...monthly].reverse().find(r => Number(r.account_rows || 0) > 0) || monthly[monthly.length - 1] || {{
    grr: 0,
    nrr: 0,
    logo_churn_rate: 0,
    discounted_share: 0,
  }};
  return {{
    current_mrr: currentMrr,
    arr: currentMrr * 12,
    gross_retention: Number(latest.grr || 0),
    net_retention: Number(latest.nrr || 0),
    logo_churn: Number(latest.logo_churn_rate || 0),
    avg_discount: avgDiscount,
    discounted_revenue_share: Number(latest.discounted_share || 0),
    revenue_at_risk_mrr: atRiskMrr,
    critical_risk_account_count: filtered.filter(r => r.governance_priority_tier === 'Critical').length,
  }};
}}

function renderInteractiveOverviewCharts(monthly) {{
  drawLineChart('chartInteractiveMrrArr', 'tooltipInteractiveMrrArr', monthly, {{
    series: [
      {{ key: 'mrr', label: 'MRR', color: '#1f5fbf', valueFormat: v => fmtMoney(v) }},
      {{ key: 'arr', label: 'ARR', color: '#12746b', valueFormat: v => fmtMoney(v) }},
    ],
    yTickFormat: v => `$${{(v / 1_000_000).toFixed(1)}}M`,
    onPointClick: d => {{
      document.getElementById('insightInteractiveMrrArr').textContent =
        `Month ${{d.month}}: MRR ${{fmtMoney(d.mrr)}} | ARR ${{fmtMoney(d.arr)}}.`;
    }},
  }});

  drawLineChart('chartInteractiveRetention', 'tooltipInteractiveRetention', monthly, {{
    series: [
      {{ key: 'grr', label: 'GRR', color: '#0f7a53', valueFormat: v => fmtPct(v) }},
      {{ key: 'nrr', label: 'NRR', color: '#c66a0a', valueFormat: v => fmtPct(v) }},
    ],
    yTickFormat: v => `${{(v * 100).toFixed(1)}}%`,
    onPointClick: d => {{
      document.getElementById('insightInteractiveRetention').textContent =
        `Month ${{d.month}}: GRR ${{fmtPct(d.grr)}} | NRR ${{fmtPct(d.nrr)}}.`;
    }},
  }});

  drawLineChart('chartInteractiveDiscountChurn', 'tooltipInteractiveDiscountChurn', monthly, {{
    series: [
      {{ key: 'discounted_share', label: 'Discounted Revenue Share', color: '#6f4ba3', valueFormat: v => fmtPct(v) }},
      {{ key: 'logo_churn_rate', label: 'Logo Churn Rate', color: '#b3342b', valueFormat: v => fmtPct(v) }},
    ],
    yTickFormat: v => `${{(v * 100).toFixed(1)}}%`,
    onPointClick: d => {{
      document.getElementById('insightInteractiveDiscountChurn').textContent =
        `Month ${{d.month}}: discounted share ${{fmtPct(d.discounted_share)}} and logo churn ${{fmtPct(d.logo_churn_rate)}}.`;
    }},
  }});
}}

function renderInteractiveSliceDrill(filtered) {{
  const tierOrder = ['Low', 'Moderate', 'High', 'Critical'];
  const tierRows = tierOrder.map(t => {{
    const count = filtered.filter(r => r.governance_priority_tier === t).length;
    const colorMap = {{ Low: '#4f8dd9', Moderate: '#32a07f', High: '#d99124', Critical: '#c74b40' }};
    return {{ label: t, value: count, color: colorMap[t] }};
  }});
  drawBarChart('chartInteractiveTierDrill', 'tooltipInteractiveTierDrill', tierRows, {{
    valueFormat: v => `${{Number(v).toLocaleString('en-US')}} accounts`,
    onClick: row => {{
      document.getElementById('f_risk').value = row.label;
      state.page = 1;
      applyAll();
      document.getElementById('insightInteractiveDrill').textContent =
        `Applied filter: risk tier = ${{row.label}}.`;
    }},
  }});

  const segmentMap = new Map();
  filtered.forEach(r => {{
    const key = r.segment || 'Unknown';
    segmentMap.set(key, (segmentMap.get(key) || 0) + Number(r.current_mrr || 0));
  }});
  const segmentRows = [...segmentMap.entries()]
    .map(([label, value]) => ({{ label, value }}))
    .sort((a, b) => b.value - a.value)
    .slice(0, 6);
  drawBarChart('chartInteractiveSegmentDrill', 'tooltipInteractiveSegmentDrill', segmentRows, {{
    color: '#1f5fbf',
    valueFormat: v => fmtMoney(v),
    onClick: row => {{
      document.getElementById('f_segment').value = row.label;
      state.page = 1;
      applyAll();
      document.getElementById('insightInteractiveDrill').textContent =
        `Applied filter: segment = ${{row.label}}.`;
    }},
  }});
}

function populateSelect(id, values) {{
  const select = document.getElementById(id);
  const options = ['All', ...(values || [])];
  select.innerHTML = options.map(v => `<option value="${{esc(v)}}">${{esc(v)}}</option>`).join('');
}}

function getFilterValues() {{
  return {{
    region: document.getElementById('f_region').value,
    segment: document.getElementById('f_segment').value,
    industry: document.getElementById('f_industry').value,
    plan: document.getElementById('f_plan').value,
    channel: document.getElementById('f_channel').value,
    manager: document.getElementById('f_manager').value,
    risk: document.getElementById('f_risk').value,
    start: document.getElementById('f_start').value,
    end: document.getElementById('f_end').value,
    search: document.getElementById('f_search').value.trim().toLowerCase(),
  }};
}}

function accountMatches(a, f) {{
  if (f.region !== 'All' && a.region !== f.region) return false;
  if (f.segment !== 'All' && a.segment !== f.segment) return false;
  if (f.industry !== 'All' && a.industry !== f.industry) return false;
  if (f.plan !== 'All' && a.plan_tier !== f.plan) return false;
  if (f.channel !== 'All' && a.acquisition_channel !== f.channel) return false;
  if (f.manager !== 'All' && a.account_manager_id !== f.manager) return false;
  if (f.risk !== 'All' && a.governance_priority_tier !== f.risk) return false;
  if (f.start !== 'All' && String(a.signup_month || '') < f.start) return false;
  if (f.end !== 'All' && String(a.signup_month || '') > f.end) return false;
  if (f.search) {{
    const hay = [a.customer_id, a.industry, a.recommended_action, a.account_manager_id, a.plan_tier]
      .map(v => String(v || '').toLowerCase())
      .join(' ');
    if (!hay.includes(f.search)) return false;
  }}
  return true;
}}

function getFilteredAccounts() {{
  const f = getFilterValues();
  return accounts.filter(a => accountMatches(a, f));
}}

function renderOfficialKpis(k, filteredCount) {{
  const baseline = payload.official_kpis || {{}};
  const total = accounts.length || 1;
  const isFiltered = filteredCount < total;
  const foot = (label, deltaValue, isPct = false) => {{
    if (!isFiltered) return 'Full portfolio scope';
    const sign = deltaValue > 0 ? '+' : '';
    if (isPct) return `vs portfolio: ${{sign}}${{(deltaValue * 100).toFixed(2)}}pp`;
    return `vs portfolio: ${{sign}}${{fmtMoney(deltaValue)}}`;
  }};
  const cards = [
    ['Current MRR', fmtMoney(k.current_mrr), foot('MRR', Number(k.current_mrr || 0) - Number(baseline.current_mrr || 0))],
    ['ARR', fmtMoney(k.arr), foot('ARR', Number(k.arr || 0) - Number(baseline.arr || 0))],
    ['Gross Retention', fmtPct(k.gross_retention), foot('GRR', Number(k.gross_retention || 0) - Number(baseline.gross_retention || 0), true)],
    ['Net Retention', fmtPct(k.net_retention), foot('NRR', Number(k.net_retention || 0) - Number(baseline.net_retention || 0), true)],
    ['Logo Churn', fmtPct(k.logo_churn), foot('Logo Churn', Number(k.logo_churn || 0) - Number(baseline.logo_churn || 0), true)],
    ['Average Discount', fmtPct(k.avg_discount), foot('Discount', Number(k.avg_discount || 0) - Number(baseline.avg_discount || 0), true)],
    ['Discounted Revenue Share', fmtPct(k.discounted_revenue_share), foot('Disc. Share', Number(k.discounted_revenue_share || 0) - Number(baseline.discounted_revenue_share || 0), true)],
    ['Revenue At Risk (MRR)', fmtMoney(k.revenue_at_risk_mrr), foot('Risk MRR', Number(k.revenue_at_risk_mrr || 0) - Number(baseline.revenue_at_risk_mrr || 0))],
    ['Critical-Risk Accounts', fmtNum(k.critical_risk_account_count, 0), isFiltered ? 'Filtered critical account count' : 'Full portfolio critical count'],
  ];
  document.getElementById('officialKpis').innerHTML = cards.map(([label, value, foot]) => `
    <article class="kpi-card">
      <div class="kpi-label">${{esc(label)}}</div>
      <div class="kpi-value">${{esc(value)}}</div>
      <div class="kpi-foot">${{esc(foot)}}</div>
    </article>
  `).join('');
}}

function renderAlerts(kpis, filteredCount) {{
  const alerts = [];
  if (kpis.net_retention < 1) {{
    alerts.push({{
      severity: 'high',
      label: 'NRR Below 100%',
      detail: `Filtered NRR is ${{fmtPct(kpis.net_retention)}}; expansion is not fully offsetting losses.`,
    }});
  }}
  if (kpis.discounted_revenue_share > 0.15) {{
    alerts.push({{
      severity: 'medium',
      label: 'High Discount Dependency',
      detail: `Filtered discounted-revenue share is ${{fmtPct(kpis.discounted_revenue_share)}}.`,
    }});
  }}
  if (kpis.critical_risk_account_count > 0) {{
    alerts.push({{
      severity: 'high',
      label: 'Critical Accounts Present',
      detail: `${{kpis.critical_risk_account_count}} critical accounts remain in current filter scope.`,
    }});
  }}
  if (!alerts.length) {{
    alerts.push({{
      severity: 'low',
      label: 'No Alert Threshold Breach',
      detail: `No high-priority breach detected in current filtered scope (${{filteredCount.toLocaleString()}} accounts).`,
    }});
  }}
  document.getElementById('alerts').innerHTML = alerts.map(a => `
    <div class="alert-item ${{esc(a.severity)}}">
      <div class="alert-title">${{esc(a.label)}}</div>
      <div class="alert-detail">${{esc(a.detail)}}</div>
    </div>
  `).join('');
}}

function renderSliceKpis(filtered) {{
  if (!filtered.length) {{
    document.getElementById('sliceKpis').innerHTML = '<div class="empty-state">No accounts match current filters.</div>';
    return;
  }}
  const totalMrr = filtered.reduce((acc, r) => acc + Number(r.current_mrr || 0), 0);
  const avgGov = filtered.reduce((acc, r) => acc + Number(r.governance_priority_score || 0), 0) / filtered.length;
  const highCritical = filtered.filter(r => ['High', 'Critical'].includes(r.governance_priority_tier)).length;
  const avgChurn = filtered.reduce((acc, r) => acc + Number(r.churn_risk_score || 0), 0) / filtered.length;
  const cards = [
    ['Filtered Accounts', fmtNum(filtered.length, 0), 'Accounts in current view'],
    ['Filtered Portfolio MRR', fmtMoney(totalMrr), 'Current recurring revenue in scope'],
    ['Avg Governance Priority', fmtNum(avgGov, 1), 'Intervention urgency index'],
    ['High/Critical Share', `${{((highCritical / filtered.length) * 100).toFixed(1)}}%`, 'Share of accounts needing active attention'],
    ['Avg Churn Risk', fmtNum(avgChurn, 1), 'Forward churn-pressure index'],
  ];
  document.getElementById('sliceKpis').innerHTML = cards.map(([label, value, foot]) => `
    <article class="kpi-card">
      <div class="kpi-label">${{esc(label)}}</div>
      <div class="kpi-value">${{esc(value)}}</div>
      <div class="kpi-foot">${{esc(foot)}}</div>
    </article>
  `).join('');
}}

function renderRiskBars(filtered) {{
  const tiers = ['Low', 'Moderate', 'High', 'Critical'];
  const counts = Object.fromEntries(tiers.map(t => [t, 0]));
  filtered.forEach(r => {{
    if (counts[r.governance_priority_tier] !== undefined) counts[r.governance_priority_tier] += 1;
  }});
  const total = Math.max(filtered.length, 1);
  const cls = {{ Low: 'fill-low', Moderate: 'fill-moderate', High: 'fill-high', Critical: 'fill-critical' }};
  document.getElementById('riskBars').innerHTML = tiers.map(t => {{
    const share = (counts[t] / total) * 100;
    return `
      <div class="risk-bar">
        <div>${{esc(t)}}</div>
        <div class="risk-track"><div class="risk-fill ${{cls[t]}}" style="width:${{share.toFixed(1)}}%"></div></div>
        <div>${{share.toFixed(1)}}%</div>
      </div>
    `;
  }}).join('');
}}

function sortedRows(rows, sortBy, sortDir) {{
  const dir = sortDir === 'asc' ? 1 : -1;
  return [...rows].sort((a, b) => {{
    const va = a[sortBy];
    const vb = b[sortBy];
    const na = Number(va);
    const nb = Number(vb);
    if (!Number.isNaN(na) && !Number.isNaN(nb)) return (na - nb) * dir;
    return String(va || '').localeCompare(String(vb || '')) * dir;
  }});
}}

function renderAccountsTable(filtered) {{
  const rows = sortedRows(filtered, state.sortBy, state.sortDir);
  const pages = Math.max(1, Math.ceil(rows.length / state.perPage));
  state.page = Math.min(state.page, pages);
  const start = (state.page - 1) * state.perPage;
  const slice = rows.slice(start, start + state.perPage);
  const tbody = document.querySelector('#accountsTable tbody');

  if (!slice.length) {{
    tbody.innerHTML = '<tr><td colspan="9"><div class="empty-state">No accounts match current filters.</div></td></tr>';
  }} else {{
    tbody.innerHTML = slice.map(r => `
      <tr>
        <td>${{esc(r.customer_id)}}</td>
        <td>${{esc(r.segment)}}</td>
        <td>${{esc(r.region)}}</td>
        <td>${{esc(r.plan_tier)}}</td>
        <td>${{fmtMoney(r.current_mrr)}}</td>
        <td>${{fmtNum(r.churn_risk_score, 1)}}</td>
        <td>${{fmtNum(r.governance_priority_score, 1)}}</td>
        <td>${{esc(r.governance_priority_tier)}}</td>
        <td title="${{esc(r.recommended_action_reason)}}">${{esc(r.recommended_action)}}</td>
      </tr>
    `).join('');
  }}

  document.getElementById('accountsMeta').textContent = `Rows: ${{rows.length.toLocaleString()}} | Showing ${{slice.length ? start + 1 : 0}}-${{Math.min(start + state.perPage, rows.length)}}`;
  document.getElementById('acctPage').textContent = `Page ${{state.page}} / ${{pages}}`;
  document.getElementById('acctPrev').disabled = state.page <= 1;
  document.getElementById('acctNext').disabled = state.page >= pages;
}}

function aggregateManagers(filtered) {{
  const panelMap = Object.fromEntries(managerPanel.map(r => [r.account_manager_id, r]));
  const agg = new Map();
  filtered.forEach(r => {{
    const id = r.account_manager_id || 'Unknown';
    if (!agg.has(id)) {{
      const p = panelMap[id] || {{}};
      agg.set(id, {{
        account_manager_id: id,
        team: p.team || 'Unknown',
        portfolio_accounts: 0,
        portfolio_mrr: 0,
        avg_governance: 0,
        critical_accounts: 0,
      }});
    }}
    const row = agg.get(id);
    row.portfolio_accounts += 1;
    row.portfolio_mrr += Number(r.current_mrr || 0);
    row.avg_governance += Number(r.governance_priority_score || 0);
    if (r.governance_priority_tier === 'Critical') row.critical_accounts += 1;
  }});

  return [...agg.values()].map(r => ({
    ...r,
    avg_governance: r.portfolio_accounts ? r.avg_governance / r.portfolio_accounts : 0,
  }));
}}

function renderManagerTable(filtered) {{
  const rows = sortedRows(aggregateManagers(filtered), state.managerSortBy, state.managerSortDir);
  const tbody = document.querySelector('#managerTable tbody');
  if (!rows.length) {{
    tbody.innerHTML = '<tr><td colspan="6"><div class="empty-state">No managers in current filtered slice.</div></td></tr>';
    document.getElementById('managerMeta').textContent = 'Rows: 0';
    return;
  }}
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${{esc(r.account_manager_id)}}</td>
      <td>${{esc(r.team)}}</td>
      <td>${{fmtNum(r.portfolio_accounts, 0)}}</td>
      <td>${{fmtMoney(r.portfolio_mrr)}}</td>
      <td>${{fmtNum(r.avg_governance, 1)}}</td>
      <td>${{fmtNum(r.critical_accounts, 0)}}</td>
    </tr>
  `).join('');
  document.getElementById('managerMeta').textContent = `Rows: ${{rows.length.toLocaleString()}}`;
}}

function renderScenarioCards() {{
  const cards = (payload.scenario_cards || []).map(s => `
    <article class="kpi-card">
      <div class="kpi-label">${{esc(s.scenario)}}</div>
      <div class="kpi-value">${{fmtMoney(s.end_mrr)}}</div>
      <div class="kpi-foot">End MRR | growth ${{(Number(s.mrr_growth_pct || 0) * 100).toFixed(2)}}%</div>
      <div class="kpi-foot">vs base MRR: ${{fmtMoney(s.mrr_vs_base)}} | vs base ARR: ${{fmtMoney(s.arr_vs_base)}}</div>
    </article>
  `).join('');
  document.getElementById('scenarioCards').innerHTML = cards || '<div class="empty-state">Scenario table unavailable.</div>';
}}

function renderRiskImpact() {{
  const rows = payload.risk_impact || [];
  const tbody = document.querySelector('#riskImpactTable tbody');
  if (!rows.length) {{
    tbody.innerHTML = '<tr><td colspan="4"><div class="empty-state">Risk impact table unavailable.</div></td></tr>';
    return;
  }}
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${{esc(r.metric)}}</td>
      <td>${{Number(r.value || 0).toLocaleString('en-US', {{maximumFractionDigits: 2}})}}</td>
      <td>${{esc(r.unit)}}</td>
      <td>${{esc(r.definition)}}</td>
    </tr>
  `).join('');
}}

function renderMethodology() {{
  const m = payload.methodology || {{}};
  const blocks = [
    ['Glossary', (m.glossary || []).map(x => `${{x.term}}: ${{x.definition}}`)],
    ['Scoring Logic', m.scoring_logic || []],
    ['Assumptions', m.assumptions || []],
    ['Validation Notes', m.validation_notes || []],
    ['Caveats', m.caveats || []],
  ];
  document.getElementById('methodGrid').innerHTML = blocks.map(([title, items]) => `
    <div class="panel">
      <h4>${{esc(title)}}</h4>
      <ul>${{(items || []).map(i => `<li>${{esc(i)}}</li>`).join('')}}</ul>
    </div>
  `).join('');

  const sourceMap = payload.source_map || {{}};
  const srcHtml = Object.entries(sourceMap).map(([section, files]) => `
    <div class="panel" style="margin-bottom:8px;">
      <h3 style="margin:0 0 5px; font-size:0.84rem;">${{esc(section)}}</h3>
      <ul style="margin:0; padding-left:18px;">
        ${{(files || []).map(f => `<li style=\"font-size:0.82rem;\">${{esc(f)}}</li>`).join('')}}
      </ul>
    </div>
  `).join('');
  document.getElementById('sourceMap').innerHTML = srcHtml || '<div class="empty-state">No source map available.</div>';
}}

function renderSliceNotes(filtered) {{
  if (!filtered.length) {{
    document.getElementById('sliceNotes').textContent = 'No rows available under current filters. Reset filters or widen signup range.';
    return;
  }}
  const critical = filtered.filter(r => r.governance_priority_tier === 'Critical').length;
  const high = filtered.filter(r => r.governance_priority_tier === 'High').length;
  const heavyDiscount = filtered.filter(r => Number(r.discount_dependency_score || 0) >= 75).length;
  const note = [
    `Filtered universe contains ${{filtered.length.toLocaleString()}} accounts.`,
    `${{critical}} are Critical and ${{high}} are High governance priority.`,
    `${{heavyDiscount}} accounts show heavy discount-dependency signals.`,
    'Use this slice to prioritize interventions and renewal actions.',
  ].join(' ');
  document.getElementById('sliceNotes').textContent = note;
}}

function renderExecutiveNarrative(kpis, filteredCount) {{
  const total = accounts.length || 1;
  const scope = `${{filteredCount.toLocaleString()}} / ${{total.toLocaleString()}} accounts`;
  const narrative = [
    `Current filter scope covers ${{scope}}.`,
    `MRR is ${{fmtMoney(kpis.current_mrr)}} with NRR at ${{fmtPct(kpis.net_retention)}} and GRR at ${{fmtPct(kpis.gross_retention)}}.`,
    `Discounted revenue share is ${{fmtPct(kpis.discounted_revenue_share)}} and revenue-at-risk is ${{fmtMoney(kpis.revenue_at_risk_mrr)}}.`,
  ].join(' ');
  document.getElementById('executiveNarrative').textContent = narrative;
}

function renderFilterStateChip(filtered) {{
  const chip = document.getElementById('filterStateChip');
  if (!chip) return;
  const total = accounts.length || 1;
  const filteredCount = filtered.length;
  const share = (filteredCount / total) * 100;
  chip.textContent = `Filtered: ${{filteredCount.toLocaleString()}} / ${{total.toLocaleString()}} accounts (${{share.toFixed(1)}}%)`;
}}

function applyAll() {{
  const filtered = getFilteredAccounts();
  const filteredIds = new Set(filtered.map(r => String(r.customer_id)));
  const monthlyFiltered = aggregateMonthlyForFiltered(filteredIds);
  const filteredKpis = computeFilteredKpis(filtered, monthlyFiltered);

  renderOfficialKpis(filteredKpis, filtered.length);
  renderAlerts(filteredKpis, filtered.length);
  renderExecutiveNarrative(filteredKpis, filtered.length);
  renderInteractiveOverviewCharts(monthlyFiltered);
  renderFilterStateChip(filtered);
  renderSliceKpis(filtered);
  renderRiskBars(filtered);
  renderSliceNotes(filtered);
  renderInteractiveSliceDrill(filtered);
  renderAccountsTable(filtered);
  renderManagerTable(filtered);
}}

function bindSorting() {{
  document.querySelectorAll('#accountsTable th[data-sort]').forEach(th => {{
    th.addEventListener('click', () => {{
      const key = th.getAttribute('data-sort');
      if (state.sortBy === key) {{
        state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
      }} else {{
        state.sortBy = key;
        state.sortDir = 'desc';
      }}
      state.page = 1;
      applyAll();
    }});
  }});

  document.querySelectorAll('#managerTable th[data-sort]').forEach(th => {{
    th.addEventListener('click', () => {{
      const key = th.getAttribute('data-sort');
      if (state.managerSortBy === key) {{
        state.managerSortDir = state.managerSortDir === 'asc' ? 'desc' : 'asc';
      }} else {{
        state.managerSortBy = key;
        state.managerSortDir = 'desc';
      }}
      applyAll();
    }});
  }});
}}

function bindTabs() {{
  document.querySelectorAll('.tab-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const tab = btn.getAttribute('data-tab');
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
      const section = document.getElementById(`section_${{tab}}`);
      if (section) section.classList.add('active');
      window.scrollTo({{ top: 0, behavior: 'smooth' }});
    }});
  }});
}}

function bindTheme() {{
  const btn = document.getElementById('btn_theme');
  if (!btn) return;
  btn.addEventListener('click', () => {{
    const next = state.theme === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    saveThemePreference(next);
  }});
}}

function bindChartInteractions() {{
  document.querySelectorAll('.chart-density-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const density = btn.getAttribute('data-density');
      if (!density) return;
      state.chartDensity = density;
      applyDensity();
    }});
  }});

  const modal = document.getElementById('chartModal');
  const stage = document.getElementById('chartModalStage');

  document.getElementById('chartClose').addEventListener('click', closeChartModal);
  document.getElementById('chartPrev').addEventListener('click', () => moveChart(-1));
  document.getElementById('chartNext').addEventListener('click', () => moveChart(1));
  document.getElementById('chartZoomIn').addEventListener('click', () => {{
    modalState.zoom = Math.min(MAX_MODAL_ZOOM, modalState.zoom + 0.2);
    updateModalTransform();
  }});
  document.getElementById('chartZoomOut').addEventListener('click', () => {{
    modalState.zoom = Math.max(MIN_MODAL_ZOOM, modalState.zoom - 0.2);
    updateModalTransform();
  }});
  document.getElementById('chartZoomReset').addEventListener('click', () => {{
    modalState.zoom = MIN_MODAL_ZOOM;
    modalState.tx = 0;
    modalState.ty = 0;
    updateModalTransform();
  }});

  modal.addEventListener('click', (e) => {{
    if (e.target === modal) closeChartModal();
  }});

  stage.addEventListener('mousedown', (e) => {{
    if (modal.getAttribute('aria-hidden') === 'true') return;
    modalState.dragging = true;
    stage.classList.add('dragging');
    modalState.dragStartX = e.clientX - modalState.tx;
    modalState.dragStartY = e.clientY - modalState.ty;
  }});
  window.addEventListener('mouseup', () => {{
    modalState.dragging = false;
    stage.classList.remove('dragging');
  }});
  window.addEventListener('mousemove', (e) => {{
    if (!modalState.dragging) return;
    modalState.tx = e.clientX - modalState.dragStartX;
    modalState.ty = e.clientY - modalState.dragStartY;
    updateModalTransform();
  }});
  stage.addEventListener('wheel', (e) => {{
    if (modal.getAttribute('aria-hidden') === 'true') return;
    e.preventDefault();
    const direction = e.deltaY > 0 ? -0.1 : 0.1;
    modalState.zoom = Math.max(MIN_MODAL_ZOOM, Math.min(MAX_MODAL_ZOOM, modalState.zoom + direction));
    updateModalTransform();
  }}, {{ passive: false }});

  window.addEventListener('keydown', (e) => {{
    if (!modal.classList.contains('active')) return;
    if (e.key === 'Escape') closeChartModal();
    if (e.key === 'ArrowRight') moveChart(1);
    if (e.key === 'ArrowLeft') moveChart(-1);
  }});
}}

function bindFilters() {{
  const ids = ['f_region','f_segment','f_industry','f_plan','f_channel','f_manager','f_risk','f_start','f_end','f_search'];
  ids.forEach(id => {{
    document.getElementById(id).addEventListener('input', () => {{
      state.page = 1;
      applyAll();
    }});
    document.getElementById(id).addEventListener('change', () => {{
      state.page = 1;
      applyAll();
    }});
  }});

  document.getElementById('btn_reset').addEventListener('click', () => {{
    document.getElementById('f_region').value = 'All';
    document.getElementById('f_segment').value = 'All';
    document.getElementById('f_industry').value = 'All';
    document.getElementById('f_plan').value = 'All';
    document.getElementById('f_channel').value = 'All';
    document.getElementById('f_manager').value = 'All';
    document.getElementById('f_risk').value = 'All';
    document.getElementById('f_start').value = 'All';
    document.getElementById('f_end').value = 'All';
    document.getElementById('f_search').value = '';
    state.page = 1;
    applyAll();
  }});

  document.getElementById('acctPrev').addEventListener('click', () => {{
    state.page = Math.max(1, state.page - 1);
    applyAll();
  }});
  document.getElementById('acctNext').addEventListener('click', () => {{
    state.page += 1;
    applyAll();
  }});

  document.getElementById('btn_method').addEventListener('click', () => {{
    document.querySelector('[data-tab="method"]').click();
  }});

  document.getElementById('methodDrawerBtn').addEventListener('click', () => {{
    document.getElementById('methodDrawer').classList.toggle('active');
  }});
}}

function init() {{
  state.chartDensity = 'comfortable';
  applyTheme(loadThemePreference());
  populateSelect('f_region', filters.regions || []);
  populateSelect('f_segment', filters.segments || []);
  populateSelect('f_industry', filters.industries || []);
  populateSelect('f_plan', filters.plan_tiers || []);
  populateSelect('f_channel', filters.channels || []);
  populateSelect('f_manager', filters.account_managers || []);
  populateSelect('f_risk', filters.risk_tiers || []);
  populateSelect('f_start', filters.signup_months || []);
  populateSelect('f_end', filters.signup_months || []);

  renderCharts('execCharts', 'executive_overview');
  renderCharts('revenueCharts', 'revenue_quality');
  renderCharts('retentionCharts', 'retention_churn');
  renderCharts('riskCharts', 'account_risk');
  renderCharts('portfolioCharts', 'portfolio_manager');
  renderCharts('scenarioCharts', 'scenario_forecast');

  renderScenarioCards();
  renderRiskImpact();
  renderMethodology();

  applyDensity();
  bindTheme();
  bindTabs();
  bindChartInteractions();
  bindSorting();
  bindFilters();
  applyAll();
}}

init();
</script>
</body>
</html>
"""

    html = html_template.replace("{{", "{").replace("}}", "}")
    return html.replace("__PAYLOAD_JSON__", payload_json)


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    output_path = (base_dir / args.output).resolve()

    payload = build_payload(base_dir)
    html = build_html(payload)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    print("Executive dashboard generated")
    print(f"output: {output_path}")


if __name__ == "__main__":
    main()

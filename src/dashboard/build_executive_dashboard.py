from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from dashboard_contract import (
        CANONICAL_DASHBOARD_PATH,
        DASHBOARD_SECTIONS,
        OFFICIAL_KPI_SPECS,
        REDIRECT_ENTRYPOINTS,
    )
except ImportError:  # pragma: no cover - supports package-style imports in tests.
    from src.dashboard.dashboard_contract import (
        CANONICAL_DASHBOARD_PATH,
        DASHBOARD_SECTIONS,
        OFFICIAL_KPI_SPECS,
        REDIRECT_ENTRYPOINTS,
    )



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build executive offline dashboard with governed payload.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--output", type=str, default=CANONICAL_DASHBOARD_PATH)
    return parser.parse_args()


def _to_month(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m")


def _safe_float(value: Any, digits: int = 4) -> float:
    if pd.isna(value):
        return 0.0
    return round(float(value), digits)


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


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


def _png_data_uri(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _build_chart_catalog(base_dir: Path) -> list[dict[str, str]]:
    chart_specs = [
        (
            "chart_01_mrr_arr",
            "Recurring Revenue Is Growing, But Quality Guardrails Matter",
            "MRR/ARR trend over time",
            "Is recurring revenue compounding at a pace that justifies the current risk profile?",
            "Use this to separate headline growth from the quality controls needed to protect it.",
            "01_mrr_arr_growth_trend.png",
            "executive_overview",
        ),
        (
            "chart_02_grr_nrr",
            "Retention Quality Holds Near Parity, With Limited Expansion Cushion",
            "Gross vs net retention trend",
            "Is expansion large enough to offset churn and contraction?",
            "Use this before approving growth plans that depend on upsell masking base-book weakness.",
            "02_grr_nrr_retention_trend.png",
            "retention_churn",
        ),
        (
            "chart_03_churn_segment",
            "Churn Burden Is Uneven Across Segments",
            "Logo churn by segment",
            "Which segments are creating disproportionate logo churn pressure?",
            "Use this to target retention plays where churn is structurally concentrated.",
            "03_logo_churn_by_segment.png",
            "retention_churn",
        ),
        (
            "chart_04_concentration",
            "A Small Account Core Concentrates Disproportionate Revenue Exposure",
            "Revenue concentration curve",
            "How dependent is the portfolio on the largest accounts?",
            "Use this to size executive attention on concentration and renewal exposure.",
            "04_revenue_concentration_curve.png",
            "revenue_quality",
        ),
        (
            "chart_05_discount_mix",
            "Discount Behavior Differs Materially by Segment, Channel, and Manager",
            "Average discount by segment/channel/manager",
            "Where is discounting becoming a management behavior rather than a deal exception?",
            "Use this to focus pricing governance by segment, channel, or manager.",
            "05_average_discount_segment_channel_manager.png",
            "revenue_quality",
        ),
        (
            "chart_06_discount_share",
            "Discount-Dependent Revenue Share Stays Material",
            "Discounted revenue share trend",
            "Is more of the book becoming dependent on pricing concessions?",
            "Use this as the guardrail for margin quality and renewal-price discipline.",
            "06_discounted_revenue_share_trend.png",
            "revenue_quality",
        ),
        (
            "chart_07_churn_risk_dist",
            "Risk Distribution Is Skewed to Low, With a High-Impact Tail",
            "Churn risk score distribution",
            "Is churn exposure broad-based or concentrated in a manageable tail?",
            "Use this to decide whether the response is operating cadence or targeted recovery.",
            "07_churn_risk_score_distribution.png",
            "account_risk",
        ),
        (
            "chart_08_revenue_quality_dist",
            "Revenue Quality Scores Reveal Meaningful Fragility Pockets",
            "Revenue quality score distribution",
            "How much of the customer base has weak revenue quality signals?",
            "Use this to identify whether pricing, usage, and retention quality need systemic attention.",
            "08_revenue_quality_score_distribution.png",
            "account_risk",
        ),
        (
            "chart_09_expansion_quality",
            "Expansion Quality Is Strongest in Specific Segments Only",
            "Expansion quality by segment",
            "Which segments produce expansion that is likely to be durable?",
            "Use this to steer expansion effort toward segments with healthier quality signals.",
            "09_expansion_quality_by_segment.png",
            "revenue_quality",
        ),
        (
            "chart_10_governance_priority_accounts",
            "Priority Queue Is Concentrated in a Small Set of Accounts",
            "Top accounts by governance priority",
            "Which accounts should leadership act on first?",
            "Use this as the intervention queue for owner assignment and next-action tracking.",
            "10_top_accounts_governance_priority.png",
            "account_risk",
        ),
        (
            "chart_11_cohort_heatmap",
            "Cohort Retention Heatmap Highlights Uneven Durability",
            "Cohort retention heatmap",
            "Which cohorts are retaining value and which cohorts are degrading?",
            "Use this to separate onboarding quality issues from current-period commercial pressure.",
            "11_cohort_retention_heatmap.png",
            "retention_churn",
        ),
        (
            "chart_12_discount_vs_risk",
            "Higher Discount Intensity Is Associated with Higher Risk",
            "Discount vs churn risk",
            "Are heavily discounted accounts also more likely to churn?",
            "Use this to challenge concession-led retention and renewal strategies.",
            "12_discount_vs_churn_risk.png",
            "account_risk",
        ),
        (
            "chart_13_payment_vs_risk",
            "Payment Delay Is a Leading Commercial Risk Signal",
            "Payment delay vs churn risk",
            "Do payment delays identify churn pressure before renewal?",
            "Use this to trigger commercial follow-up before risk becomes a renewal event.",
            "13_payment_delay_vs_churn_risk.png",
            "account_risk",
        ),
        (
            "chart_14_usage_vs_risk",
            "Usage Deterioration Aligns with Elevated Churn Risk",
            "Usage decline vs churn risk",
            "Is usage decay explaining elevated churn risk?",
            "Use this to route recovery work between customer success and commercial teams.",
            "14_usage_decline_vs_churn_risk.png",
            "account_risk",
        ),
        (
            "chart_15_scenarios",
            "Fragile-Growth Case Compresses Near-Term MRR Trajectory",
            "Scenario comparison",
            "How much MRR is at stake if revenue quality deteriorates?",
            "Use this to quantify downside exposure and the value of intervention.",
            "15_scenario_mrr_comparison.png",
            "scenario_forecast",
        ),
    ]

    charts_dir = base_dir / "outputs" / "charts"
    catalog: list[dict[str, str]] = []
    for chart_id, title, subtitle, question, decision_use, filename, section in chart_specs:
        path = charts_dir / filename
        if not path.exists():
            continue
        catalog.append(
            {
                "chart_id": chart_id,
                "title": title,
                "subtitle": subtitle,
                "question": question,
                "decision_use": decision_use,
                "section": section,
                "filename": filename,
                "image_path": f"../charts/{filename}",
                "image_src": _png_data_uri(path),
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
            "validation_overall": validation_summary.get("overall_assessment", ""),
            "validation_readiness_tier": validation_summary.get("readiness", {}).get("tier", ""),
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
        "dashboard_contract": {
            "canonical_dashboard_path": CANONICAL_DASHBOARD_PATH,
            "redirect_entrypoints": list(REDIRECT_ENTRYPOINTS),
            "official_kpi_specs": list(OFFICIAL_KPI_SPECS),
            "sections": list(DASHBOARD_SECTIONS),
            "source_of_truth": "reports/main_business_analysis_metrics.json + governed processed tables",
        },
    }
    return payload


def build_html(payload: dict[str, Any]) -> str:
    """Render the dashboard HTML from the embedded template.

    The template (`_template.html`) is a static file held side-by-side with
    this module. Keeping markup, CSS, and JS in a real HTML file (rather
    than a multi-thousand-line Python f-string) makes the template
    syntax-highlightable, diff-friendly, and editable without touching
    Python. The single substitution point is the ``__PAYLOAD_JSON__``
    sentinel, which carries every figure and KPI consumed by the page.
    """
    payload_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    template_path = Path(__file__).parent / "_template.html"
    template = template_path.read_text(encoding="utf-8")
    return template.replace("__PAYLOAD_JSON__", payload_json)




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

"""Metric, scoring, forecast and leakage controls (checks 8–14)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.metrics import build_monthly_retention
from src.scoring.scoring_utils import quality_to_risk_tier, risk_tier
from src.validation.context import ValidationArtifacts, validate_dashboard_payload
from src.validation.models import Finding, add_finding


def run_analytical_checks(
    base_dir: Path,
    tables: dict[str, pd.DataFrame],
    findings: list[Finding],
) -> ValidationArtifacts:
    t = tables
    customers = t["customers"]
    mm = t["monthly_account_metrics"]
    amrq = t["account_monthly_revenue_quality"]
    chf = t["customer_health_features"]
    coh = t["cohort_retention_summary"]
    risk_base = t["account_risk_base"]
    scoring = t["account_scoring_model_output"]
    comp = t["account_scoring_components"]
    scen = t["scenario_mrr_trajectories"]
    scen_sum = t["mrr_scenario_table"]
    # 8) Retention denominator correctness
    monthly_ret = build_monthly_retention(amrq, mm)
    invalid_grr = int(((monthly_ret["grr"] < 0) | (monthly_ret["grr"] > 1.05)).sum())
    invalid_nrr = int((monthly_ret["nrr"] < 0).sum())

    metrics_json = json.loads(
        (base_dir / "reports" / "main_business_analysis_metrics.json").read_text(encoding="utf-8")
    )
    reported_latest_grr = float(metrics_json["section2"]["latest_grr"])
    reported_latest_nrr = float(metrics_json["section2"]["latest_nrr"])
    reported_latest_logo_churn = float(metrics_json["section2"]["latest_logo_churn_rate"])
    reported_latest_revenue_churn = float(metrics_json["section2"]["latest_revenue_churn_rate"])

    latest_calc = monthly_ret.iloc[-1]
    delta_grr = abs(float(latest_calc["grr"]) - reported_latest_grr)
    delta_nrr = abs(float(latest_calc["nrr"]) - reported_latest_nrr)
    delta_logo_churn = abs(float(latest_calc["logo_churn_rate"]) - reported_latest_logo_churn)
    delta_revenue_churn = abs(float(latest_calc["revenue_churn_rate"]) - reported_latest_revenue_churn)

    if (
        invalid_grr == 0
        and invalid_nrr == 0
        and delta_grr < 1e-6
        and delta_nrr < 1e-6
        and delta_logo_churn < 1e-6
        and delta_revenue_churn < 1e-6
    ):
        add_finding(
            findings,
            "8",
            "Retention Denominator Correctness",
            "Metrics",
            "PASS",
            "None",
            "GRR/NRR and logo/revenue churn use beginning-base denominators, exclude new logos, and reconcile exactly to reported latest values.",
        )
    else:
        add_finding(
            findings,
            "8",
            "Retention Denominator Correctness",
            "Metrics",
            "WARN",
            "Medium",
            (
                f"invalid_grr_rows={invalid_grr}, invalid_nrr_rows={invalid_nrr}, "
                f"latest_delta_grr={delta_grr:.6f}, latest_delta_nrr={delta_nrr:.6f}, "
                f"latest_delta_logo_churn={delta_logo_churn:.6f}, "
                f"latest_delta_revenue_churn={delta_revenue_churn:.6f}"
            ),
            "Reconcile denominator definitions and update memo definitions if methodology differs.",
        )

    # 9) Cohort logic correctness
    coh_check = coh.copy()
    coh_check["cohort_key"] = (
        coh_check["cohort_month"].dt.strftime("%Y-%m")
        + "|"
        + coh_check["segment"].astype(str)
        + "|"
        + coh_check["region"].astype(str)
    )
    month0 = coh_check[coh_check["month_number"] == 0]
    month0_grr_ok = int((month0["gross_retention_rate"].sub(1).abs() < 1e-9).sum())
    month0_nrr_ok = int((month0["net_retention_rate"].sub(1).abs() < 1e-9).sum())
    month0_total = int(len(month0))

    coh_check["cohort_revenue_implied"] = np.where(
        coh_check["net_retention_rate"] > 0,
        coh_check["retained_revenue"] / coh_check["net_retention_rate"],
        np.nan,
    )
    spread = coh_check.groupby("cohort_key")["cohort_revenue_implied"].agg(["min", "max"])
    spread["rel_spread"] = (spread["max"] - spread["min"]) / spread["max"].replace(0, np.nan)
    unstable_denoms = int((spread["rel_spread"] > 0.01).sum())

    if month0_total > 0 and month0_grr_ok == month0_total and month0_nrr_ok == month0_total and unstable_denoms == 0:
        add_finding(
            findings,
            "9",
            "Cohort Logic Correctness",
            "Features/Metrics",
            "PASS",
            "None",
            "Cohort month 0 starts at GRR=NRR=100% and implied cohort denominator remains stable across age buckets.",
        )
    else:
        add_finding(
            findings,
            "9",
            "Cohort Logic Correctness",
            "Features/Metrics",
            "FAIL",
            "High",
            (
                f"month0_grr_ok={month0_grr_ok}/{month0_total}, month0_nrr_ok={month0_nrr_ok}/{month0_total}, "
                f"unstable_denominators={unstable_denoms}"
            ),
            "Rebuild cohort table ensuring fixed cohort denominator and explicit month-0 baseline.",
        )

    # 10) Score range correctness
    score_cols = [
        "churn_risk_score",
        "revenue_quality_score",
        "discount_dependency_score",
        "expansion_quality_score",
        "governance_priority_score",
    ]
    out_of_range = {c: int(((scoring[c] < 0) | (scoring[c] > 100)).sum()) for c in score_cols}
    if sum(out_of_range.values()) == 0:
        add_finding(
            findings, "10", "Score Range Correctness", "Scoring", "PASS", "None", "All scoring outputs are in [0,100]."
        )
    else:
        bad = [f"{k}={v}" for k, v in out_of_range.items() if v > 0]
        add_finding(
            findings,
            "10",
            "Score Range Correctness",
            "Scoring",
            "FAIL",
            "High",
            "Out-of-range score values detected: " + ", ".join(bad),
            "Clamp score outputs to [0,100] and add score-range assertions.",
        )

    # 11) Risk tier assignment consistency
    tier_mismatch = {
        "churn_risk_tier": int(
            (scoring.apply(lambda r: risk_tier(float(r["churn_risk_score"])) != r["churn_risk_tier"], axis=1)).sum()
        ),
        "discount_dependency_tier": int(
            (
                scoring.apply(
                    lambda r: risk_tier(float(r["discount_dependency_score"])) != r["discount_dependency_tier"], axis=1
                )
            ).sum()
        ),
        "governance_priority_tier": int(
            (
                scoring.apply(
                    lambda r: risk_tier(float(r["governance_priority_score"])) != r["governance_priority_tier"], axis=1
                )
            ).sum()
        ),
        "revenue_quality_risk_tier": int(
            (
                scoring.apply(
                    lambda r: quality_to_risk_tier(float(r["revenue_quality_score"])) != r["revenue_quality_risk_tier"],
                    axis=1,
                )
            ).sum()
        ),
        "expansion_quality_risk_tier": int(
            (
                scoring.apply(
                    lambda r: (
                        quality_to_risk_tier(float(r["expansion_quality_score"])) != r["expansion_quality_risk_tier"]
                    ),
                    axis=1,
                )
            ).sum()
        ),
    }
    if sum(tier_mismatch.values()) == 0:
        add_finding(
            findings,
            "11",
            "Risk Tier Assignment Consistency",
            "Scoring",
            "PASS",
            "None",
            "All tier labels match threshold rules.",
        )
    else:
        bad = [f"{k}={v}" for k, v in tier_mismatch.items() if v > 0]
        add_finding(
            findings,
            "11",
            "Risk Tier Assignment Consistency",
            "Scoring",
            "FAIL",
            "High",
            "Tier mismatches detected: " + ", ".join(bad),
            "Recompute tier assignment with shared helper functions.",
        )

    # 12) Scenario calculation integrity
    row_calc = (
        scen["start_mrr"] + scen["expansion_mrr"] - scen["contraction_mrr"] - scen["churn_mrr"] + scen["net_new_mrr"]
    )
    row_mismatch = int((row_calc.sub(scen["forecast_mrr"]).abs() > 0.05).sum())
    arr_mismatch = int((scen["forecast_arr"].sub(scen["forecast_mrr"] * 12).abs() > 0.1).sum())
    realized_arr_mismatch = int(
        (
            scen["realized_arr_estimate"].sub(scen["forecast_arr"] * scen["realized_price_index_assumption"]).abs()
            > 0.1
        ).sum()
    )

    summary_mismatch = 0
    for _, row in scen_sum.iterrows():
        g = scen[scen["scenario"] == row["scenario"]].sort_values("forecast_month")
        if len(g) == 0:
            summary_mismatch += 1
            continue
        if abs(float(g.iloc[-1]["forecast_mrr"]) - float(row["end_mrr"])) > 0.1:
            summary_mismatch += 1

    if row_mismatch == 0 and summary_mismatch == 0 and arr_mismatch == 0 and realized_arr_mismatch == 0:
        add_finding(
            findings,
            "12",
            "Scenario Calculation Integrity",
            "Forecasting",
            "PASS",
            "None",
            "Scenario row math and summary rollups fully reconcile.",
        )
    elif row_mismatch == 0 and summary_mismatch == 0 and arr_mismatch <= 10 and realized_arr_mismatch <= 40:
        add_finding(
            findings,
            "12",
            "Scenario Calculation Integrity",
            "Forecasting",
            "WARN",
            "Low",
            (
                f"Minor rounding-level arithmetic deltas: forecast_arr_mismatch_rows={arr_mismatch}, "
                f"realized_arr_mismatch_rows={realized_arr_mismatch}; row_mismatch={row_mismatch}, summary_mismatch={summary_mismatch}."
            ),
            "Document rounding policy or round derived fields consistently at final write step.",
        )
    else:
        add_finding(
            findings,
            "12",
            "Scenario Calculation Integrity",
            "Forecasting",
            "FAIL",
            "High",
            (
                f"scenario_row_mismatch={row_mismatch}, forecast_arr_mismatch={arr_mismatch}, "
                f"realized_arr_mismatch={realized_arr_mismatch}, summary_mismatch={summary_mismatch}"
            ),
            "Recompute forecast equations and regenerate scenario outputs.",
        )

    # 13) Join inflation risk + dashboard feed tables
    uniq_counts = {
        "customer_health_features": int(chf["customer_id"].nunique()),
        "account_risk_base": int(risk_base["customer_id"].nunique()),
        "account_scoring_model_output": int(scoring["customer_id"].nunique()),
        "account_scoring_components": int(comp["customer_id"].nunique()),
    }
    uniq_ok = all(v == len(customers) for v in uniq_counts.values())

    dashboard = validate_dashboard_payload(base_dir)
    dashboard_ok = False
    dashboard_detail = "dashboard_not_found"
    dashboard_payload: dict[str, Any] = {}
    if dashboard.get("exists") and dashboard.get("embedded_json_found"):
        payload = dashboard["payload"]
        dashboard_payload = payload
        db_accounts = payload.get("accounts", [])
        db_kpis = payload.get("official_kpis", {})
        db_charts = payload.get("chart_catalog", [])
        meta = payload.get("meta", {})
        coverage = meta.get("data_coverage", {})
        required_keys = {
            "meta",
            "official_kpis",
            "accounts",
            "monthly_summary",
            "scenario_cards",
            "scenario_trajectory",
            "chart_catalog",
            "dashboard_contract",
        }
        dashboard_ok = (
            required_keys.issubset(payload.keys())
            and len(db_accounts) == len(customers)
            and len(db_charts) >= 15
            and "current_mrr" in db_kpis
            and "arr" in db_kpis
            and coverage.get("month_start") == str(amrq["month"].min().date())[:7]
            and coverage.get("month_end") == str(amrq["month"].max().date())[:7]
        )
        dashboard_detail = (
            f"accounts={len(db_accounts)}, charts={len(db_charts)}, "
            f"coverage={coverage.get('month_start')}..{coverage.get('month_end')}"
        )

    profiling_memo_path = base_dir / "reports" / "data_profiling_memo.md"
    analysis_metrics_path = base_dir / "reports" / "main_business_analysis_metrics.json"
    analysis_memo_path = base_dir / "reports" / "main_business_analysis_memo.md"

    profiling_ok = profiling_memo_path.exists()
    analysis_ok = analysis_metrics_path.exists() and analysis_memo_path.exists()
    analysis_payload: dict[str, Any] = {}
    analysis_detail = "analysis_artifacts_missing"
    if analysis_ok:
        analysis_payload = json.loads(analysis_metrics_path.read_text(encoding="utf-8"))
        meta = analysis_payload.get("meta", {})
        section2 = analysis_payload.get("section2", {})
        analysis_ok = (
            bool(meta.get("month_end"))
            and ("latest_grr" in section2)
            and ("latest_nrr" in section2)
            and ("latest_logo_churn_rate" in section2)
            and ("latest_revenue_churn_rate" in section2)
        )
        analysis_detail = (
            f"month_end={meta.get('month_end', '')}, "
            f"latest_grr_present={'latest_grr' in section2}, latest_nrr_present={'latest_nrr' in section2}, "
            f"latest_logo_churn_present={'latest_logo_churn_rate' in section2}, "
            f"latest_revenue_churn_present={'latest_revenue_churn_rate' in section2}"
        )

    profiling_detail = "profiling_artifacts_present" if profiling_ok else "profiling_artifacts_missing"
    artifacts_ok = profiling_ok and analysis_ok

    if uniq_ok and dashboard_ok and artifacts_ok:
        add_finding(
            findings,
            "13",
            "Join Inflation Risk + Dashboard Feed Integrity",
            "Processed/Dashboard",
            "PASS",
            "None",
            (
                "Account-level table joins remain 1:1; dashboard embedded payload reconciles to processed row counts; "
                "profiling memo and analysis artifacts are present with required metric keys."
            ),
        )
    else:
        add_finding(
            findings,
            "13",
            "Join Inflation Risk + Dashboard Feed Integrity",
            "Processed/Dashboard",
            "FAIL",
            "High",
            (
                f"unique_counts={uniq_counts}; dashboard_detail={dashboard_detail}; "
                f"{profiling_detail}; {analysis_detail}"
            ),
            "Fix join keys in analytical layer/dashboard build and enforce uniqueness assertions.",
        )

    # 14) Leakage risk
    latest_raw_month = mm["month"].max()
    latest_quality_month = amrq["month"].max()
    # Direct guard: the historical-churn feature must exclude the current
    # snapshot month's churn event.
    same_month_churn = mm[mm["month"] == latest_raw_month][["customer_id", "churn_flag"]]
    history_probe = chf[["customer_id", "churn_history_flag"]].merge(
        same_month_churn,
        on="customer_id",
        how="left",
        validate="one_to_one",
    )
    concurrent_history_leaks = int(
        ((history_probe["churn_flag"] == 1) & (history_probe["churn_history_flag"] == 1)).sum()
    )
    leak_probe = (
        scoring[["customer_id", "churn_risk_score"]]
        .merge(same_month_churn, on="customer_id", how="left")
        .fillna({"churn_flag": 0})
    )
    corr_same_month = float(leak_probe["churn_risk_score"].corr(leak_probe["churn_flag"]))

    if latest_quality_month <= latest_raw_month and concurrent_history_leaks == 0 and abs(corr_same_month) < 0.2:
        add_finding(
            findings,
            "14",
            "Leakage Risk",
            "Features/Scoring",
            "PASS",
            "None",
            (
                f"No future-date leakage detected (latest_processed_month={latest_quality_month.date()}, latest_raw_month={latest_raw_month.date()}); "
                f"concurrent_history_leaks={concurrent_history_leaks}; same-month churn correlation probe={corr_same_month:.3f}."
            ),
        )
    else:
        add_finding(
            findings,
            "14",
            "Leakage Risk",
            "Features/Scoring",
            "FAIL",
            "High",
            (
                f"Potential leakage signal: latest_processed_month={latest_quality_month.date()}, latest_raw_month={latest_raw_month.date()}, "
                f"concurrent_history_leaks={concurrent_history_leaks}, same_month_corr={corr_same_month:.3f}"
            ),
            "Apply strict temporal feature cutoffs and rerun feature/scoring pipeline.",
        )

    return ValidationArtifacts(
        dashboard=dashboard,
        dashboard_payload=dashboard_payload,
        analysis_payload=analysis_payload,
    )

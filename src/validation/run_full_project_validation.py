from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


@dataclass
class Finding:
    check_id: str
    check_name: str
    component: str
    status: str  # PASS, WARN, FAIL
    severity: str  # None, Low, Medium, High, Critical
    details: str
    recommended_fix: str
    fix_applied: str


SEVERITY_ORDER = {"Critical": 5, "High": 4, "Medium": 3, "Low": 2, "None": 1}
STATUS_ORDER = {"FAIL": 3, "WARN": 2, "PASS": 1}
READINESS_ORDER = {
    "publish-blocked": 1,
    "not committee-grade": 2,
    "screening-grade only": 3,
    "decision-support only": 4,
    "analytically acceptable": 5,
    "technically valid": 6,
}


def add_finding(
    findings: List[Finding],
    check_id: str,
    check_name: str,
    component: str,
    status: str,
    severity: str,
    details: str,
    recommended_fix: str = "None",
    fix_applied: str = "No",
) -> None:
    findings.append(
        Finding(
            check_id=check_id,
            check_name=check_name,
            component=component,
            status=status,
            severity=severity,
            details=details,
            recommended_fix=recommended_fix,
            fix_applied=fix_applied,
        )
    )


def risk_tier(score: float) -> str:
    if score < 30:
        return "Low"
    if score < 55:
        return "Moderate"
    if score < 75:
        return "High"
    return "Critical"


def quality_to_risk_tier(score: float) -> str:
    return risk_tier(100.0 - score)


def load_tables(base_dir: Path) -> Dict[str, pd.DataFrame]:
    raw = base_dir / "data" / "raw"
    processed = base_dir / "data" / "processed"

    return {
        "customers": pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"]),
        "plans": pd.read_csv(raw / "plans.csv"),
        "subscriptions": pd.read_csv(
            raw / "subscriptions.csv",
            parse_dates=["subscription_start_date", "subscription_end_date"],
        ),
        "monthly_account_metrics": pd.read_csv(raw / "monthly_account_metrics.csv", parse_dates=["month"]),
        "invoices": pd.read_csv(raw / "invoices.csv", parse_dates=["invoice_month"]),
        "account_managers": pd.read_csv(raw / "account_managers.csv"),
        "account_monthly_revenue_quality": pd.read_csv(processed / "account_monthly_revenue_quality.csv", parse_dates=["month"]),
        "customer_health_features": pd.read_csv(processed / "customer_health_features.csv"),
        "cohort_retention_summary": pd.read_csv(processed / "cohort_retention_summary.csv", parse_dates=["cohort_month"]),
        "account_risk_base": pd.read_csv(processed / "account_risk_base.csv", parse_dates=["current_month"]),
        "account_manager_summary": pd.read_csv(processed / "account_manager_summary.csv"),
        "account_scoring_model_output": pd.read_csv(processed / "account_scoring_model_output.csv"),
        "account_scoring_components": pd.read_csv(processed / "account_scoring_components.csv"),
        "scenario_mrr_trajectories": pd.read_csv(processed / "scenario_mrr_trajectories.csv", parse_dates=["forecast_month"]),
        "mrr_scenario_table": pd.read_csv(processed / "mrr_scenario_table.csv"),
        "commercial_risk_impact_estimates": pd.read_csv(processed / "commercial_risk_impact_estimates.csv"),
        "main_metrics_json": pd.DataFrame([json.loads((base_dir / "reports" / "main_business_analysis_metrics.json").read_text())]),
    }


def validate_dashboard_payload(base_dir: Path, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    html_path = base_dir / "outputs" / "dashboard" / "executive_dashboard.html"
    if not html_path.exists():
        return {"exists": False}

    html = html_path.read_text(encoding="utf-8")
    match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, flags=re.S)
    if not match:
        return {"exists": True, "embedded_json_found": False}

    payload = json.loads(match.group(1))
    return {
        "exists": True,
        "embedded_json_found": True,
        "payload": payload,
    }


def run_validation(base_dir: Path) -> tuple[List[Finding], Dict[str, Any]]:
    t = load_tables(base_dir)
    findings: List[Finding] = []
    summary: Dict[str, Any] = {}

    customers = t["customers"]
    plans = t["plans"]
    subs = t["subscriptions"]
    mm = t["monthly_account_metrics"]
    inv = t["invoices"]
    amrq = t["account_monthly_revenue_quality"]
    chf = t["customer_health_features"]
    coh = t["cohort_retention_summary"]
    risk_base = t["account_risk_base"]
    scoring = t["account_scoring_model_output"]
    comp = t["account_scoring_components"]
    scen = t["scenario_mrr_trajectories"]
    scen_sum = t["mrr_scenario_table"]

    # 1) Row count sanity
    row_msgs = []
    conds = []
    conds.append(2000 <= len(customers) <= 8000)
    row_msgs.append(f"customers={len(customers)}")
    conds.append(len(amrq) == len(mm))
    row_msgs.append(f"account_monthly_revenue_quality={len(amrq)}, monthly_account_metrics={len(mm)}")
    conds.append(len(chf) == len(customers) == len(scoring) == len(risk_base) == len(comp))
    row_msgs.append(
        f"account-level tables rows: chf={len(chf)}, scoring={len(scoring)}, risk={len(risk_base)}, components={len(comp)}"
    )

    expected_scen_rows = int(scen_sum["horizon_months"].iloc[0]) * len(scen_sum) if len(scen_sum) > 0 else 0
    conds.append(len(scen) == expected_scen_rows)
    row_msgs.append(f"scenario_mrr_trajectories={len(scen)}, expected={expected_scen_rows}")

    if all(conds):
        add_finding(findings, "1", "Row Count Sanity", "Raw/Processed", "PASS", "None", " | ".join(row_msgs))
    else:
        add_finding(
            findings,
            "1",
            "Row Count Sanity",
            "Raw/Processed",
            "FAIL",
            "High",
            " | ".join(row_msgs),
            "Re-run generation and transformation pipeline with row-level assertions.",
        )

    # 2) Null checks
    key_null_checks = {
        "customers.customer_id": customers["customer_id"].isna().mean(),
        "subscriptions.customer_id": subs["customer_id"].isna().mean(),
        "subscriptions.subscription_start_date": subs["subscription_start_date"].isna().mean(),
        "monthly_account_metrics.customer_id": mm["customer_id"].isna().mean(),
        "monthly_account_metrics.month": mm["month"].isna().mean(),
        "account_monthly_revenue_quality.customer_id": amrq["customer_id"].isna().mean(),
        "account_monthly_revenue_quality.month": amrq["month"].isna().mean(),
        "customer_health_features.customer_id": chf["customer_id"].isna().mean(),
        "account_scoring_model_output.customer_id": scoring["customer_id"].isna().mean(),
        "scenario_mrr_trajectories.forecast_month": scen["forecast_month"].isna().mean(),
    }
    max_null = max(key_null_checks.values())
    if max_null == 0:
        add_finding(findings, "2", "Null Checks", "Raw/Processed", "PASS", "None", "All key fields have 0.00% null rate.")
    else:
        bad = [f"{k}={v:.2%}" for k, v in key_null_checks.items() if v > 0]
        add_finding(
            findings,
            "2",
            "Null Checks",
            "Raw/Processed",
            "FAIL",
            "High",
            "Key null rates detected: " + ", ".join(bad),
            "Backfill critical keys and enforce non-null constraints in generation/transformation.",
        )

    # 3) Duplicate checks
    dup_checks = {
        "customers.customer_id": int(customers.duplicated("customer_id").sum()),
        "plans.plan_id": int(plans.duplicated("plan_id").sum()),
        "subscriptions.subscription_id": int(subs.duplicated("subscription_id").sum()),
        "monthly_account_metrics(customer_id,month)": int(mm.duplicated(["customer_id", "month"]).sum()),
        "invoices.invoice_id": int(inv.duplicated("invoice_id").sum()),
        "account_monthly_revenue_quality(customer_id,month)": int(amrq.duplicated(["customer_id", "month"]).sum()),
        "customer_health_features.customer_id": int(chf.duplicated("customer_id").sum()),
        "cohort_retention_summary(cohort,segment,region,month_number)": int(
            coh.duplicated(["cohort_month", "segment", "region", "month_number"]).sum()
        ),
        "account_scoring_model_output.customer_id": int(scoring.duplicated("customer_id").sum()),
    }
    if sum(dup_checks.values()) == 0:
        add_finding(findings, "3", "Duplicate Checks", "Raw/Processed", "PASS", "None", "No duplicate primary-key rows detected.")
    else:
        bad = [f"{k}={v}" for k, v in dup_checks.items() if v > 0]
        add_finding(
            findings,
            "3",
            "Duplicate Checks",
            "Raw/Processed",
            "FAIL",
            "Critical",
            "Duplicate keys detected: " + ", ".join(bad),
            "Deduplicate upstream and add unique-key assertions in build scripts.",
        )

    # 4) Impossible values
    effective_adjust_col = "effective_revenue_adjustment_amount" if "effective_revenue_adjustment_amount" in inv.columns else "discount_amount"
    impossible = {
        "subscriptions.discount_pct_out_of_range": int(((subs["discount_pct"] < 0) | (subs["discount_pct"] > 1)).sum()),
        "subscriptions.active_nonpositive_contracted_mrr": int(((subs["status"] == "active") & (subs["contracted_mrr"] <= 0)).sum()),
        "subscriptions.realized_gt_120pct_contracted": int((subs["realized_mrr"] > (subs["contracted_mrr"] * 1.2)).sum()),
        "monthly.flags_not_binary": int(
            ((~mm["active_flag"].isin([0, 1])) | (~mm["churn_flag"].isin([0, 1])) | (~mm["renewal_due_flag"].isin([0, 1]))).sum()
        ),
        "monthly.usage_outside_0_100": int(((mm["product_usage_score"] < 0) | (mm["product_usage_score"] > 100)).sum()),
        "monthly.nps_outside_-100_100": int(((mm["nps_score"] < -100) | (mm["nps_score"] > 100)).sum()),
        "invoices.negative_discount_amount": int((inv["discount_amount"] < 0).sum()),
        "invoices.negative_collection_loss_amount": int((inv.get("collection_loss_amount", pd.Series([0] * len(inv))) < 0).sum()),
        "invoices.effective_adjustment_gt_billed": int((inv[effective_adjust_col] > inv["billed_mrr"] + 1e-6).sum()),
        "invoices.realized_gt_105pct_billed": int((inv["realized_mrr"] > (inv["billed_mrr"] * 1.05)).sum()),
        "amrq.realized_price_index_gt_1p2": int((amrq["realized_price_index"] > 1.2).sum()),
    }
    if sum(impossible.values()) == 0:
        add_finding(findings, "4", "Impossible Values", "Raw/Processed", "PASS", "None", "No impossible-value violations in checked fields.")
    else:
        bad = [f"{k}={v}" for k, v in impossible.items() if v > 0]
        add_finding(
            findings,
            "4",
            "Impossible Values",
            "Raw/Processed",
            "FAIL",
            "High",
            "Impossible-value violations: " + ", ".join(bad),
            "Tighten simulation constraints and enforce value-range assertions.",
        )

    # 5) Date logic consistency
    first_sub = subs.groupby("customer_id", as_index=False)["subscription_start_date"].min().rename(
        columns={"subscription_start_date": "first_subscription_start"}
    )
    signup_cmp = customers.merge(first_sub, on="customer_id", how="left")
    signup_after_first_sub = int((signup_cmp["signup_date"] > signup_cmp["first_subscription_start"]).sum())
    sub_end_before_start = int((subs["subscription_end_date"] < subs["subscription_start_date"]).sum())

    max_raw_month = mm["month"].max()
    max_processed_month = amrq["month"].max()
    future_processed_rows = int((amrq["month"] > max_raw_month).sum())

    if signup_after_first_sub == 0 and sub_end_before_start == 0 and future_processed_rows == 0 and max_raw_month == max_processed_month:
        add_finding(
            findings,
            "5",
            "Date Logic Consistency",
            "Raw/Features",
            "PASS",
            "None",
            "Signup/subscription chronology and processed coverage are consistent.",
        )
    else:
        details = (
            f"signup_after_first_subscription={signup_after_first_sub}; "
            f"subscription_end_before_start={sub_end_before_start}; "
            f"future_processed_rows={future_processed_rows}; "
            f"max_raw_month={max_raw_month.date()}, max_processed_month={max_processed_month.date()}"
        )
        severity = "High" if signup_after_first_sub > 0 else "Medium"
        add_finding(
            findings,
            "5",
            "Date Logic Consistency",
            "Raw/Features",
            "FAIL" if signup_after_first_sub > 0 else "WARN",
            severity,
            details,
            "In data generation, constrain signup_date <= first subscription_start_date; regenerate dependent tables.",
        )

    # 6) Revenue reconciliation checks
    subs_monthly = subs.rename(columns={"subscription_start_date": "month"})[["customer_id", "month", "contracted_mrr"]]
    rev_cmp = (
        amrq[["customer_id", "month", "active_mrr"]]
        .merge(mm[["customer_id", "month", "active_flag"]], on=["customer_id", "month"], how="left")
        .merge(subs_monthly, on=["customer_id", "month"], how="left")
    )
    rev_cmp["expected_active_mrr"] = np.where(rev_cmp["active_flag"] == 1, rev_cmp["contracted_mrr"].fillna(0.0), 0.0)
    rev_cmp["delta"] = (rev_cmp["active_mrr"] - rev_cmp["expected_active_mrr"]).abs()
    rev_mismatch_rows = int((rev_cmp["delta"] > 0.01).sum())

    latest_month = amrq["month"].max()
    latest_mrr_from_monthly = (
        amrq[amrq["month"] == latest_month].groupby("customer_id", as_index=False)["active_mrr"].sum().rename(columns={"active_mrr": "mrr_latest"})
    )
    latest_mrr_from_scores = scoring[["customer_id", "current_mrr"]]
    current_cmp = latest_mrr_from_monthly.merge(latest_mrr_from_scores, on="customer_id", how="outer").fillna(0.0)
    current_mrr_mismatch = int(((current_cmp["mrr_latest"] - current_cmp["current_mrr"]).abs() > 0.01).sum())

    if rev_mismatch_rows == 0 and current_mrr_mismatch == 0:
        add_finding(
            findings,
            "6",
            "Revenue Reconciliation",
            "Processed/Metrics",
            "PASS",
            "None",
            "Account-month active_mrr reconciles to subscription contracted_mrr and latest current_mrr reconciles to scoring output.",
        )
    else:
        add_finding(
            findings,
            "6",
            "Revenue Reconciliation",
            "Processed/Metrics",
            "FAIL",
            "High",
            f"account_month_mrr_mismatches={rev_mismatch_rows}; current_mrr_mismatches={current_mrr_mismatch}",
            "Trace join keys in feature layer and correct row-level revenue lineage before analysis use.",
        )

    # 7) Discount logic consistency
    effective_adjust = inv[effective_adjust_col]
    implied_adjust = (inv["billed_mrr"] - inv["realized_mrr"]).clip(lower=0.0)
    effective_diff = (effective_adjust - implied_adjust).abs()
    effective_over_2c = int((effective_diff > 0.02).sum())
    effective_over_2c_rate = effective_over_2c / max(len(inv), 1)

    component_diff = pd.Series([0.0] * len(inv))
    has_collection = "collection_loss_amount" in inv.columns
    if has_collection:
        component_sum = (inv["discount_amount"] + inv["collection_loss_amount"]).clip(lower=0.0)
        component_diff = (component_sum - effective_adjust).abs()
    component_over_2c = int((component_diff > 0.02).sum()) if has_collection else 0
    component_over_2c_rate = component_over_2c / max(len(inv), 1) if has_collection else 0.0

    if effective_over_2c_rate <= 0.0025 and component_over_2c_rate <= 0.0025:
        add_finding(
            findings,
            "7",
            "Discount Logic Consistency",
            "Raw/Processed",
            "PASS",
            "None",
            (
                f"Invoice effective-adjustment arithmetic mismatches >2c in {effective_over_2c_rate:.2%} of rows; "
                f"component sum mismatches >2c in {component_over_2c_rate:.2%} of rows."
            ),
        )
    else:
        add_finding(
            findings,
            "7",
            "Discount Logic Consistency",
            "Raw/Processed",
            "WARN",
            "Medium",
            (
                f"effective_adjustment_mismatch_rows={effective_over_2c} ({effective_over_2c_rate:.2%}), "
                f"component_sum_mismatch_rows={component_over_2c} ({component_over_2c_rate:.2%}), "
                f"max_effective_diff={effective_diff.max():.4f}."
            ),
            "Align effective adjustment to billed-realized arithmetic and ensure discount + collection components reconcile.",
        )

    # 8) Retention denominator correctness
    panel = amrq.merge(mm[["customer_id", "month", "active_flag", "churn_flag"]], on=["customer_id", "month"], how="left")
    active = panel[panel["active_flag"] == 1].copy()
    monthly_ret = active.groupby("month", as_index=False).agg(
        mrr=("active_mrr", "sum"),
        expansion_mrr=("expansion_mrr", "sum"),
        contraction_mrr=("contraction_mrr", "sum"),
    )
    churn_mrr = (
        active[active["churn_flag"] == 1].groupby("month", as_index=False)["active_mrr"].sum().rename(columns={"active_mrr": "churn_mrr"})
    )
    monthly_ret = monthly_ret.merge(churn_mrr, on="month", how="left").fillna({"churn_mrr": 0.0}).sort_values("month")

    monthly_ret["grr"] = np.where(
        monthly_ret["mrr"] > 0,
        (monthly_ret["mrr"] - monthly_ret["contraction_mrr"] - monthly_ret["churn_mrr"]) / monthly_ret["mrr"],
        np.nan,
    )
    monthly_ret["nrr"] = np.where(
        monthly_ret["mrr"] > 0,
        (monthly_ret["mrr"] + monthly_ret["expansion_mrr"] - monthly_ret["contraction_mrr"] - monthly_ret["churn_mrr"]) / monthly_ret["mrr"],
        np.nan,
    )
    invalid_grr = int(((monthly_ret["grr"] < 0) | (monthly_ret["grr"] > 1.05)).sum())
    invalid_nrr = int((monthly_ret["nrr"] < 0).sum())

    metrics_json = json.loads((base_dir / "reports" / "main_business_analysis_metrics.json").read_text())
    reported_latest_grr = float(metrics_json["section2"]["latest_grr"])
    reported_latest_nrr = float(metrics_json["section2"]["latest_nrr"])

    latest_calc = monthly_ret.iloc[-1]
    delta_grr = abs(float(latest_calc["grr"]) - reported_latest_grr)
    delta_nrr = abs(float(latest_calc["nrr"]) - reported_latest_nrr)

    if invalid_grr == 0 and invalid_nrr == 0 and delta_grr < 1e-6 and delta_nrr < 1e-6:
        add_finding(
            findings,
            "8",
            "Retention Denominator Correctness",
            "Metrics",
            "PASS",
            "None",
            "GRR/NRR denominators are positive and reported latest values reconcile exactly to recomputation.",
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
                f"latest_delta_grr={delta_grr:.6f}, latest_delta_nrr={delta_nrr:.6f}"
            ),
            "Reconcile denominator definitions and update memo definitions if methodology differs.",
        )

    # 9) Cohort logic correctness
    coh_check = coh.copy()
    coh_check["cohort_key"] = (
        coh_check["cohort_month"].dt.strftime("%Y-%m") + "|" + coh_check["segment"].astype(str) + "|" + coh_check["region"].astype(str)
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
        add_finding(findings, "10", "Score Range Correctness", "Scoring", "PASS", "None", "All scoring outputs are in [0,100].")
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
        "churn_risk_tier": int((scoring.apply(lambda r: risk_tier(float(r["churn_risk_score"])) != r["churn_risk_tier"], axis=1)).sum()),
        "discount_dependency_tier": int((scoring.apply(lambda r: risk_tier(float(r["discount_dependency_score"])) != r["discount_dependency_tier"], axis=1)).sum()),
        "governance_priority_tier": int((scoring.apply(lambda r: risk_tier(float(r["governance_priority_score"])) != r["governance_priority_tier"], axis=1)).sum()),
        "revenue_quality_risk_tier": int((scoring.apply(lambda r: quality_to_risk_tier(float(r["revenue_quality_score"])) != r["revenue_quality_risk_tier"], axis=1)).sum()),
        "expansion_quality_risk_tier": int((scoring.apply(lambda r: quality_to_risk_tier(float(r["expansion_quality_score"])) != r["expansion_quality_risk_tier"], axis=1)).sum()),
    }
    if sum(tier_mismatch.values()) == 0:
        add_finding(findings, "11", "Risk Tier Assignment Consistency", "Scoring", "PASS", "None", "All tier labels match threshold rules.")
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
    row_calc = scen["start_mrr"] + scen["expansion_mrr"] - scen["contraction_mrr"] - scen["churn_mrr"] + scen["net_new_mrr"]
    row_mismatch = int((row_calc.sub(scen["forecast_mrr"]).abs() > 0.05).sum())
    arr_mismatch = int((scen["forecast_arr"].sub(scen["forecast_mrr"] * 12).abs() > 0.1).sum())
    realized_arr_mismatch = int((scen["realized_arr_estimate"].sub(scen["forecast_arr"] * scen["realized_price_index_assumption"]).abs() > 0.1).sum())

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

    dashboard = validate_dashboard_payload(base_dir, t)
    dashboard_ok = False
    dashboard_detail = "dashboard_not_found"
    dashboard_payload: Dict[str, Any] = {}
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
            "filters",
            "accounts",
            "manager_panel",
            "scenario_cards",
            "risk_impact",
            "chart_catalog",
            "methodology",
            "source_map",
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
    analysis_payload: Dict[str, Any] = {}
    analysis_detail = "analysis_artifacts_missing"
    if analysis_ok:
        analysis_payload = json.loads(analysis_metrics_path.read_text(encoding="utf-8"))
        meta = analysis_payload.get("meta", {})
        section2 = analysis_payload.get("section2", {})
        analysis_ok = bool(meta.get("generated_at_utc")) and ("latest_grr" in section2) and ("latest_nrr" in section2)
        analysis_detail = (
            f"generated_at_utc={meta.get('generated_at_utc', '')[:19]}, "
            f"latest_grr_present={'latest_grr' in section2}, latest_nrr_present={'latest_nrr' in section2}"
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
    # Proxy: check whether scoring correlates materially with same-month churn (can indicate concurrent leakage if misused as prediction).
    same_month_churn = mm[mm["month"] == latest_raw_month][["customer_id", "churn_flag"]]
    leak_probe = scoring[["customer_id", "churn_risk_score"]].merge(same_month_churn, on="customer_id", how="left").fillna({"churn_flag": 0})
    corr_same_month = float(leak_probe["churn_risk_score"].corr(leak_probe["churn_flag"]))

    if latest_quality_month <= latest_raw_month and abs(corr_same_month) < 0.2:
        add_finding(
            findings,
            "14",
            "Leakage Risk",
            "Features/Scoring",
            "PASS",
            "None",
            (
                f"No future-date leakage detected (latest_processed_month={latest_quality_month.date()}, latest_raw_month={latest_raw_month.date()}); "
                f"same-month churn correlation probe={corr_same_month:.3f}."
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
                f"same_month_corr={corr_same_month:.3f}"
            ),
            "Apply strict temporal feature cutoffs and rerun feature/scoring pipeline.",
        )

    # 15) Overclaiming risk in written narrative
    report_files = [
        base_dir / "reports" / "main_business_analysis_memo.md",
        base_dir / "reports" / "forecasting_scenario_analysis.md",
    ]
    text = "\n".join(p.read_text(encoding="utf-8") for p in report_files if p.exists())
    lower = text.lower()

    # Strong causal language scan, excluding explicit negations.
    causal_hits = []
    patterns = [
        r"\bcauses\b",
        r"\bcaused by\b",
        r"\bdrives\b",
        r"\bproves\b",
        r"\bguarantees\b",
        r"\bensures\b",
    ]
    for pat in patterns:
        for m in re.finditer(pat, lower):
            snippet = lower[max(0, m.start() - 30) : m.end() + 30]
            if "does not" in snippet or "not " in snippet:
                continue
            causal_hits.append(re.sub(r"\\b", "", pat))

    has_correlation_caveat = ("correlation does not" in lower) or ("does not establish" in lower)
    has_caveat_sections = lower.count("caveat") >= 2

    if len(causal_hits) == 0 and has_correlation_caveat and has_caveat_sections:
        add_finding(
            findings,
            "15",
            "Overclaiming Risk in Written Narrative",
            "Narrative",
            "PASS",
            "None",
            "Narrative language is mostly associative and includes explicit caveats against causal overclaiming.",
        )
    else:
        add_finding(
            findings,
            "15",
            "Overclaiming Risk in Written Narrative",
            "Narrative",
            "WARN",
            "Medium",
            f"causal_hits={causal_hits}; correlation_caveat_present={has_correlation_caveat}; caveat_section_count={lower.count('caveat')}",
            "Reword strong causal verbs to associative language and ensure caveat lines are retained near key claims.",
        )

    # 16) Metric governance / cross-output consistency
    metric_consistency_ok = False
    metric_detail = "analysis_or_dashboard_payload_missing"
    if analysis_payload and dashboard_payload:
        section1 = analysis_payload.get("section1", {})
        section5 = analysis_payload.get("section5", {})
        db_kpis = dashboard_payload.get("official_kpis", {})

        latest_month = amrq["month"].max()
        latest_mrr_calc = float(amrq.loc[amrq["month"] == latest_month, "active_mrr"].sum())
        mrr_end_reported = float(section1.get("mrr_end", 0.0))
        arr_reported = float(section1.get("arr_end", 0.0))
        at_risk_reported = float(section5.get("at_risk_mrr_total", 0.0))

        delta_mrr_processed_vs_report = abs(latest_mrr_calc - mrr_end_reported)
        delta_mrr_report_vs_dashboard = abs(float(db_kpis.get("current_mrr", 0.0)) - mrr_end_reported)
        delta_arr_report_vs_dashboard = abs(float(db_kpis.get("arr", 0.0)) - arr_reported)
        delta_risk_report_vs_dashboard = abs(float(db_kpis.get("revenue_at_risk_mrr", 0.0)) - at_risk_reported)

        metric_consistency_ok = (
            delta_mrr_processed_vs_report <= 2.0
            and delta_mrr_report_vs_dashboard <= 2.0
            and delta_arr_report_vs_dashboard <= 24.0
            and delta_risk_report_vs_dashboard <= 2.0
        )
        metric_detail = (
            f"delta_mrr_processed_vs_report={delta_mrr_processed_vs_report:.2f}, "
            f"delta_mrr_report_vs_dashboard={delta_mrr_report_vs_dashboard:.2f}, "
            f"delta_arr_report_vs_dashboard={delta_arr_report_vs_dashboard:.2f}, "
            f"delta_risk_report_vs_dashboard={delta_risk_report_vs_dashboard:.2f}"
        )

    if metric_consistency_ok:
        add_finding(
            findings,
            "16",
            "Cross-Output Metric Consistency",
            "Metrics",
            "PASS",
            "None",
            "Governed metrics reconcile across processed tables, analysis report, and dashboard KPI payload.",
        )
    else:
        add_finding(
            findings,
            "16",
            "Cross-Output Metric Consistency",
            "Metrics",
            "FAIL",
            "High",
            metric_detail,
            "Align metric derivations and dashboard feed mapping with the official analysis metric layer.",
        )

    # 17) Score stability and calibration safeguards
    churn_tier_counts = scoring["churn_risk_tier"].value_counts()
    gov_tier_counts = scoring["governance_priority_tier"].value_counts()
    nonzero_churn_tiers = int((churn_tier_counts > 0).sum())
    nonzero_gov_tiers = int((gov_tier_counts > 0).sum())
    churn_iqr = float(scoring["churn_risk_score"].quantile(0.75) - scoring["churn_risk_score"].quantile(0.25))
    low_tier_share = float((scoring["churn_risk_tier"] == "Low").mean())

    stability_fail = (
        nonzero_churn_tiers < 2
        or nonzero_gov_tiers < 2
        or churn_iqr < 1.0
    )
    stability_warn = low_tier_share > 0.97

    stability_detail = (
        f"nonzero_churn_tiers={nonzero_churn_tiers}, nonzero_governance_tiers={nonzero_gov_tiers}, "
        f"churn_iqr={churn_iqr:.2f}, low_tier_share={low_tier_share:.3f}"
    )

    if not stability_fail and not stability_warn:
        add_finding(
            findings,
            "17",
            "Score Stability & Calibration Guardrails",
            "Scoring",
            "PASS",
            "None",
            stability_detail,
        )
    elif stability_fail:
        add_finding(
            findings,
            "17",
            "Score Stability & Calibration Guardrails",
            "Scoring",
            "FAIL",
            "High",
            stability_detail,
            "Recalibrate score weights/thresholds and restore temporal calibration quality before release.",
        )
    else:
        add_finding(
            findings,
            "17",
            "Score Stability & Calibration Guardrails",
            "Scoring",
            "WARN",
            "Medium",
            stability_detail,
            "Review score tier thresholds and monitoring cadence to avoid silent drift.",
        )

    # 18) Financial and decision logic integrity
    impact_df = t["commercial_risk_impact_estimates"]
    impact_map = {str(r.metric): float(r.value) for r in impact_df.itertuples(index=False)}
    section5 = analysis_payload.get("section5", {}) if analysis_payload else {}
    scenario_map = {str(r.scenario): float(r.end_mrr) for r in scen_sum.itertuples(index=False)}

    arr_at_risk_expected = float(section5.get("at_risk_mrr_total", 0.0)) * 12.0 if section5 else 0.0
    arr_at_risk_reported = impact_map.get("arr_at_risk", 0.0)
    full_churn_arr = impact_map.get("top20_high_risk_full_churn_arr_impact", 0.0)
    contraction_20_arr = impact_map.get("top20_high_risk_20pct_contraction_arr_impact", 0.0)
    stress_ratio = full_churn_arr / contraction_20_arr if contraction_20_arr > 0 else np.nan

    scenario_order_ok = (
        scenario_map.get("improvement_case", -np.inf) >= scenario_map.get("base_case", np.inf)
        and scenario_map.get("base_case", -np.inf) >= scenario_map.get("downside_case", np.inf)
        and scenario_map.get("discount_discipline_improvement_case", -np.inf) >= scenario_map.get("base_case", np.inf)
        and scenario_map.get("base_case", -np.inf) >= scenario_map.get("risk_adjusted_case", np.inf)
    )

    delta_arr_at_risk = abs(arr_at_risk_expected - arr_at_risk_reported)
    ratio_ok = not np.isnan(stress_ratio) and abs(stress_ratio - 5.0) <= 0.02
    finance_detail = (
        f"delta_arr_at_risk={delta_arr_at_risk:.2f}, stress_ratio={stress_ratio:.4f}, scenario_order_ok={scenario_order_ok}"
    )

    if delta_arr_at_risk <= 24.0 and ratio_ok and scenario_order_ok:
        add_finding(
            findings,
            "18",
            "Financial & Decision Logic Integrity",
            "Forecasting",
            "PASS",
            "None",
            finance_detail,
        )
    else:
        add_finding(
            findings,
            "18",
            "Financial & Decision Logic Integrity",
            "Forecasting",
            "FAIL",
            "High",
            finance_detail,
            "Reconcile scenario/impact logic before using outputs for financial planning decisions.",
        )

    # 19) Release artifact readiness (dashboard only)
    dashboard_path = base_dir / "outputs" / "dashboard" / "executive_dashboard.html"
    dashboard_size_bytes = int(dashboard_path.stat().st_size) if dashboard_path.exists() else 0
    dashboard_size_ok = dashboard_size_bytes <= 15_000_000
    dashboard_ready = dashboard_path.exists() and dashboard_size_ok
    release_detail = (
        f"dashboard_exists={dashboard_path.exists()}, "
        f"dashboard_size_bytes={dashboard_size_bytes}, dashboard_size_ok={dashboard_size_ok}"
    )

    if dashboard_ready:
        add_finding(
            findings,
            "19",
            "Release Artifact Readiness",
            "Dashboard",
            "PASS",
            "None",
            release_detail,
        )
    else:
        add_finding(
            findings,
            "19",
            "Release Artifact Readiness",
            "Dashboard",
            "FAIL",
            "High",
            release_detail,
            "Regenerate the executive dashboard or reduce payload size before distribution.",
        )

    summary["total_findings"] = len(findings)
    summary["status_counts"] = {
        "PASS": sum(1 for f in findings if f.status == "PASS"),
        "WARN": sum(1 for f in findings if f.status == "WARN"),
        "FAIL": sum(1 for f in findings if f.status == "FAIL"),
    }
    summary["severity_counts"] = {
        level: sum(1 for f in findings if f.severity == level)
        for level in ["Critical", "High", "Medium", "Low", "None"]
    }
    summary["readiness"] = classify_readiness(summary)

    return findings, summary


def confidence_by_component(findings: List[Finding]) -> pd.DataFrame:
    component_map = {
        "Raw Data Logic": ["Raw/Processed", "Raw/Features"],
        "Processed Tables": ["Processed/Metrics", "Processed/Dashboard"],
        "Feature Engineering": ["Features/Metrics", "Features/Scoring"],
        "Metrics": ["Metrics"],
        "Scoring Outputs": ["Scoring"],
        "Forecast Outputs": ["Forecasting"],
        "Dashboard Feeding Tables": ["Processed/Dashboard"],
        "Written Conclusions": ["Narrative"],
    }

    rows = []
    for component, tags in component_map.items():
        comp_findings = [f for f in findings if f.component in tags]
        worst_status = max([STATUS_ORDER[f.status] for f in comp_findings], default=1)
        worst_sev = max([SEVERITY_ORDER[f.severity] for f in comp_findings], default=1)

        if worst_status == 3 and worst_sev >= 4:
            confidence = "Low"
        elif worst_status == 3 or worst_status == 2:
            confidence = "Medium"
        else:
            confidence = "High"

        rows.append(
            {
                "component": component,
                "confidence": confidence,
                "pass": sum(1 for f in comp_findings if f.status == "PASS"),
                "warn": sum(1 for f in comp_findings if f.status == "WARN"),
                "fail": sum(1 for f in comp_findings if f.status == "FAIL"),
            }
        )

    return pd.DataFrame(rows)


def classify_readiness(summary: Dict[str, Any]) -> Dict[str, str]:
    status_counts = summary.get("status_counts", {})
    severity_counts = summary.get("severity_counts", {})
    fail_count = int(status_counts.get("FAIL", 0))
    warn_count = int(status_counts.get("WARN", 0))
    critical_count = int(severity_counts.get("Critical", 0))
    high_count = int(severity_counts.get("High", 0))
    medium_count = int(severity_counts.get("Medium", 0))
    low_count = int(severity_counts.get("Low", 0))

    if fail_count > 0 and (critical_count > 0 or high_count > 0):
        return {
            "tier": "publish-blocked",
            "rationale": "At least one High/Critical failed control blocks publication.",
        }
    if fail_count > 0:
        return {
            "tier": "not committee-grade",
            "rationale": "Validation has failures; outputs are not suitable for committee distribution.",
        }
    if high_count > 0 or warn_count >= 5:
        return {
            "tier": "screening-grade only",
            "rationale": "No hard failures, but risk signals are too material for decision authority.",
        }
    if warn_count >= 2 or medium_count >= 2:
        return {
            "tier": "decision-support only",
            "rationale": "Analytical caveats exist; use for directional decisions with explicit caveats.",
        }
    if warn_count == 1 or medium_count == 1 or low_count > 0:
        return {
            "tier": "analytically acceptable",
            "rationale": "Minor caveats remain; interpretation is acceptable for leadership use with disclosure.",
        }
    return {
        "tier": "technically valid",
        "rationale": "All governed controls passed with no warnings or failures.",
    }


def overall_assessment(findings: List[Finding], summary: Dict[str, Any]) -> str:
    readiness = classify_readiness(summary)
    tier = readiness["tier"]
    rationale = readiness["rationale"]

    if tier == "publish-blocked":
        return f"Publish-blocked. {rationale}"
    if tier == "not committee-grade":
        return f"Not committee-grade. {rationale}"
    if tier == "screening-grade only":
        return f"Screening-grade only. {rationale}"
    if tier == "decision-support only":
        return f"Decision-support only. {rationale}"
    if tier == "analytically acceptable":
        return f"Analytically acceptable. {rationale}"

    critical_fails = [f for f in findings if f.status == "FAIL" and f.severity in {"Critical", "High"}]
    fails = [f for f in findings if f.status == "FAIL"]
    warns = [f for f in findings if f.status == "WARN"]

    if critical_fails:
        return (
            "Conditional readiness. Core analytical outputs are largely coherent, but high-severity validation issues exist "
            "and should be explicitly caveated before stakeholder circulation."
        )
    if fails:
        return "Moderate readiness. Some failed controls require remediation before leadership distribution."
    if warns:
        return "Near-ready. No hard failures, with caveats that should be documented in stakeholder materials."
    return "Technically valid. Validation controls passed without material caveats."


def write_report(base_dir: Path, findings: List[Finding], summary: Dict[str, Any]) -> None:
    reports_dir = base_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    findings_sorted = sorted(
        findings,
        key=lambda f: (STATUS_ORDER[f.status], SEVERITY_ORDER[f.severity]),
        reverse=True,
    )

    findings_df = pd.DataFrame([asdict(f) for f in findings_sorted])
    findings_csv_path = reports_dir / "formal_validation_findings.csv"
    findings_df.to_csv(findings_csv_path, index=False)

    confidence_df = confidence_by_component(findings)
    confidence_md = "\n".join(
        [
            "| Component | Confidence | PASS | WARN | FAIL |",
            "|---|---:|---:|---:|---:|",
        ]
        + [
            f"| {r.component} | {r.confidence} | {r['pass']} | {r.warn} | {r.fail} |"
            for _, r in confidence_df.iterrows()
        ]
    )

    issues = [f for f in findings_sorted if f.status in {"WARN", "FAIL"}]
    if issues:
        issues_md = "\n".join(
            [
                "| Check | Status | Severity | Component | Issue | Recommended Fix |",
                "|---|---|---|---|---|---|",
            ]
            + [
                f"| {f.check_id}. {f.check_name} | {f.status} | {f.severity} | {f.component} | {f.details} | {f.recommended_fix} |"
                for f in issues
            ]
        )
    else:
        issues_md = "No issues found."

    fixes_applied = [f for f in findings_sorted if f.fix_applied == "Yes"]
    fixes_md = "\n".join(
        [f"- {f.check_id}. {f.check_name}: {f.details}" for f in fixes_applied]
    )
    if not fixes_md:
        fixes_md = "- No automatic data/output rewrites were applied during validation."

    unresolved = [f for f in issues if f.status in {"WARN", "FAIL"}]
    unresolved_md = "\n".join(
        [f"- [{f.severity}] {f.check_id}. {f.check_name}: {f.details}" for f in unresolved]
    )
    if not unresolved_md:
        unresolved_md = "- None."

    overall = overall_assessment(findings, summary)
    readiness = summary.get("readiness", classify_readiness(summary))
    readiness_tier = readiness.get("tier", "publish-blocked")
    readiness_rationale = readiness.get("rationale", "")

    report_text = f"""# Formal Validation QA Memo

## Overall Assessment
{overall}

## Governance Readiness Classification
- Current tier: `{readiness_tier}`
- Rationale: {readiness_rationale}
- Ordered scale: `publish-blocked` -> `not committee-grade` -> `screening-grade only` -> `decision-support only` -> `analytically acceptable` -> `technically valid`

Validation execution summary:
- Total checks run: {summary['total_findings']}
- PASS: {summary['status_counts']['PASS']}
- WARN: {summary['status_counts']['WARN']}
- FAIL: {summary['status_counts']['FAIL']}
- High/Critical findings: {summary['severity_counts']['High'] + summary['severity_counts']['Critical']}

## Issues Found (Ranked by Severity)
{issues_md}

## Fixes Applied During Validation
{fixes_md}

## Unresolved Caveats
{unresolved_md}

## Confidence Level by Project Component
{confidence_md}

## QA Positioning for Stakeholder Share-Out
- This memo is a pre-publication QA gate.
- Any unresolved High/Critical findings should be disclosed in stakeholder readouts.
- Narrative claims should remain associative (not causal) unless supported by causal design.
"""

    report_path = reports_dir / "formal_validation_report.md"
    report_path.write_text(report_text, encoding="utf-8")

    summary_payload = {
        "overall_assessment": overall,
        "summary": summary,
        "readiness": readiness,
        "readiness_scale": list(READINESS_ORDER.keys()),
        "confidence_by_component": confidence_df.to_dict(orient="records"),
        "report_path": str(report_path),
        "findings_csv_path": str(findings_csv_path),
    }
    (reports_dir / "formal_validation_summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full-project formal validation and produce QA memo.")
    parser.add_argument("--base-dir", type=str, default=".")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()

    findings, summary = run_validation(base_dir)
    write_report(base_dir, findings, summary)

    print("Formal validation complete")
    print(f"checks_run: {summary['total_findings']}")
    print(f"pass: {summary['status_counts']['PASS']}")
    print(f"warn: {summary['status_counts']['WARN']}")
    print(f"fail: {summary['status_counts']['FAIL']}")


if __name__ == "__main__":
    main()

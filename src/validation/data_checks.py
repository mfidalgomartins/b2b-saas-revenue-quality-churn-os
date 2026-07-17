"""Raw and processed data-quality controls (checks 1–7)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.validation.models import Finding, add_finding


def run_data_quality_checks(tables: dict[str, pd.DataFrame], findings: list[Finding]) -> None:
    t = tables
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
        add_finding(
            findings, "2", "Null Checks", "Raw/Processed", "PASS", "None", "All key fields have 0.00% null rate."
        )
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
        add_finding(
            findings,
            "3",
            "Duplicate Checks",
            "Raw/Processed",
            "PASS",
            "None",
            "No duplicate primary-key rows detected.",
        )
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
    effective_adjust_col = (
        "effective_revenue_adjustment_amount"
        if "effective_revenue_adjustment_amount" in inv.columns
        else "discount_amount"
    )
    impossible = {
        "subscriptions.discount_pct_out_of_range": int(((subs["discount_pct"] < 0) | (subs["discount_pct"] > 1)).sum()),
        "subscriptions.active_nonpositive_contracted_mrr": int(
            ((subs["status"] == "active") & (subs["contracted_mrr"] <= 0)).sum()
        ),
        "subscriptions.realized_gt_120pct_contracted": int(
            (subs["realized_mrr"] > (subs["contracted_mrr"] * 1.2)).sum()
        ),
        "monthly.flags_not_binary": int(
            (
                (~mm["active_flag"].isin([0, 1]))
                | (~mm["churn_flag"].isin([0, 1]))
                | (~mm["renewal_due_flag"].isin([0, 1]))
            ).sum()
        ),
        "monthly.usage_outside_0_100": int(((mm["product_usage_score"] < 0) | (mm["product_usage_score"] > 100)).sum()),
        "monthly.nps_outside_-100_100": int(((mm["nps_score"] < -100) | (mm["nps_score"] > 100)).sum()),
        "invoices.negative_discount_amount": int((inv["discount_amount"] < 0).sum()),
        "invoices.negative_collection_loss_amount": int(
            (inv.get("collection_loss_amount", pd.Series([0] * len(inv))) < 0).sum()
        ),
        "invoices.effective_adjustment_gt_billed": int((inv[effective_adjust_col] > inv["billed_mrr"] + 1e-6).sum()),
        "invoices.realized_gt_105pct_billed": int((inv["realized_mrr"] > (inv["billed_mrr"] * 1.05)).sum()),
        "amrq.realized_price_index_gt_1p2": int((amrq["realized_price_index"] > 1.2).sum()),
    }
    if sum(impossible.values()) == 0:
        add_finding(
            findings,
            "4",
            "Impossible Values",
            "Raw/Processed",
            "PASS",
            "None",
            "No impossible-value violations in checked fields.",
        )
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
    first_sub = (
        subs.groupby("customer_id", as_index=False)["subscription_start_date"]
        .min()
        .rename(columns={"subscription_start_date": "first_subscription_start"})
    )
    signup_cmp = customers.merge(first_sub, on="customer_id", how="left")
    signup_after_first_sub = int((signup_cmp["signup_date"] > signup_cmp["first_subscription_start"]).sum())
    sub_end_before_start = int((subs["subscription_end_date"] < subs["subscription_start_date"]).sum())

    max_raw_month = mm["month"].max()
    max_processed_month = amrq["month"].max()
    future_processed_rows = int((amrq["month"] > max_raw_month).sum())

    if (
        signup_after_first_sub == 0
        and sub_end_before_start == 0
        and future_processed_rows == 0
        and max_raw_month == max_processed_month
    ):
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
        amrq[amrq["month"] == latest_month]
        .groupby("customer_id", as_index=False)["active_mrr"]
        .sum()
        .rename(columns={"active_mrr": "mrr_latest"})
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

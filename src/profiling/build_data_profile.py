from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


RAW_FILE_CONFIG = {
    "customers": {"file": "customers.csv", "parse_dates": ["signup_date"], "grain": "one row per customer", "pk": ["customer_id"]},
    "plans": {"file": "plans.csv", "parse_dates": [], "grain": "one row per plan", "pk": ["plan_id"]},
    "subscriptions": {
        "file": "subscriptions.csv",
        "parse_dates": ["subscription_start_date", "subscription_end_date"],
        "grain": "one row per customer-month subscription snapshot",
        "pk": ["subscription_id"],
    },
    "monthly_account_metrics": {
        "file": "monthly_account_metrics.csv",
        "parse_dates": ["month"],
        "grain": "one row per customer-month health panel",
        "pk": ["customer_id", "month"],
    },
    "invoices": {"file": "invoices.csv", "parse_dates": ["invoice_month"], "grain": "one row per customer-month invoice", "pk": ["invoice_id"]},
    "account_managers": {"file": "account_managers.csv", "parse_dates": [], "grain": "one row per account manager", "pk": ["account_manager_id"]},
}


@dataclass
class QualityIssue:
    severity: str
    check: str
    details: str
    impact: str
    recommended_fix: str


def load_raw_tables(raw_dir: Path) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for name, cfg in RAW_FILE_CONFIG.items():
        tables[name] = pd.read_csv(raw_dir / cfg["file"], parse_dates=cfg["parse_dates"])
    return tables


def _maybe_date_coverage(df: pd.DataFrame) -> dict[str, dict[str, str]]:
    coverage: dict[str, dict[str, str]] = {}
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            series = df[col].dropna()
            if len(series) > 0:
                coverage[col] = {"min": str(series.min().date()), "max": str(series.max().date())}
    return coverage


def _column_profile(df: pd.DataFrame) -> dict[str, Any]:
    prof: dict[str, Any] = {}
    for col in df.columns:
        s = df[col]
        entry: dict[str, Any] = {
            "dtype": str(s.dtype),
            "null_rate": round(float(s.isna().mean()), 6),
            "nunique": int(s.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(s):
            numeric = pd.to_numeric(s, errors="coerce")
            if numeric.notna().any():
                q = numeric.quantile([0.01, 0.50, 0.99])
                entry.update(
                    {
                        "min": round(float(numeric.min()), 4),
                        "p01": round(float(q.loc[0.01]), 4),
                        "median": round(float(q.loc[0.50]), 4),
                        "p99": round(float(q.loc[0.99]), 4),
                        "max": round(float(numeric.max()), 4),
                    }
                )
        else:
            top = s.astype(str).fillna("__NULL__").value_counts().head(3).to_dict()
            entry["top_values"] = {str(k): int(v) for k, v in top.items()}
        prof[col] = entry
    return prof


def _duplicate_count(df: pd.DataFrame, key_cols: list[str]) -> int:
    return int(df.duplicated(key_cols).sum())


def profile_tables(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for name, cfg in RAW_FILE_CONFIG.items():
        df = tables[name]
        summary[name] = {
            "grain": cfg["grain"],
            "primary_key_candidate": cfg["pk"],
            "rows": int(len(df)),
            "cols": int(len(df.columns)),
            "date_coverage": _maybe_date_coverage(df),
            "null_cells_pct": round(float(df.isna().mean().mean()), 6),
            "duplicate_count_on_pk_candidate": _duplicate_count(df, cfg["pk"]),
            "columns": _column_profile(df),
        }
    return summary


def run_quality_checks(tables: dict[str, pd.DataFrame]) -> tuple[list[QualityIssue], dict[str, Any]]:
    customers = tables["customers"]
    plans = tables["plans"]
    subs = tables["subscriptions"]
    monthly = tables["monthly_account_metrics"]
    invoices = tables["invoices"]
    managers = tables["account_managers"]

    issues: list[QualityIssue] = []
    checks: dict[str, Any] = {}

    # Referential integrity
    checks["ri_subscriptions_customer"] = int((~subs["customer_id"].isin(customers["customer_id"])).sum())
    checks["ri_monthly_customer"] = int((~monthly["customer_id"].isin(customers["customer_id"])).sum())
    checks["ri_invoices_customer"] = int((~invoices["customer_id"].isin(customers["customer_id"])).sum())
    checks["ri_subscriptions_plan"] = int((~subs["plan_id"].isin(plans["plan_id"])).sum())
    checks["ri_customer_manager"] = int((~customers["account_manager_id"].isin(managers["account_manager_id"])).sum())

    # Subscription/date coherence
    checks["subscription_end_before_start"] = int((subs["subscription_end_date"] < subs["subscription_start_date"]).sum())
    checks["active_nonpositive_contracted_mrr"] = int(((subs["status"] == "active") & (subs["contracted_mrr"] <= 0)).sum())
    checks["unknown_subscription_status"] = int((~subs["status"].isin(["active", "churned"])).sum())
    checks["signup_after_first_subscription"] = int(
        (
            customers.merge(
                subs.groupby("customer_id", as_index=False)["subscription_start_date"].min(),
                on="customer_id",
                how="left",
            )["signup_date"]
            > customers.merge(
                subs.groupby("customer_id", as_index=False)["subscription_start_date"].min(),
                on="customer_id",
                how="left",
            )["subscription_start_date"]
        ).sum()
    )

    # Realization plausibility
    checks["realized_gt_120pct_contracted"] = int((subs["realized_mrr"] > 1.2 * subs["contracted_mrr"]).sum())
    checks["discount_pct_out_of_range"] = int(((subs["discount_pct"] < 0) | (subs["discount_pct"] > 1)).sum())

    # Discount coherence: discount + collection loss == billed - realized
    commercial_col = "discount_amount"
    collection_col = "collection_loss_amount" if "collection_loss_amount" in invoices.columns else None
    effective_col = (
        "effective_revenue_adjustment_amount"
        if "effective_revenue_adjustment_amount" in invoices.columns
        else "discount_amount"
    )
    billed = invoices["billed_mrr"].replace(0, np.nan)
    effective_ratio = (invoices[effective_col] / billed).fillna(0.0)
    implied_effective = (invoices["billed_mrr"] - invoices["realized_mrr"]).clip(lower=0)
    checks["invoice_effective_adjustment_mismatch"] = int((invoices[effective_col] - implied_effective).abs().gt(0.02).sum())
    checks["invoice_commercial_discount_out_of_range"] = int(
        ((invoices[commercial_col] < 0) | (invoices[commercial_col] > invoices["billed_mrr"])).sum()
    )
    checks["invoice_effective_adjustment_gt_billed"] = int((invoices[effective_col] > invoices["billed_mrr"] + 1e-6).sum())
    checks["invoice_effective_discount_ratio_p99"] = round(float(effective_ratio.quantile(0.99)), 4)
    checks["invoice_effective_discount_ratio_mean"] = round(float(effective_ratio.mean()), 4)
    if collection_col is not None:
        checks["invoice_collection_loss_rate"] = round(
            float((invoices[collection_col] > 0).mean()),
            4,
        )

    # Churn alignment
    sub_month = subs.rename(columns={"subscription_start_date": "month"})[["customer_id", "month", "status"]]
    churn_cmp = monthly.merge(sub_month, on=["customer_id", "month"], how="left")
    checks["churn_flag_without_churned_status"] = int(((churn_cmp["churn_flag"] == 1) & (churn_cmp["status"] != "churned")).sum())

    # Leakage checks
    checks["max_raw_month"] = str(monthly["month"].max().date())
    checks["max_subscription_month"] = str(subs["subscription_start_date"].max().date())
    checks["max_invoice_month"] = str(invoices["invoice_month"].max().date())
    checks["future_month_misalignment"] = int(
        max(subs["subscription_start_date"].max(), invoices["invoice_month"].max()) > monthly["month"].max()
    )

    # Issue ranking
    if any(checks[k] > 0 for k in checks if k.startswith("ri_")):
        issues.append(
            QualityIssue(
                severity="Critical",
                check="Referential integrity",
                details=f"RI violations: {[k for k,v in checks.items() if k.startswith('ri_') and v > 0]}",
                impact="Join inflation, missing-link analysis errors, and unreliable segmented views.",
                recommended_fix="Regenerate entities with strict foreign-key constraints in generation logic.",
            )
        )

    if checks["subscription_end_before_start"] > 0 or checks["signup_after_first_subscription"] > 0:
        issues.append(
            QualityIssue(
                severity="High",
                check="Date coherence",
                details=(
                    f"subscription_end_before_start={checks['subscription_end_before_start']}, "
                    f"signup_after_first_subscription={checks['signup_after_first_subscription']}"
                ),
                impact="Breaks lifecycle sequencing and invalidates retention/cohort logic.",
                recommended_fix="Enforce signup <= first subscription start and end >= start constraints.",
            )
        )

    if checks["invoice_effective_adjustment_mismatch"] > 0 or checks["invoice_effective_adjustment_gt_billed"] > 0:
        issues.append(
            QualityIssue(
                severity="High",
                check="Invoice arithmetic coherence",
                details=(
                    f"invoice_effective_adjustment_mismatch={checks['invoice_effective_adjustment_mismatch']}, "
                    f"invoice_effective_adjustment_gt_billed={checks['invoice_effective_adjustment_gt_billed']}"
                ),
                impact="Realized pricing and discount metrics become untrustworthy.",
                recommended_fix="Align billed/realized/effective adjustment formulas at generation time.",
            )
        )

    if checks["churn_flag_without_churned_status"] > 0:
        issues.append(
            QualityIssue(
                severity="Medium",
                check="Churn flag vs status alignment",
                details=f"churn_flag_without_churned_status={checks['churn_flag_without_churned_status']}",
                impact="Weakens churn diagnostics and intervention targeting.",
                recommended_fix="Synchronize monthly churn event flags with subscription status snapshots.",
            )
        )

    if checks["realized_gt_120pct_contracted"] > 0:
        issues.append(
            QualityIssue(
                severity="Medium",
                check="Realized MRR plausibility",
                details=f"realized_gt_120pct_contracted={checks['realized_gt_120pct_contracted']}",
                impact="Distorts pricing-quality metrics.",
                recommended_fix="Cap realized MRR relative to contracted MRR at generation and transform levels.",
            )
        )

    if checks["future_month_misalignment"] > 0:
        issues.append(
            QualityIssue(
                severity="High",
                check="Look-ahead leakage risk",
                details="Subscription or invoice dates exceed monthly metrics max month.",
                impact="Potential temporal leakage in scoring and scenario layers.",
                recommended_fix="Constrain all table coverage windows to shared max month before feature generation.",
            )
        )

    return issues, checks


def build_memo(
    profile_summary: dict[str, Any],
    checks: dict[str, Any],
    issues: list[QualityIssue],
    output_path: Path,
) -> None:
    def issue_table(items: list[QualityIssue]) -> str:
        if not items:
            return "No material data quality issues detected in current run."
        lines = [
            "| Severity | Check | Details | Impact | Recommended Fix |",
            "|---|---|---|---|---|",
        ]
        severity_order = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}
        for i in sorted(items, key=lambda x: severity_order.get(x.severity, 0), reverse=True):
            lines.append(
                f"| {i.severity} | {i.check} | {i.details} | {i.impact} | {i.recommended_fix} |"
            )
        return "\n".join(lines)

    rows = [f"- `{name}`: {stats['rows']:,} rows, {stats['cols']} columns, PK candidate `{stats['primary_key_candidate']}`" for name, stats in profile_summary.items()]
    coverage_start = min(
        [
            stats["date_coverage"][col]["min"]
            for stats in profile_summary.values()
            for col in stats["date_coverage"]
        ]
    )
    coverage_end = max(
        [
            stats["date_coverage"][col]["max"]
            for stats in profile_summary.values()
            for col in stats["date_coverage"]
        ]
    )
    issue_count = len(issues)
    high_count = sum(1 for i in issues if i.severity in {"High", "Critical"})

    memo = f"""# Data Profiling Memo

## Executive Summary
- Coverage window: `{coverage_start}` to `{coverage_end}`
- Tables profiled: `{len(profile_summary)}`
- Material issues identified: `{issue_count}` (High/Critical: `{high_count}`)
- Assessment: {"Ready for analysis" if high_count == 0 else "Conditional - remediation recommended before stakeholder use"}

## Table Inventory
{chr(10).join(rows)}

## Key Integrity Checks
- Referential integrity violations: `{sum(v for k,v in checks.items() if k.startswith('ri_'))}`
- Subscription date coherence issues: `{checks['subscription_end_before_start']}`
- Signup chronology issues: `{checks['signup_after_first_subscription']}`
- Invoice effective-adjustment mismatches (>2 cents): `{checks['invoice_effective_adjustment_mismatch']}`
- Churn flag/status misalignment rows: `{checks['churn_flag_without_churned_status']}`
- Future-month misalignment flags: `{checks['future_month_misalignment']}`

## Issues Ranked by Severity
{issue_table(issues)}

## Analytical Implications
- Revenue quality and retention conclusions are reliable only when invoice arithmetic and churn/status coherence are preserved.
- Segment/channel diagnostics depend on zero RI breaks; otherwise, denominator inflation risk emerges.
- Temporal consistency is a hard precondition for churn early-warning credibility.

## Recommended Focus Areas for Main Analysis
1. Discount discipline versus realized pricing quality (separating commercial discount from collection loss).
2. Renewal-window churn concentration by segment/channel.
3. Fragile expansion identification (growth with deteriorating health signals).
4. Concentration-adjusted downside exposure in High/Critical governance tiers.
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(memo, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run formal raw-data profiling and emit profiling artifacts.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--stats-path", type=str, default="reports/profiling_stats.json")
    parser.add_argument("--memo-path", type=str, default="reports/data_profiling_memo.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    raw_dir = base_dir / "data" / "raw"

    tables = load_raw_tables(raw_dir)
    profile_summary = profile_tables(tables)
    issues, checks = run_quality_checks(tables)

    payload = {
        "meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "base_dir": str(base_dir),
            "table_count": len(profile_summary),
        },
        "summary": profile_summary,
        "quality_checks": checks,
        "issues_ranked": [issue.__dict__ for issue in issues],
    }

    stats_path = (base_dir / args.stats_path).resolve()
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    build_memo(profile_summary=profile_summary, checks=checks, issues=issues, output_path=(base_dir / args.memo_path).resolve())

    print("Data profiling complete.")
    print(f"Stats: {stats_path}")
    print(f"Memo: {(base_dir / args.memo_path).resolve()}")


if __name__ == "__main__":
    main()

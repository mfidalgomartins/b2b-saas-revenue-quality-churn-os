"""Run the governed project-validation suite and publish its evidence."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from src.io.logging_setup import get_logger
from src.validation.analytical_checks import run_analytical_checks
from src.validation.data_checks import run_data_quality_checks
from src.validation.expansion_checks import run_strategic_expansion_checks
from src.validation.governance_checks import run_release_governance_checks
from src.validation.models import Finding

log = get_logger(__name__)

SEVERITY_ORDER = {"Critical": 5, "High": 4, "Medium": 3, "Low": 2, "None": 1}
STATUS_ORDER = {"FAIL": 3, "WARN": 2, "PASS": 1}  # nosec
READINESS_ORDER = {
    "publish-blocked": 1,
    "not committee-grade": 2,
    "screening-grade only": 3,
    "decision-support only": 4,
    "analytically acceptable": 5,
    "technically valid": 6,
}


def load_tables(base_dir: Path) -> dict[str, pd.DataFrame]:
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
        "account_monthly_revenue_quality": pd.read_csv(
            processed / "account_monthly_revenue_quality.csv", parse_dates=["month"]
        ),
        "customer_health_features": pd.read_csv(processed / "customer_health_features.csv"),
        "cohort_retention_summary": pd.read_csv(
            processed / "cohort_retention_summary.csv", parse_dates=["cohort_month"]
        ),
        "account_risk_base": pd.read_csv(processed / "account_risk_base.csv", parse_dates=["current_month"]),
        "account_manager_summary": pd.read_csv(processed / "account_manager_summary.csv"),
        "account_scoring_model_output": pd.read_csv(processed / "account_scoring_model_output.csv"),
        "account_scoring_components": pd.read_csv(processed / "account_scoring_components.csv"),
        "scenario_mrr_trajectories": pd.read_csv(
            processed / "scenario_mrr_trajectories.csv", parse_dates=["forecast_month"]
        ),
        "mrr_scenario_table": pd.read_csv(processed / "mrr_scenario_table.csv"),
        "commercial_risk_impact_estimates": pd.read_csv(processed / "commercial_risk_impact_estimates.csv"),
        "intervention_assignment_log": pd.read_csv(
            processed / "intervention_assignment_log.csv", parse_dates=["assignment_month"]
        ),
        "intervention_outcomes": pd.read_csv(
            processed / "intervention_outcomes.csv", parse_dates=["assignment_month", "outcome_month"]
        ),
        "intervention_effectiveness_by_segment": pd.read_csv(processed / "intervention_effectiveness_by_segment.csv"),
        "intervention_covariate_balance": pd.read_csv(processed / "intervention_covariate_balance.csv"),
        "probabilistic_mrr_forecast": pd.read_csv(
            processed / "probabilistic_mrr_forecast.csv", parse_dates=["forecast_month"]
        ),
        "probabilistic_forecast_backtest": pd.read_csv(
            processed / "probabilistic_forecast_backtest.csv", parse_dates=["origin_month", "target_month"]
        ),
        "probabilistic_forecast_backtest_summary": pd.read_csv(
            processed / "probabilistic_forecast_backtest_summary.csv"
        ),
        "main_metrics_json": pd.DataFrame(
            [json.loads((base_dir / "reports" / "main_business_analysis_metrics.json").read_text(encoding="utf-8"))]
        ),
    }


def run_validation(base_dir: Path) -> tuple[list[Finding], dict[str, Any]]:
    tables = load_tables(base_dir)
    findings: list[Finding] = []
    run_data_quality_checks(tables, findings)
    artifacts = run_analytical_checks(base_dir, tables, findings)
    run_release_governance_checks(base_dir, tables, findings, artifacts)
    run_strategic_expansion_checks(base_dir, tables, findings)

    summary: dict[str, Any] = {
        "total_findings": len(findings),
        "status_counts": {
            "PASS": sum(1 for finding in findings if finding.status == "PASS"),
            "WARN": sum(1 for finding in findings if finding.status == "WARN"),
            "FAIL": sum(1 for finding in findings if finding.status == "FAIL"),
        },
        "severity_counts": {
            level: sum(1 for finding in findings if finding.severity == level)
            for level in ["Critical", "High", "Medium", "Low", "None"]
        },
    }
    summary["readiness"] = classify_readiness(summary)
    return findings, summary


def confidence_by_component(findings: list[Finding]) -> pd.DataFrame:
    component_map = {
        "Raw Data Logic": ["Raw/Processed", "Raw/Features"],
        "Processed Tables": ["Processed/Metrics", "Processed/Dashboard"],
        "Feature Engineering": ["Features/Metrics", "Features/Scoring"],
        "Metrics": ["Metrics"],
        "Scoring Outputs": ["Scoring"],
        "Forecast Outputs": ["Forecasting"],
        "Dashboard Feeding Tables": ["Processed/Dashboard"],
        "Written Conclusions": ["Narrative"],
        "Release Governance": ["Release Governance"],
        "Intervention Measurement": ["Interventions"],
        "Source Provenance": ["Source Provenance"],
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


def classify_readiness(summary: dict[str, Any]) -> dict[str, str]:
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


def overall_assessment(findings: list[Finding], summary: dict[str, Any]) -> str:
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


def write_validation_outputs(base_dir: Path, findings: list[Finding], summary: dict[str, Any]) -> None:
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

    overall = overall_assessment(findings, summary)
    readiness = summary.get("readiness", classify_readiness(summary))

    summary_payload = {
        "overall_assessment": overall,
        "summary": summary,
        "readiness": readiness,
        "readiness_scale": list(READINESS_ORDER.keys()),
        "confidence_by_component": confidence_by_component(findings).to_dict(orient="records"),
        "findings_csv_path": str(findings_csv_path.relative_to(base_dir)),
    }
    (reports_dir / "formal_validation_summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run full-project validation and write governed machine-readable outputs."
    )
    parser.add_argument("--base-dir", type=str, default=".")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()

    findings, summary = run_validation(base_dir)
    write_validation_outputs(base_dir, findings, summary)

    log.info("Project validation complete")
    log.info("checks_run: %s", summary["total_findings"])
    log.info("pass: %s", summary["status_counts"]["PASS"])
    log.info("warn: %s", summary["status_counts"]["WARN"])
    log.info("fail: %s", summary["status_counts"]["FAIL"])


if __name__ == "__main__":
    main()

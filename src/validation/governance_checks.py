"""Narrative, cross-output and release-governance controls (checks 15–21)."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from src.scoring.scoring_utils import CHURN_WEIGHTS
from src.validation.context import ValidationArtifacts
from src.validation.models import Finding, add_finding


def run_release_governance_checks(
    base_dir: Path,
    tables: dict[str, pd.DataFrame],
    findings: list[Finding],
    artifacts: ValidationArtifacts,
) -> None:
    t = tables
    amrq = t["account_monthly_revenue_quality"]
    scoring = t["account_scoring_model_output"]
    scen_sum = t["mrr_scenario_table"]
    dashboard = artifacts.dashboard
    dashboard_payload = artifacts.dashboard_payload
    analysis_payload = artifacts.analysis_payload
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
        section2 = analysis_payload.get("section2", {})
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
        delta_logo_churn_report_vs_dashboard = abs(
            float(db_kpis.get("logo_churn", 0.0)) - float(section2.get("latest_logo_churn_rate", 0.0))
        )

        metric_consistency_ok = (
            delta_mrr_processed_vs_report <= 2.0
            and delta_mrr_report_vs_dashboard <= 2.0
            and delta_arr_report_vs_dashboard <= 24.0
            and delta_risk_report_vs_dashboard <= 2.0
            and delta_logo_churn_report_vs_dashboard <= 1e-6
        )
        metric_detail = (
            f"delta_mrr_processed_vs_report={delta_mrr_processed_vs_report:.2f}, "
            f"delta_mrr_report_vs_dashboard={delta_mrr_report_vs_dashboard:.2f}, "
            f"delta_arr_report_vs_dashboard={delta_arr_report_vs_dashboard:.2f}, "
            f"delta_risk_report_vs_dashboard={delta_risk_report_vs_dashboard:.2f}, "
            f"delta_logo_churn_report_vs_dashboard={delta_logo_churn_report_vs_dashboard:.6f}"
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

    stability_fail = nonzero_churn_tiers < 2 or nonzero_gov_tiers < 2 or churn_iqr < 1.0
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
    finance_detail = f"delta_arr_at_risk={delta_arr_at_risk:.2f}, stress_ratio={stress_ratio:.4f}, scenario_order_ok={scenario_order_ok}"

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

    # 19) Release artifact readiness (canonical dashboard only)
    dashboard_path = base_dir / "outputs" / "dashboard" / "revenue-quality-command-center.html"
    dashboard_size_bytes = int(dashboard_path.stat().st_size) if dashboard_path.exists() else 0
    dashboard_size_ok = dashboard_size_bytes <= 15_000_000
    payload_files = dashboard.get("payload_files", []) if dashboard else []
    contract = dashboard_payload.get("dashboard_contract", {}) if dashboard_payload else {}
    canonical_path = contract.get("canonical_dashboard_path", "")
    single_payload_ok = payload_files == ["outputs/dashboard/revenue-quality-command-center.html"]
    contract_ok = canonical_path == "outputs/dashboard/revenue-quality-command-center.html"
    dashboard_ready = dashboard_path.exists() and dashboard_size_ok and single_payload_ok and contract_ok
    release_detail = (
        f"dashboard_exists={dashboard_path.exists()}, "
        f"dashboard_size_bytes={dashboard_size_bytes}, dashboard_size_ok={dashboard_size_ok}, "
        f"payload_files={payload_files}, contract_ok={contract_ok}"
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
            "Regenerate the canonical dashboard, remove duplicate dashboard payloads, or restore dashboard_contract before distribution.",
        )

    # 20) Test suite integrity
    test_cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"]
    test_run = subprocess.run(
        test_cmd,
        cwd=str(base_dir),
        capture_output=True,
        text=True,
    )
    if test_run.returncode == 0:
        add_finding(
            findings,
            "20",
            "Test Suite Integrity",
            "Release Governance",
            "PASS",
            "None",
            "Unit test discovery/execution completed successfully.",
        )
    else:
        err_excerpt = (test_run.stderr or test_run.stdout)[-1000:]
        add_finding(
            findings,
            "20",
            "Test Suite Integrity",
            "Release Governance",
            "FAIL",
            "Critical",
            f"Unit test suite failed (exit={test_run.returncode}). Excerpt: {err_excerpt}",
            "Fix failing/broken tests before claiming release readiness.",
        )

    # 21) Backtest calibration monotonicity
    #
    # The backtest scores every historical (customer, month) with the SAME
    # weights as production and measures forward 3M churn by tier. A credible
    # risk model must produce a monotonic relationship — accounts the model
    # labels High must churn more often than Moderate, which must churn more
    # often than Low. Tiers with fewer than 30 observations are excluded
    # because their rates are too noisy to falsify monotonicity.
    backtest_summary_path = base_dir / "reports" / "scoring_backtest_summary.json"
    backtest_tier_path = base_dir / "data" / "processed" / "scoring_backtest_calibration_by_tier.csv"
    if backtest_summary_path.exists() and backtest_tier_path.exists():
        backtest_summary = json.loads(backtest_summary_path.read_text(encoding="utf-8"))
        tier_df = pd.read_csv(backtest_tier_path)
        eligible = tier_df[tier_df["observations"] >= 30].set_index("risk_tier")
        ordered = [t for t in ["Low", "Moderate", "High", "Critical"] if t in eligible.index]
        rates = [(t, float(eligible.loc[t, "forward_churn_rate"])) for t in ordered]
        violations = [
            f"{a[0]}({a[1]:.1%})>{b[0]}({b[1]:.1%})" for a, b in zip(rates, rates[1:], strict=False) if a[1] > b[1]
        ]
        weights_match = backtest_summary.get("weights") == dict(CHURN_WEIGHTS)
        evidence = (
            f"Eligible tiers (≥30 obs): {', '.join(f'{t}={r:.2%}' for t, r in rates)}; "
            f"weights match production: {weights_match}"
        )
        if not weights_match:
            add_finding(
                findings,
                "21",
                "Backtest Calibration",
                "Release Governance",
                "FAIL",
                "Critical",
                evidence + ". Weights drift detected between backtest and production scorer.",
                "Re-run backtest after confirming scoring_utils.CHURN_WEIGHTS is the single source.",
            )
        elif violations:
            add_finding(
                findings,
                "21",
                "Backtest Calibration",
                "Release Governance",
                "FAIL",
                "High",
                f"Monotonicity violated: {', '.join(violations)}. {evidence}",
                "Inspect component weights/thresholds; the model is not separating tiers reliably.",
            )
        else:
            add_finding(
                findings,
                "21",
                "Backtest Calibration",
                "Release Governance",
                "PASS",
                "None",
                evidence,
            )
    else:
        add_finding(
            findings,
            "21",
            "Backtest Calibration",
            "Release Governance",
            "WARN",
            "Medium",
            "Backtest artifacts not found (reports/scoring_backtest_summary.json).",
            "Run `python -m src.scoring.backtest_scoring_calibration` to generate the calibration report.",
        )

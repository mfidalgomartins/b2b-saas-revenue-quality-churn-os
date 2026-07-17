"""Controls for intervention, probabilistic-forecast and source-provenance evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.io.contracts import REQUIRED_RAW_SCHEMAS
from src.validation.models import Finding, add_finding


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_recommendation(row: pd.Series) -> str:
    if float(row["mrr_uplift_ci_lower"]) > 0 and float(row["commercial_roi"]) > 0:
        return "scale_candidate"
    if float(row["mrr_uplift_ci_upper"]) <= 0 or float(row["commercial_roi"]) <= 0:
        return "do_not_scale"
    return "continue_test"


def _intervention_evidence_is_valid(
    base_dir: Path,
    tables: dict[str, pd.DataFrame],
) -> tuple[bool, str]:
    ledger = tables["intervention_assignment_log"]
    outcomes = tables["intervention_outcomes"]
    effectiveness = tables["intervention_effectiveness_by_segment"]
    balance = tables["intervention_covariate_balance"]
    metadata = json.loads(
        (base_dir / "reports" / "intervention_effectiveness_summary.json").read_text(encoding="utf-8")
    )

    identity_ok = (
        ledger["assignment_id"].is_unique
        and ledger["customer_id"].is_unique
        and outcomes["assignment_id"].is_unique
        and set(ledger["assignment_id"]) == set(outcomes["assignment_id"])
        and set(ledger["assignment_group"]) == {"control", "treatment"}
        and ledger["assignment_probability"].between(0, 1, inclusive="neither").all()
        and np.allclose(ledger["assignment_probability"], 0.5, atol=1e-12, rtol=0)
    )
    chronology_ok = (
        outcomes["outcome_month"].gt(outcomes["assignment_month"]).all()
        and (
            (outcomes["outcome_month"].dt.year - outcomes["assignment_month"].dt.year) * 12
            + outcomes["outcome_month"].dt.month
            - outcomes["assignment_month"].dt.month
        )
        .eq(int(metadata.get("followup_months", -1)))
        .all()
    )

    assignment_month = ledger["assignment_month"].iloc[0]
    canonical_baseline = tables["account_monthly_revenue_quality"].loc[
        lambda frame: frame["month"].eq(assignment_month), ["customer_id", "active_mrr"]
    ]
    baseline_check = ledger[["customer_id", "baseline_mrr"]].merge(
        canonical_baseline,
        on="customer_id",
        how="left",
        validate="one_to_one",
    )
    baseline_ok = baseline_check["active_mrr"].notna().all() and np.allclose(
        baseline_check["baseline_mrr"], baseline_check["active_mrr"], atol=0.01, rtol=0
    )

    max_smd = float(balance["absolute_smd"].max())
    balance_ok = max_smd <= 0.10 and balance["balance_status"].eq("PASS").all()
    overall_rows = effectiveness.loc[effectiveness["scope"].eq("All")]
    if len(overall_rows) != 1:
        return False, f"overall_rows={len(overall_rows)}"
    overall = overall_rows.iloc[0]
    metadata_overall = metadata.get("overall", {})
    metadata_ok = (
        metadata.get("design") == "blocked_randomized_intent_to_treat"
        and int(metadata.get("bootstrap_samples", 0)) >= 1000
        and int(metadata_overall.get("n_total", -1)) == len(ledger)
        and str(metadata_overall.get("recommendation")) == str(overall["recommendation"])
        and np.isclose(
            float(metadata_overall.get("gross_mrr_retention_uplift", np.nan)),
            float(overall["gross_mrr_retention_uplift"]),
            atol=1e-12,
            rtol=0,
        )
    )

    annualization_months = int(metadata.get("annualization_months", 0))
    gross_margin = float(metadata.get("gross_margin", np.nan))
    finance_ok = True
    interval_ok = True
    recommendation_ok = True
    for _, row in effectiveness.iterrows():
        incremental_mrr = float(row["gross_mrr_retention_uplift"] * row["treated_baseline_mrr"])
        annualized_gp = incremental_mrr * annualization_months * gross_margin
        cost = float(row["intervention_cost"])
        expected_roi = (annualized_gp - cost) / cost if cost > 0 else np.nan
        finance_ok &= bool(
            np.isclose(row["incremental_retained_mrr"], incremental_mrr, atol=0.01, rtol=0)
            and np.isclose(row["annualized_incremental_gross_profit"], annualized_gp, atol=0.01, rtol=0)
            and np.isclose(row["commercial_roi"], expected_roi, atol=1e-12, rtol=0)
        )
        interval_ok &= bool(
            row["logo_uplift_ci_lower"] <= row["logo_retention_uplift"] <= row["logo_uplift_ci_upper"]
            and row["mrr_uplift_ci_lower"] <= row["gross_mrr_retention_uplift"] <= row["mrr_uplift_ci_upper"]
            and row["roi_ci_lower"] <= row["commercial_roi"] <= row["roi_ci_upper"]
        )
        recommendation_ok &= str(row["recommendation"]) == _expected_recommendation(row)

    valid = all(
        [
            identity_ok,
            chronology_ok,
            baseline_ok,
            balance_ok,
            metadata_ok,
            finance_ok,
            interval_ok,
            recommendation_ok,
        ]
    )
    detail = (
        f"assignments={len(ledger)}, max_absolute_smd={max_smd:.4f}, "
        f"bootstrap_samples={metadata.get('bootstrap_samples')}, recommendation={overall['recommendation']}; "
        f"identity={identity_ok}, chronology={chronology_ok}, baseline={baseline_ok}, "
        f"finance={finance_ok}, intervals={interval_ok}"
    )
    return valid, detail


def _probabilistic_forecast_is_valid(
    base_dir: Path,
    tables: dict[str, pd.DataFrame],
) -> tuple[bool, str]:
    forecast = tables["probabilistic_mrr_forecast"]
    backtest = tables["probabilistic_forecast_backtest"]
    summary = tables["probabilistic_forecast_backtest_summary"]
    metadata = json.loads((base_dir / "reports" / "probabilistic_forecast_validation.json").read_text(encoding="utf-8"))
    quantiles = ["p05", "p10", "p50", "p90", "p95"]
    forecast_quantiles_ok = forecast[quantiles].diff(axis=1).iloc[:, 1:].ge(0).all().all()
    backtest_quantiles_ok = backtest[quantiles].diff(axis=1).iloc[:, 1:].ge(0).all().all()
    horizon = int(metadata.get("forecast_horizon_months", 0))
    simulations = int(metadata.get("simulations", 0))
    forecast_shape_ok = (
        len(forecast) == horizon
        and forecast["horizon_month"].tolist() == list(range(1, horizon + 1))
        and forecast["simulation_count"].eq(simulations).all()
        and simulations >= 2000
        and forecast["p80_interval_width"].gt(0).all()
        and forecast["p90_interval_width"].gt(forecast["p80_interval_width"]).all()
    )

    month_delta = (
        (backtest["target_month"].dt.year - backtest["origin_month"].dt.year) * 12
        + backtest["target_month"].dt.month
        - backtest["origin_month"].dt.month
    )
    chronology_ok = (
        backtest["target_month"].gt(backtest["origin_month"]).all()
        and month_delta.eq(backtest["horizon_month"]).all()
        and backtest["training_months"].ge(int(metadata.get("min_train_months", 0))).all()
    )
    overall_rows = summary.loc[summary["scope"].eq("All")]
    if len(overall_rows) != 1:
        return False, f"overall_rows={len(overall_rows)}"
    overall = overall_rows.iloc[0]
    summary_metrics = (
        "n_forecasts",
        "mae",
        "mape",
        "bias",
        "p80_coverage",
        "p90_coverage",
        "mean_p80_width",
        "mean_p80_relative_width",
    )
    reconciliation_ok = all(
        np.isclose(float(overall[key]), float(metadata.get("overall", {}).get(key, np.nan)), atol=1e-12, rtol=0)
        for key in summary_metrics
    )
    coverage_80 = float(overall["p80_coverage"])
    coverage_90 = float(overall["p90_coverage"])
    calibration_ok = (
        abs(coverage_80 - 0.80) <= 0.15
        and abs(coverage_90 - 0.90) <= 0.10
        and coverage_90 >= coverage_80
        and float(overall["mape"]) <= 0.05
        and abs(float(overall["bias"])) / float(backtest["actual_mrr"].mean()) <= 0.03
    )
    method = "local_trend_residual_block_bootstrap"
    method_ok = metadata.get("method") == method and forecast["method"].eq(method).all()
    valid = all(
        [
            forecast_quantiles_ok,
            backtest_quantiles_ok,
            forecast_shape_ok,
            chronology_ok,
            reconciliation_ok,
            calibration_ok,
            method_ok,
        ]
    )
    detail = (
        f"simulations={simulations}, forecast_horizon={horizon}, backtest_observations={len(backtest)}, "
        f"mape={float(overall['mape']):.3%}, p80_coverage={coverage_80:.2%}, p90_coverage={coverage_90:.2%}; "
        f"quantile_order={forecast_quantiles_ok and backtest_quantiles_ok}, chronology={chronology_ok}, "
        f"reconciliation={reconciliation_ok}"
    )
    return valid, detail


def _manifest_integrity(manifest: dict[str, Any], raw_dir: Path) -> tuple[bool, str]:
    table_metadata = manifest.get("tables", {})
    required_tables = set(REQUIRED_RAW_SCHEMAS)
    declared_tables = set(table_metadata) if isinstance(table_metadata, dict) else set()
    table_set_ok = declared_tables == required_tables
    files_ok = True
    for table_name in required_tables:
        metadata = table_metadata.get(table_name, {}) if isinstance(table_metadata, dict) else {}
        output_file = metadata.get("output_file")
        if not isinstance(output_file, str):
            files_ok = False
            continue
        path = (raw_dir / output_file).resolve()
        if path.parent != raw_dir.resolve() or not path.is_file():
            files_ok = False
            continue
        expected_rows = metadata.get("rows")
        with path.open(encoding="utf-8") as handle:
            observed_rows = sum(1 for _ in handle) - 1
        files_ok &= (
            isinstance(expected_rows, int)
            and observed_rows == expected_rows
            and _sha256(path) == metadata.get("output_sha256")
        )
    status_ok = manifest.get("manifest_version") == 1 and manifest.get("status") == "PASS"
    return status_ok and table_set_ok and files_ok, (
        f"status={manifest.get('status')}, declared_tables={len(declared_tables)}, "
        f"expected_tables={len(required_tables)}, file_hashes_and_rows={files_ok}"
    )


def _source_provenance_is_valid(base_dir: Path) -> tuple[bool, str, str, str]:
    raw_dir = base_dir / "data" / "raw"
    ingestion_path = raw_dir / "ingestion_manifest.json"
    synthetic_path = raw_dir / "synthetic_data_manifest.json"
    if ingestion_path.exists() and synthetic_path.exists():
        return False, "ambiguous", "Both real-ingestion and synthetic manifests exist.", "Critical"
    if ingestion_path.exists():
        manifest = json.loads(ingestion_path.read_text(encoding="utf-8"))
        integrity_ok, detail = _manifest_integrity(manifest, raw_dir)
        publication_allowed = manifest.get("publication_allowed") is True
        if not integrity_ok:
            return False, "real", detail, "Critical"
        if not publication_allowed:
            return (
                False,
                "real",
                detail + ", publication_allowed=False",
                "Critical",
            )
        return True, "real", detail + ", publication_allowed=True", "None"
    if synthetic_path.exists():
        manifest = json.loads(synthetic_path.read_text(encoding="utf-8"))
        integrity_ok, detail = _manifest_integrity(manifest, raw_dir)
        source_ok = manifest.get("source_type") == "synthetic" and isinstance(manifest.get("seed"), int)
        return (
            integrity_ok and source_ok,
            "synthetic",
            detail + f", seed={manifest.get('seed')}",
            ("None" if integrity_ok and source_ok else "High"),
        )
    return False, "unknown", "No governed source manifest found in data/raw.", "High"


def run_strategic_expansion_checks(
    base_dir: Path,
    tables: dict[str, pd.DataFrame],
    findings: list[Finding],
) -> None:
    """Append checks 22–24 for the strategic expansion outputs."""
    try:
        intervention_ok, intervention_detail = _intervention_evidence_is_valid(base_dir, tables)
    except (KeyError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        intervention_ok, intervention_detail = False, f"Evidence validation error: {exc}"
    add_finding(
        findings,
        "22",
        "Intervention Measurement Integrity",
        "Interventions",
        "PASS" if intervention_ok else "FAIL",
        "None" if intervention_ok else "High",
        intervention_detail,
        "Rebuild the assignment, outcome and effectiveness artifacts from the governed intervention pipeline."
        if not intervention_ok
        else "None",
    )

    try:
        forecast_ok, forecast_detail = _probabilistic_forecast_is_valid(base_dir, tables)
    except (KeyError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        forecast_ok, forecast_detail = False, f"Evidence validation error: {exc}"
    add_finding(
        findings,
        "23",
        "Probabilistic Forecast Integrity",
        "Forecasting",
        "PASS" if forecast_ok else "FAIL",
        "None" if forecast_ok else "High",
        forecast_detail,
        "Rebuild the probabilistic forecast and rolling-origin calibration artifacts." if not forecast_ok else "None",
    )

    try:
        provenance_ok, source_type, provenance_detail, severity = _source_provenance_is_valid(base_dir)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        provenance_ok, source_type, provenance_detail, severity = (
            False,
            "unknown",
            f"Manifest validation error: {exc}",
            "Critical",
        )
    add_finding(
        findings,
        "24",
        "Source Provenance & Publication Authorization",
        "Source Provenance",
        "PASS" if provenance_ok else "FAIL",
        "None" if provenance_ok else severity,
        f"source_type={source_type}; {provenance_detail}",
        "Publish only a complete checksummed snapshot with explicit authorization; use --skip-gate for confidential analysis."
        if not provenance_ok
        else "None",
    )

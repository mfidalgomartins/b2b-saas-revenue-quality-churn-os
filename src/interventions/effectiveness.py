"""Intent-to-treat retention uplift and commercial ROI measurement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.interventions.assignment import validate_assignment_ledger


@dataclass(frozen=True)
class EffectivenessConfig:
    followup_months: int = 3
    annualization_months: int = 12
    gross_margin: float = 0.80
    bootstrap_samples: int = 1000
    seed: int = 42


def _validate_config(config: EffectivenessConfig) -> None:
    if config.followup_months < 1 or config.annualization_months < 1:
        raise ValueError("followup and annualization months must be positive")
    if not 0 <= config.gross_margin <= 1:
        raise ValueError("gross_margin must be between 0 and 1")
    if config.bootstrap_samples < 100:
        raise ValueError("bootstrap_samples must be at least 100")


def attach_forward_outcomes(
    ledger: pd.DataFrame,
    account_monthly: pd.DataFrame,
    monthly_metrics: pd.DataFrame,
    followup_months: int,
) -> pd.DataFrame:
    """Attach observed outcomes after assignment and verify the baseline join."""
    ledger = validate_assignment_ledger(ledger)
    if ledger["assignment_month"].nunique() != 1:
        raise ValueError("One evaluation run must contain a single assignment month")
    if followup_months < 1:
        raise ValueError("followup_months must be positive")

    account_monthly = account_monthly.copy()
    monthly_metrics = monthly_metrics.copy()
    account_monthly["month"] = pd.to_datetime(account_monthly["month"], errors="raise")
    monthly_metrics["month"] = pd.to_datetime(monthly_metrics["month"], errors="raise")
    assignment_month = pd.Timestamp(ledger["assignment_month"].iloc[0])
    outcome_month = assignment_month + pd.DateOffset(months=followup_months)

    baseline = account_monthly.loc[account_monthly["month"].eq(assignment_month), ["customer_id", "active_mrr"]].rename(
        columns={"active_mrr": "verified_baseline_mrr"}
    )
    ending = account_monthly.loc[account_monthly["month"].eq(outcome_month), ["customer_id", "active_mrr"]].rename(
        columns={"active_mrr": "end_mrr"}
    )
    if ending.empty:
        raise ValueError(f"No account-month outcomes available for {outcome_month.date()}")

    followup = monthly_metrics.loc[
        monthly_metrics["month"].gt(assignment_month) & monthly_metrics["month"].le(outcome_month)
    ]
    churn = (
        followup.groupby("customer_id", as_index=False)["churn_flag"]
        .max()
        .rename(columns={"churn_flag": "churn_within_followup"})
    )
    outcomes = ledger.merge(baseline, on="customer_id", how="left", validate="one_to_one")
    outcomes = outcomes.merge(ending, on="customer_id", how="left", validate="one_to_one")
    outcomes = outcomes.merge(churn, on="customer_id", how="left", validate="one_to_one")
    if outcomes[["verified_baseline_mrr", "end_mrr", "churn_within_followup"]].isna().any().any():
        raise ValueError("Outcome join is incomplete for one or more assigned customers")
    if not np.allclose(outcomes["baseline_mrr"], outcomes["verified_baseline_mrr"], atol=0.01, rtol=0):
        raise ValueError("Ledger baseline_mrr does not match the assignment-month canonical mart")

    outcomes["outcome_month"] = outcome_month
    outcomes["logo_retained"] = outcomes["end_mrr"].gt(0).astype(int)
    outcomes["gross_retained_mrr"] = outcomes[["baseline_mrr", "end_mrr"]].min(axis=1)
    outcomes["gross_mrr_retention"] = outcomes["gross_retained_mrr"] / outcomes["baseline_mrr"]
    outcomes["net_mrr_retention"] = outcomes["end_mrr"] / outcomes["baseline_mrr"]
    outcomes["churn_within_followup"] = outcomes["churn_within_followup"].astype(int)
    return outcomes.drop(columns="verified_baseline_mrr").sort_values("assignment_id").reset_index(drop=True)


def _stratified_difference(data: pd.DataFrame, outcome: str, strata: list[str]) -> float:
    weighted_difference = 0.0
    represented_rows = 0
    estimates: list[tuple[int, float]] = []
    for _, group in data.groupby(strata, observed=True, sort=True):
        treated = group.loc[group["assignment_group"].eq("treatment"), outcome]
        control = group.loc[group["assignment_group"].eq("control"), outcome]
        if treated.empty or control.empty:
            continue
        estimates.append((len(group), float(treated.mean() - control.mean())))
        represented_rows += len(group)
    if represented_rows == 0:
        raise ValueError("No stratum contains both treatment and control observations")
    for count, difference in estimates:
        weighted_difference += count / represented_rows * difference
    return weighted_difference


def _bootstrap_interval(
    data: pd.DataFrame,
    outcome: str,
    strata: list[str],
    samples: int,
    rng: np.random.Generator,
) -> tuple[float, float]:
    group_columns = [*strata, "assignment_group"]
    groups = [group for _, group in data.groupby(group_columns, observed=True, sort=True)]
    estimates = np.empty(samples, dtype=float)
    for sample_index in range(samples):
        resampled = [group.iloc[rng.integers(0, len(group), size=len(group))] for group in groups]
        estimates[sample_index] = _stratified_difference(pd.concat(resampled, ignore_index=True), outcome, strata)
    lower, upper = np.quantile(estimates, [0.025, 0.975])
    return float(lower), float(upper)


def _standardized_mean_difference(treatment: pd.Series, control: pd.Series) -> float:
    pooled_variance = (float(treatment.var(ddof=1)) + float(control.var(ddof=1))) / 2
    if pooled_variance <= 0:
        return 0.0
    return float((treatment.mean() - control.mean()) / np.sqrt(pooled_variance))


def build_balance_table(outcomes: pd.DataFrame) -> pd.DataFrame:
    """Return signed and absolute pre-treatment standardized differences."""
    working = outcomes.assign(log_baseline_mrr=np.log1p(outcomes["baseline_mrr"]))
    rows: list[dict[str, Any]] = []
    for covariate in ("pre_risk_score", "log_baseline_mrr"):
        treatment = working.loc[working["assignment_group"].eq("treatment"), covariate]
        control = working.loc[working["assignment_group"].eq("control"), covariate]
        smd = _standardized_mean_difference(treatment, control)
        rows.append(
            {
                "covariate": covariate,
                "treatment_mean": float(treatment.mean()),
                "control_mean": float(control.mean()),
                "standardized_mean_difference": smd,
                "absolute_smd": abs(smd),
                "balance_status": "PASS" if abs(smd) <= 0.10 else "REVIEW",
            }
        )
    return pd.DataFrame(rows)


def _estimate_scope(
    outcomes: pd.DataFrame,
    scope: str,
    strata: list[str],
    config: EffectivenessConfig,
    rng: np.random.Generator,
) -> dict[str, Any]:
    logo_uplift = _stratified_difference(outcomes, "logo_retained", strata)
    logo_lower, logo_upper = _bootstrap_interval(outcomes, "logo_retained", strata, config.bootstrap_samples, rng)
    mrr_uplift = _stratified_difference(outcomes, "gross_mrr_retention", strata)
    mrr_lower, mrr_upper = _bootstrap_interval(outcomes, "gross_mrr_retention", strata, config.bootstrap_samples, rng)
    treated = outcomes.loc[outcomes["assignment_group"].eq("treatment")]
    control = outcomes.loc[outcomes["assignment_group"].eq("control")]
    treated_baseline_mrr = float(treated["baseline_mrr"].sum())
    total_cost = float(treated["intervention_cost"].sum())
    incremental_retained_mrr = mrr_uplift * treated_baseline_mrr
    annualized_gross_profit = incremental_retained_mrr * config.annualization_months * config.gross_margin
    roi = (annualized_gross_profit - total_cost) / total_cost if total_cost > 0 else np.nan
    roi_lower = (
        (mrr_lower * treated_baseline_mrr * config.annualization_months * config.gross_margin - total_cost) / total_cost
        if total_cost > 0
        else np.nan
    )
    roi_upper = (
        (mrr_upper * treated_baseline_mrr * config.annualization_months * config.gross_margin - total_cost) / total_cost
        if total_cost > 0
        else np.nan
    )
    if mrr_lower > 0 and roi > 0:
        recommendation = "scale_candidate"
    elif mrr_upper <= 0 or roi <= 0:
        recommendation = "do_not_scale"
    else:
        recommendation = "continue_test"
    return {
        "scope": scope,
        "n_total": len(outcomes),
        "n_treatment": len(treated),
        "n_control": len(control),
        "treatment_logo_retention": float(treated["logo_retained"].mean()),
        "control_logo_retention": float(control["logo_retained"].mean()),
        "logo_retention_uplift": logo_uplift,
        "logo_uplift_ci_lower": logo_lower,
        "logo_uplift_ci_upper": logo_upper,
        "treatment_gross_mrr_retention": float(treated["gross_mrr_retention"].mean()),
        "control_gross_mrr_retention": float(control["gross_mrr_retention"].mean()),
        "gross_mrr_retention_uplift": mrr_uplift,
        "mrr_uplift_ci_lower": mrr_lower,
        "mrr_uplift_ci_upper": mrr_upper,
        "treated_baseline_mrr": treated_baseline_mrr,
        "intervention_cost": total_cost,
        "incremental_retained_mrr": incremental_retained_mrr,
        "annualized_incremental_gross_profit": annualized_gross_profit,
        "commercial_roi": float(roi),
        "roi_ci_lower": float(roi_lower),
        "roi_ci_upper": float(roi_upper),
        "recommendation": recommendation,
    }


def evaluate_intervention(
    outcomes: pd.DataFrame,
    config: EffectivenessConfig = EffectivenessConfig(),
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Estimate blocked ITT uplift globally and by segment with uncertainty."""
    _validate_config(config)
    required = {
        "assignment_group",
        "segment",
        "risk_band",
        "logo_retained",
        "gross_mrr_retention",
        "baseline_mrr",
        "intervention_cost",
    }
    if missing := required - set(outcomes):
        raise ValueError(f"Outcome table missing fields: {sorted(missing)}")

    rng = np.random.default_rng(config.seed)
    overall = _estimate_scope(outcomes, "All", ["segment", "risk_band"], config, rng)
    segment_rows = [
        _estimate_scope(segment_data, str(segment), ["risk_band"], config, rng)
        for segment, segment_data in outcomes.groupby("segment", observed=True, sort=True)
    ]
    summary = pd.DataFrame([overall, *segment_rows])
    balance = build_balance_table(outcomes)
    metadata: dict[str, Any] = {
        "design": "blocked_randomized_intent_to_treat",
        "estimand": "assignment effect among eligible active high-risk accounts",
        "unit": "customer account",
        "treatment": "assignment to success-plan outreach",
        "counterfactual": "assignment to no-action control",
        "followup_months": config.followup_months,
        "confidence_interval": "95% stratified nonparametric bootstrap",
        "bootstrap_samples": config.bootstrap_samples,
        "gross_margin": config.gross_margin,
        "annualization_months": config.annualization_months,
        "max_absolute_smd": float(balance["absolute_smd"].max()),
        "overall": overall,
    }
    return summary, balance, metadata

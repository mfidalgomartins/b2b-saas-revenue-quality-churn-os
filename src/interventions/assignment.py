"""Leakage-safe assignment ledger for the synthetic retention experiment."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import numpy as np
import pandas as pd

ASSIGNMENT_COLUMNS = (
    "experiment_id",
    "assignment_id",
    "customer_id",
    "assignment_month",
    "assignment_group",
    "intervention_type",
    "account_manager_id",
    "segment",
    "risk_band",
    "pre_risk_score",
    "baseline_mrr",
    "intervention_cost",
    "assignment_probability",
)


@dataclass(frozen=True)
class AssignmentConfig:
    experiment_id: str = "retention_outreach_2025q4"
    followup_months: int = 3
    eligible_risk_quantile: float = 0.50
    seed: int = 42


def _validate_config(config: AssignmentConfig) -> None:
    if config.followup_months < 1:
        raise ValueError("followup_months must be positive")
    if not 0 < config.eligible_risk_quantile < 1:
        raise ValueError("eligible_risk_quantile must be between 0 and 1")
    if not config.experiment_id.strip():
        raise ValueError("experiment_id cannot be empty")


def _pre_risk_score(frame: pd.DataFrame) -> pd.Series:
    """Policy score using only fields observed at the assignment month."""
    discount_level = np.clip(frame["avg_discount_pct"] / 0.50, 0.0, 1.0)
    realization_gap = np.clip(1.0 - frame["realized_price_index"], 0.0, 1.0)
    score = (
        0.45 * frame["renewal_risk_proxy"]
        + 0.25 * discount_level
        + 0.20 * realization_gap
        + 0.10 * frame["discount_dependency_flag"]
    )
    return 100.0 * np.clip(score, 0.0, 1.0)


def _risk_bands(scores: pd.Series) -> pd.Series:
    band_number = pd.qcut(scores.rank(method="first"), q=min(4, len(scores)), labels=False, duplicates="drop")
    return band_number.map(lambda value: f"B{int(value) + 1}")


def _assignment_cost(segment: pd.Series, treated: pd.Series) -> pd.Series:
    cost_by_segment = {"SMB": 350.0, "Mid-Market": 900.0, "Enterprise": 2500.0}
    unknown = sorted(set(segment) - set(cost_by_segment))
    if unknown:
        raise ValueError(f"No intervention cost configured for segments: {unknown}")
    return segment.map(cost_by_segment).astype(float) * treated


def _stable_assignment_id(experiment_id: str, customer_id: str) -> str:
    digest = hashlib.sha256(f"{experiment_id}:{customer_id}".encode()).hexdigest()[:16]
    return f"ASG-{digest}"


def build_randomized_assignment(
    account_monthly: pd.DataFrame,
    customers: pd.DataFrame,
    config: AssignmentConfig = AssignmentConfig(),
) -> pd.DataFrame:
    """Create a blocked 50/50 assignment from a pre-treatment snapshot.

    Eligibility is the upper within-segment risk quantile among active accounts.
    Assignment is randomized within segment and risk band. No forward outcome is
    read or passed to this function.
    """
    _validate_config(config)
    required_account = {
        "customer_id",
        "month",
        "active_mrr",
        "realized_price_index",
        "avg_discount_pct",
        "discount_dependency_flag",
        "renewal_risk_proxy",
    }
    required_customers = {"customer_id", "segment", "account_manager_id"}
    if missing := required_account - set(account_monthly):
        raise ValueError(f"account_monthly missing fields: {sorted(missing)}")
    if missing := required_customers - set(customers):
        raise ValueError(f"customers missing fields: {sorted(missing)}")

    account_monthly = account_monthly.copy()
    account_monthly["month"] = pd.to_datetime(account_monthly["month"], errors="raise")
    outcome_month = account_monthly["month"].max()
    assignment_month = outcome_month - pd.DateOffset(months=config.followup_months)
    population = account_monthly.loc[
        account_monthly["month"].eq(assignment_month) & account_monthly["active_mrr"].gt(0)
    ].copy()
    population = population.merge(
        customers[["customer_id", "segment", "account_manager_id"]],
        on="customer_id",
        how="left",
        validate="one_to_one",
    )
    if population[["segment", "account_manager_id"]].isna().any().any():
        raise ValueError("Assignment population contains customers without segment or account manager")

    population["pre_risk_score"] = _pre_risk_score(population)
    threshold = population.groupby("segment")["pre_risk_score"].transform(
        lambda values: values.quantile(config.eligible_risk_quantile)
    )
    eligible = population.loc[population["pre_risk_score"].ge(threshold)].copy()
    eligible["risk_band"] = eligible.groupby("segment", group_keys=False)["pre_risk_score"].transform(_risk_bands)
    eligible["is_treated"] = 0

    rng = np.random.default_rng(config.seed)
    groups = eligible.groupby(["segment", "risk_band"], observed=True, sort=True).groups
    for indexes in groups.values():
        shuffled = np.asarray(list(indexes), dtype=int)
        rng.shuffle(shuffled)
        eligible.loc[shuffled[: len(shuffled) // 2], "is_treated"] = 1

    eligible["experiment_id"] = config.experiment_id
    eligible["assignment_id"] = eligible["customer_id"].map(
        lambda customer_id: _stable_assignment_id(config.experiment_id, str(customer_id))
    )
    eligible["assignment_month"] = assignment_month
    eligible["assignment_group"] = np.where(eligible["is_treated"].eq(1), "treatment", "control")
    eligible["intervention_type"] = np.where(eligible["is_treated"].eq(1), "success_plan_outreach", "no_action_control")
    eligible["baseline_mrr"] = eligible["active_mrr"]
    eligible["intervention_cost"] = _assignment_cost(eligible["segment"], eligible["is_treated"])
    eligible["assignment_probability"] = 0.50

    assignment = eligible[list(ASSIGNMENT_COLUMNS)].copy()
    assignment["pre_risk_score"] = assignment["pre_risk_score"].round(6)
    assignment["baseline_mrr"] = assignment["baseline_mrr"].round(2)
    return assignment.sort_values("assignment_id").reset_index(drop=True)


def validate_assignment_ledger(ledger: pd.DataFrame) -> pd.DataFrame:
    """Validate an experimental assignment ledger before outcome attachment."""
    missing = set(ASSIGNMENT_COLUMNS) - set(ledger)
    if missing:
        raise ValueError(f"Assignment ledger missing fields: {sorted(missing)}")
    validated = ledger.copy()
    validated["assignment_month"] = pd.to_datetime(validated["assignment_month"], errors="raise")
    if validated["assignment_id"].isna().any() or validated["assignment_id"].duplicated().any():
        raise ValueError("assignment_id must be unique and non-null")
    if validated.duplicated(["experiment_id", "customer_id"]).any():
        raise ValueError("Each customer may be assigned only once per experiment")
    if not set(validated["assignment_group"]).issubset({"treatment", "control"}):
        raise ValueError("assignment_group must contain only treatment or control")
    if not validated["assignment_probability"].between(0, 1, inclusive="neither").all():
        raise ValueError("assignment_probability must be strictly between 0 and 1")
    if not validated["pre_risk_score"].between(0, 100).all():
        raise ValueError("pre_risk_score must be between 0 and 100")
    if not validated["baseline_mrr"].gt(0).all():
        raise ValueError("baseline_mrr must be positive")
    if not validated["intervention_cost"].ge(0).all():
        raise ValueError("intervention_cost cannot be negative")
    control_cost = validated.loc[validated["assignment_group"].eq("control"), "intervention_cost"]
    if not control_cost.eq(0).all():
        raise ValueError("Control assignments must have zero intervention cost")
    return validated

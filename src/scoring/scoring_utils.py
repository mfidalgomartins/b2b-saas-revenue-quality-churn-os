"""Shared scoring primitives.

A single source of truth for risk-tier thresholds, weighted score composition,
and the churn-risk component formulas. Production scoring, calibration backtest,
and validation all import from here so the model definition cannot drift.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

# -- Risk tier thresholds (a higher score means higher risk) -------------------

LOW_THRESHOLD = 30.0
MODERATE_THRESHOLD = 55.0
HIGH_THRESHOLD = 75.0

RISK_TIERS = ("Low", "Moderate", "High", "Critical")


def risk_tier(score: float) -> str:
    """Map a 0-100 risk score to a tier label."""
    if score < LOW_THRESHOLD:
        return "Low"
    if score < MODERATE_THRESHOLD:
        return "Moderate"
    if score < HIGH_THRESHOLD:
        return "High"
    return "Critical"


def quality_to_risk_tier(score: float) -> str:
    """Map a 0-100 quality score (higher = better) to a risk tier label."""
    return risk_tier(100.0 - score)


# -- Component composition ----------------------------------------------------


def clip01(values: pd.Series | np.ndarray) -> pd.Series | np.ndarray:
    """Clip values to the unit interval [0, 1]."""
    return np.clip(values, 0.0, 1.0)


def score_from_components(
    components: Mapping[str, pd.Series],
    weights: Mapping[str, float],
) -> pd.Series:
    """Compose a 0-100 score from named [0,1] components and matching weights."""
    weighted = None
    for key, comp in components.items():
        contribution = weights[key] * comp
        weighted = contribution if weighted is None else weighted + contribution
    if weighted is None:
        raise ValueError("score_from_components requires at least one component")
    return (100.0 * weighted).round(3)


def component_contributions(
    components: Mapping[str, pd.Series],
    weights: Mapping[str, float],
) -> dict[str, pd.Series]:
    """Return the per-component contribution to a weighted score."""
    return {key: weights[key] * components[key] for key in components}


# -- Canonical scoring weights ------------------------------------------------

CHURN_WEIGHTS: dict[str, float] = {
    "usage_deterioration": 0.25,
    "sentiment_support": 0.15,
    "payment_stress": 0.20,
    "commercial_contraction": 0.15,
    "discount_pressure": 0.10,
    "renewal_exposure": 0.10,
    "history_tenure": 0.05,
}

REVENUE_QUALITY_WEIGHTS: dict[str, float] = {
    "pricing_realization": 0.30,
    "discount_discipline": 0.20,
    "retention_momentum": 0.20,
    "account_health_quality": 0.20,
    "stability_governance": 0.10,
}

DISCOUNT_DEPENDENCY_WEIGHTS: dict[str, float] = {
    "discount_level": 0.40,
    "discount_persistence": 0.25,
    "discounted_expansion_pressure": 0.15,
    "price_realization_erosion": 0.15,
    "policy_signal": 0.05,
}

EXPANSION_QUALITY_WEIGHTS: dict[str, float] = {
    "healthy_expansion_mix": 0.35,
    "fragility_control": 0.20,
    "expansion_discount_discipline": 0.20,
    "expansion_payment_quality": 0.10,
    "post_expansion_durability": 0.15,
}

GOVERNANCE_WEIGHTS: dict[str, float] = {
    "churn_risk": 0.32,
    "revenue_quality_risk": 0.18,
    "discount_dependency": 0.15,
    "expansion_fragility": 0.10,
    "exposure_concentration": 0.20,
    "renewal_urgency": 0.05,
}


# -- Churn risk components (canonical) ---------------------------------------
#
# Each component takes column-like inputs from a trailing-feature frame and
# returns a Series in [0, 1]. This is the single definition used by both the
# production scorer and the calibration backtest.


def compute_churn_components(features: pd.DataFrame) -> dict[str, pd.Series]:
    """Compute the seven churn-risk components from a trailing-feature frame.

    Required columns:
        trailing_3m_usage_avg, trailing_3m_usage_trend,
        trailing_3m_nps_avg, trailing_3m_support_ticket_avg,
        trailing_3m_payment_delay_avg, trailing_3m_discount_avg,
        contraction_frequency, seat_growth_rate,
        heavy_discount_frequency_12m,
        renewal_due_flag, renewal_risk_proxy,
        churn_history_flag, tenure_months
    """
    return {
        "usage_deterioration": (
            0.65 * clip01((55 - features["trailing_3m_usage_avg"]) / 35)
            + 0.35 * clip01((-features["trailing_3m_usage_trend"]) / 4)
        ),
        "sentiment_support": (
            0.70 * clip01((15 - features["trailing_3m_nps_avg"]) / 55)
            + 0.30 * clip01((features["trailing_3m_support_ticket_avg"] - 4) / 8)
        ),
        "payment_stress": clip01(features["trailing_3m_payment_delay_avg"] / 35),
        "commercial_contraction": (
            0.70 * clip01(features["contraction_frequency"] / 0.35)
            + 0.30 * clip01((-features["seat_growth_rate"]) / 0.25)
        ),
        "discount_pressure": (
            0.60 * clip01((features["trailing_3m_discount_avg"] - 0.15) / 0.25)
            + 0.40 * clip01(features["heavy_discount_frequency_12m"] / 0.60)
        ),
        "renewal_exposure": clip01(
            features["renewal_due_flag"] * features["renewal_risk_proxy"] + features["renewal_due_flag"] * 0.20
        ),
        "history_tenure": (0.70 * features["churn_history_flag"] + 0.30 * clip01((6 - features["tenure_months"]) / 6)),
    }


def argmax_driver(row: pd.Series, mapping: Mapping[str, str], keys: list[str]) -> str:
    """Pick the driver label of the largest contribution column."""
    best_key = max(keys, key=lambda k: float(row[k]))
    return mapping.get(best_key, best_key)

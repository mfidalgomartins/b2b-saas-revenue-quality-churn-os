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


def compute_revenue_quality_components(features: pd.DataFrame) -> dict[str, pd.Series]:
    """Compute the five revenue-quality components (higher = healthier quality).

    Required columns: realized_price_index, trailing_3m_discount_avg,
    contraction_frequency, expansion_frequency, trailing_3m_usage_avg,
    trailing_3m_nps_avg, trailing_3m_payment_delay_avg, renewal_risk_proxy,
    churn_history_flag, quality_flag_health_factor.
    """
    return {
        "pricing_realization": clip01((features["realized_price_index"] - 0.72) / 0.30),
        "discount_discipline": 1 - clip01((features["trailing_3m_discount_avg"] - 0.12) / 0.25),
        "retention_momentum": 0.55 * (1 - clip01(features["contraction_frequency"] / 0.35))
        + 0.45 * clip01(features["expansion_frequency"] / 0.35),
        "account_health_quality": 0.40 * clip01((features["trailing_3m_usage_avg"] - 50) / 30)
        + 0.35 * clip01((features["trailing_3m_nps_avg"] + 10) / 45)
        + 0.25 * (1 - clip01(features["trailing_3m_payment_delay_avg"] / 30)),
        "stability_governance": 0.50 * (1 - features["renewal_risk_proxy"])
        + 0.30 * (1 - features["churn_history_flag"])
        + 0.20 * features["quality_flag_health_factor"],
    }


def compute_discount_dependency_components(features: pd.DataFrame) -> dict[str, pd.Series]:
    """Compute the five discount-dependency components (higher = more dependency risk).

    Required columns: trailing_3m_discount_avg, heavy_discount_frequency_12m,
    discounted_expansion_share_12m, realized_price_index,
    discount_dependency_flag, manager_discount_outlier_flag.
    """
    return {
        "discount_level": clip01((features["trailing_3m_discount_avg"] - 0.12) / 0.25),
        "discount_persistence": clip01(features["heavy_discount_frequency_12m"] / 0.70),
        "discounted_expansion_pressure": clip01(features["discounted_expansion_share_12m"] / 0.80),
        "price_realization_erosion": clip01((0.90 - features["realized_price_index"]) / 0.35),
        "policy_signal": np.maximum(features["discount_dependency_flag"], features["manager_discount_outlier_flag"]),
    }


def compute_expansion_quality_components(features: pd.DataFrame) -> dict[str, pd.Series]:
    """Compute the five expansion-quality components (higher = healthier expansion).

    Required columns: healthy_expansion_ratio_12m, fragile_expansion_ratio_12m,
    avg_expansion_discount_12m, avg_expansion_payment_delay_12m,
    post_expansion_contraction_rate_3m.
    """
    return {
        "healthy_expansion_mix": clip01(features["healthy_expansion_ratio_12m"] / 0.80),
        "fragility_control": 1 - clip01(features["fragile_expansion_ratio_12m"] / 0.80),
        "expansion_discount_discipline": 1 - clip01((features["avg_expansion_discount_12m"] - 0.12) / 0.28),
        "expansion_payment_quality": 1 - clip01((features["avg_expansion_payment_delay_12m"] - 8) / 25),
        "post_expansion_durability": 1 - clip01(features["post_expansion_contraction_rate_3m"] / 0.70),
    }


def argmax_driver(row: pd.Series, mapping: Mapping[str, str], keys: list[str]) -> str:
    """Pick the driver label of the largest contribution column."""
    best_key = max(keys, key=lambda k: float(row[k]))
    return mapping.get(best_key, best_key)


# -- Trailing trend ----------------------------------------------------------


def rolling_trailing_slope(values: pd.Series | np.ndarray, window: int = 3) -> np.ndarray:
    """Trailing least-squares slope over an equally-spaced window (``min_periods=1``).

    For each position ``i`` this returns the slope of an ordinary-least-squares
    line fitted to the last ``min(i+1, window)`` points with ``x = 0..k-1`` — i.e.
    exactly ``np.polyfit(np.arange(k), v, 1)[0]``. Positions with fewer than two
    points return ``0.0``.

    Implemented in closed form (vectorised, no per-window solve). For an
    equally-spaced window of at most three points the OLS slope reduces to
    ``(last - first) / (k - 1)`` and is independent of the interior point, so the
    result is exact for ``window <= 3`` — the only case the pipeline uses.
    """
    if window > 3:
        raise ValueError("rolling_trailing_slope is exact only for window <= 3")
    v = np.asarray(values, dtype=float)
    out = np.zeros(len(v), dtype=float)
    if len(v) >= 2:
        out[1] = v[1] - v[0]  # two-point window
    if window >= 3 and len(v) >= 3:
        out[2:] = (v[2:] - v[:-2]) / 2.0  # three-point window: (v_i - v_{i-2}) / 2
    elif window == 2 and len(v) >= 3:
        out[2:] = v[2:] - v[1:-1]
    return out

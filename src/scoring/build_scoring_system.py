from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def clip01(series: pd.Series | np.ndarray) -> pd.Series:
    return np.clip(series, 0.0, 1.0)


def risk_tier(score: float) -> str:
    if score < 30:
        return "Low"
    if score < 55:
        return "Moderate"
    if score < 75:
        return "High"
    return "Critical"


def quality_to_risk_tier(score: float) -> str:
    # Higher quality score = lower risk. Convert to risk scale for consistent tier labels.
    return risk_tier(100.0 - score)


def score_from_components(components: Dict[str, pd.Series], weights: Dict[str, float]) -> pd.Series:
    weighted = None
    for key, comp in components.items():
        contrib = weights[key] * comp
        weighted = contrib if weighted is None else weighted + contrib
    return (100.0 * weighted).round(3)


def component_contributions(components: Dict[str, pd.Series], weights: Dict[str, float]) -> Dict[str, pd.Series]:
    return {k: (weights[k] * components[k]) for k in components}


def argmax_driver(row: pd.Series, mapping: Dict[str, str], keys: List[str]) -> str:
    best_key = max(keys, key=lambda k: float(row[k]))
    return mapping.get(best_key, best_key)


def load_inputs(base_dir: Path) -> Dict[str, pd.DataFrame]:
    raw = base_dir / "data/raw"
    processed = base_dir / "data/processed"

    tables: Dict[str, pd.DataFrame] = {
        "customers": pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"]),
        "monthly": pd.read_csv(processed / "account_monthly_revenue_quality.csv", parse_dates=["month"]),
        "health": pd.read_csv(processed / "customer_health_features.csv"),
        "manager_summary": pd.read_csv(processed / "account_manager_summary.csv"),
        "risk_base": pd.read_csv(processed / "account_risk_base.csv", parse_dates=["current_month"]),
        "monthly_raw": pd.read_csv(raw / "monthly_account_metrics.csv", parse_dates=["month"]),
        "account_managers": pd.read_csv(raw / "account_managers.csv"),
    }
    return tables


def build_trailing_12m_features(
    monthly_quality: pd.DataFrame,
    monthly_raw: pd.DataFrame,
    latest_month: pd.Timestamp,
) -> pd.DataFrame:
    start_12m = latest_month - pd.DateOffset(months=11)

    panel = monthly_quality.merge(
        monthly_raw[
            [
                "customer_id",
                "month",
                "active_flag",
                "product_usage_score",
                "nps_score",
                "payment_delay_days",
                "support_tickets",
                "churn_flag",
            ]
        ],
        on=["customer_id", "month"],
        how="left",
    )

    panel = panel[(panel["month"] >= start_12m) & (panel["month"] <= latest_month)].copy()
    panel = panel.sort_values(["customer_id", "month"]).reset_index(drop=True)

    panel["is_active"] = (panel["active_mrr"] > 0).astype(int)
    panel["heavy_discount"] = ((panel["avg_discount_pct"] >= 0.25) & (panel["is_active"] == 1)).astype(int)
    panel["is_expansion"] = ((panel["expansion_mrr"] > 0) & (panel["is_active"] == 1)).astype(int)

    panel["healthy_expansion_event"] = (
        (panel["is_expansion"] == 1)
        & (panel["product_usage_score"] >= 65)
        & (panel["nps_score"] >= 20)
        & (panel["payment_delay_days"] <= 15)
        & (panel["avg_discount_pct"] <= 0.20)
    ).astype(int)

    panel["fragile_expansion_event"] = (
        (panel["is_expansion"] == 1)
        & (
            (panel["avg_discount_pct"] >= 0.25)
            | (panel["product_usage_score"] < 55)
            | (panel["payment_delay_days"] > 20)
            | (panel["nps_score"] < 10)
        )
    ).astype(int)

    grouped = panel.groupby("customer_id", as_index=False).agg(
        active_months_12=("is_active", "sum"),
        heavy_discount_months_12=("heavy_discount", "sum"),
        expansion_events_12=("is_expansion", "sum"),
        healthy_expansion_events_12=("healthy_expansion_event", "sum"),
        fragile_expansion_events_12=("fragile_expansion_event", "sum"),
        total_expansion_mrr_12=("expansion_mrr", "sum"),
        discounted_expansion_mrr_12=(
            "expansion_mrr",
            lambda s: float(s[panel.loc[s.index, "avg_discount_pct"] >= 0.25].sum()),
        ),
    )

    # Mean discount and payment during expansion events only.
    exp_discount = (
        panel[panel["is_expansion"] == 1]
        .groupby("customer_id", as_index=False)["avg_discount_pct"]
        .mean()
        .rename(columns={"avg_discount_pct": "avg_expansion_discount_12m"})
    )
    exp_delay = (
        panel[panel["is_expansion"] == 1]
        .groupby("customer_id", as_index=False)["payment_delay_days"]
        .mean()
        .rename(columns={"payment_delay_days": "avg_expansion_payment_delay_12m"})
    )

    # Post-expansion contraction rate within next 3 months.
    post_contraction_rows: List[Dict[str, object]] = []
    for cid, g in panel.groupby("customer_id"):
        g = g.sort_values("month")
        exp_rows = g[g["is_expansion"] == 1]
        checks: List[int] = []
        for _, exp_row in exp_rows.iterrows():
            later = g[(g["month"] > exp_row["month"]) & (g["month"] <= exp_row["month"] + pd.DateOffset(months=3))]
            checks.append(int((later["contraction_mrr"] > 0).any()))
        rate = float(np.mean(checks)) if checks else 0.0
        post_contraction_rows.append({"customer_id": cid, "post_expansion_contraction_rate_3m": rate})

    post_contraction = pd.DataFrame(post_contraction_rows)

    out = grouped.merge(exp_discount, on="customer_id", how="left").merge(exp_delay, on="customer_id", how="left").merge(
        post_contraction, on="customer_id", how="left"
    )

    out["heavy_discount_frequency_12m"] = np.where(
        out["active_months_12"] > 0,
        out["heavy_discount_months_12"] / out["active_months_12"],
        0.0,
    )
    out["healthy_expansion_ratio_12m"] = np.where(
        out["expansion_events_12"] > 0,
        out["healthy_expansion_events_12"] / out["expansion_events_12"],
        0.0,
    )
    out["fragile_expansion_ratio_12m"] = np.where(
        out["expansion_events_12"] > 0,
        out["fragile_expansion_events_12"] / out["expansion_events_12"],
        0.0,
    )
    out["discounted_expansion_share_12m"] = np.where(
        out["total_expansion_mrr_12"] > 0,
        out["discounted_expansion_mrr_12"] / out["total_expansion_mrr_12"],
        0.0,
    )

    fill_cols = [
        "avg_expansion_discount_12m",
        "avg_expansion_payment_delay_12m",
        "post_expansion_contraction_rate_3m",
    ]
    for col in fill_cols:
        out[col] = out[col].fillna(0.0)

    return out


def assign_recommended_action(row: pd.Series) -> str:
    if row["governance_priority_tier"] == "Low":
        return "monitor only"

    if row["governance_priority_score"] >= 70 and row["concentration_weight"] >= 0.01:
        return "reduce exposure concentration"

    if row["churn_risk_score"] >= 68 and row["churn_risk_main_driver"] in [
        "Usage deterioration",
        "Sentiment/support deterioration",
        "Payment stress",
    ]:
        if row["renewal_due_flag"] == 1:
            return "prepare renewal intervention"
        return "escalate to customer success"

    if row["churn_risk_score"] >= 70:
        return "investigate account health deterioration"

    if row["governance_priority_tier"] == "Critical" and row["renewal_due_flag"] == 1:
        if row["discount_dependency_score"] >= 70:
            return "reprice at renewal"
        return "prepare renewal intervention"

    if row["discount_dependency_score"] >= 75 and row["manager_discount_outlier_flag"] == 1:
        return "review account manager behavior"

    if row["discount_dependency_score"] >= 75:
        if row["renewal_due_flag"] == 1:
            return "reprice at renewal"
        return "review discount policy"

    if row["renewal_due_flag"] == 1 and row["governance_priority_tier"] in ["High", "Critical"]:
        return "prepare renewal intervention"

    return "monitor only"


def build_scores(base_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    tables = load_inputs(base_dir)
    customers = tables["customers"]
    health = tables["health"].copy()
    monthly_quality = tables["monthly"].copy()
    monthly_raw = tables["monthly_raw"].copy()
    manager_summary = tables["manager_summary"].copy()
    risk_base = tables["risk_base"].copy()

    latest_month = monthly_quality["month"].max()

    latest_quality = monthly_quality[monthly_quality["month"] == latest_month][
        [
            "customer_id",
            "active_mrr",
            "realized_price_index",
            "avg_discount_pct",
            "discount_dependency_flag",
            "revenue_quality_flag",
            "renewal_risk_proxy",
            "expansion_mrr",
            "contraction_mrr",
        ]
    ].copy()

    trailing = build_trailing_12m_features(monthly_quality, monthly_raw, latest_month)

    score_df = (
        health.merge(latest_quality, on="customer_id", how="left")
        .merge(trailing, on="customer_id", how="left")
        .merge(customers[["customer_id", "segment", "region", "industry", "acquisition_channel", "account_manager_id"]], on="customer_id", how="left")
        .merge(
            manager_summary[["account_manager_id", "avg_discount", "retention_rate", "churn_rate", "risk_weighted_portfolio_score"]].rename(
                columns={
                    "avg_discount": "manager_avg_discount",
                    "retention_rate": "manager_retention_rate",
                    "churn_rate": "manager_churn_rate",
                    "risk_weighted_portfolio_score": "manager_risk_score",
                }
            ),
            on="account_manager_id",
            how="left",
        )
    )

    # Parse existing forward risk flags for shortlist context.
    risk_base = risk_base[["customer_id", "forward_risk_flags"]].copy()
    risk_base["forward_risk_flags_list"] = risk_base["forward_risk_flags"].apply(json.loads)
    score_df = score_df.merge(risk_base[["customer_id", "forward_risk_flags_list"]], on="customer_id", how="left")

    num_fill_cols = [
        "active_mrr",
        "realized_price_index",
        "avg_discount_pct",
        "renewal_risk_proxy",
        "expansion_mrr",
        "contraction_mrr",
        "manager_avg_discount",
        "manager_retention_rate",
        "manager_churn_rate",
        "manager_risk_score",
        "active_months_12",
        "heavy_discount_months_12",
        "expansion_events_12",
        "healthy_expansion_events_12",
        "fragile_expansion_events_12",
        "total_expansion_mrr_12",
        "discounted_expansion_mrr_12",
        "heavy_discount_frequency_12m",
        "healthy_expansion_ratio_12m",
        "fragile_expansion_ratio_12m",
        "discounted_expansion_share_12m",
        "avg_expansion_discount_12m",
        "avg_expansion_payment_delay_12m",
        "post_expansion_contraction_rate_3m",
    ]
    for col in num_fill_cols:
        if col in score_df.columns:
            score_df[col] = score_df[col].fillna(0.0)

    score_df["forward_risk_flags_list"] = score_df["forward_risk_flags_list"].apply(lambda x: x if isinstance(x, list) else [])

    manager_discount_p90 = float(score_df["manager_avg_discount"].quantile(0.90))
    score_df["manager_discount_outlier_flag"] = (score_df["manager_avg_discount"] >= manager_discount_p90).astype(int)

    # -------------------------
    # 1) Churn Risk Score (0-100, higher = riskier)
    # -------------------------
    churn_components = {
        "usage_deterioration": 0.65 * clip01((55 - score_df["trailing_3m_usage_avg"]) / 35)
        + 0.35 * clip01((-score_df["trailing_3m_usage_trend"]) / 4),
        "sentiment_support": 0.70 * clip01((15 - score_df["trailing_3m_nps_avg"]) / 55)
        + 0.30 * clip01((score_df["trailing_3m_support_ticket_avg"] - 4) / 8),
        "payment_stress": clip01(score_df["trailing_3m_payment_delay_avg"] / 35),
        "commercial_contraction": 0.70 * clip01(score_df["contraction_frequency"] / 0.35)
        + 0.30 * clip01((-score_df["seat_growth_rate"]) / 0.25),
        "discount_pressure": 0.60 * clip01((score_df["trailing_3m_discount_avg"] - 0.15) / 0.25)
        + 0.40 * clip01(score_df["heavy_discount_frequency_12m"] / 0.60),
        "renewal_exposure": clip01(score_df["renewal_due_flag"] * score_df["renewal_risk_proxy"] + score_df["renewal_due_flag"] * 0.20),
        "history_tenure": 0.70 * score_df["churn_history_flag"] + 0.30 * clip01((6 - score_df["tenure_months"]) / 6),
    }
    churn_weights = {
        "usage_deterioration": 0.25,
        "sentiment_support": 0.15,
        "payment_stress": 0.20,
        "commercial_contraction": 0.15,
        "discount_pressure": 0.10,
        "renewal_exposure": 0.10,
        "history_tenure": 0.05,
    }
    score_df["churn_risk_score"] = score_from_components(churn_components, churn_weights)
    score_df["churn_risk_tier"] = score_df["churn_risk_score"].apply(risk_tier)

    churn_contrib = component_contributions(churn_components, churn_weights)
    for key, val in churn_contrib.items():
        score_df[f"churn_contrib_{key}"] = val

    churn_driver_map = {
        "churn_contrib_usage_deterioration": "Usage deterioration",
        "churn_contrib_sentiment_support": "Sentiment/support deterioration",
        "churn_contrib_payment_stress": "Payment stress",
        "churn_contrib_commercial_contraction": "Commercial contraction pattern",
        "churn_contrib_discount_pressure": "Discount pressure",
        "churn_contrib_renewal_exposure": "Renewal exposure",
        "churn_contrib_history_tenure": "History/early-tenure fragility",
    }
    churn_keys = list(churn_driver_map.keys())
    score_df["churn_risk_main_driver"] = score_df[churn_keys].apply(
        lambda r: argmax_driver(r, churn_driver_map, churn_keys),
        axis=1,
    )

    # -------------------------
    # 2) Revenue Quality Score (0-100, higher = healthier quality)
    # -------------------------
    quality_flag_health_map = {"healthy": 1.0, "watch": 0.7, "fragile": 0.35, "inactive": 0.15}
    score_df["quality_flag_health_factor"] = score_df["revenue_quality_flag"].map(quality_flag_health_map).fillna(0.5)

    revenue_quality_components = {
        "pricing_realization": clip01((score_df["realized_price_index"] - 0.72) / 0.30),
        "discount_discipline": 1 - clip01((score_df["trailing_3m_discount_avg"] - 0.12) / 0.25),
        "retention_momentum": 0.55 * (1 - clip01(score_df["contraction_frequency"] / 0.35))
        + 0.45 * clip01(score_df["expansion_frequency"] / 0.35),
        "account_health_quality": 0.40 * clip01((score_df["trailing_3m_usage_avg"] - 50) / 30)
        + 0.35 * clip01((score_df["trailing_3m_nps_avg"] + 10) / 45)
        + 0.25 * (1 - clip01(score_df["trailing_3m_payment_delay_avg"] / 30)),
        "stability_governance": 0.50 * (1 - score_df["renewal_risk_proxy"])
        + 0.30 * (1 - score_df["churn_history_flag"])
        + 0.20 * score_df["quality_flag_health_factor"],
    }
    revenue_quality_weights = {
        "pricing_realization": 0.30,
        "discount_discipline": 0.20,
        "retention_momentum": 0.20,
        "account_health_quality": 0.20,
        "stability_governance": 0.10,
    }
    score_df["revenue_quality_score"] = score_from_components(revenue_quality_components, revenue_quality_weights)

    # Inactive accounts should not show strong quality scores.
    score_df["revenue_quality_score"] = np.where(
        score_df["active_mrr"] <= 0,
        np.minimum(score_df["revenue_quality_score"], 25.0),
        score_df["revenue_quality_score"],
    )

    score_df["revenue_quality_risk_tier"] = score_df["revenue_quality_score"].apply(quality_to_risk_tier)

    # Risk contributions for quality score (gap-to-best).
    revenue_quality_risk_contrib = {
        key: revenue_quality_weights[key] * (1 - revenue_quality_components[key])
        for key in revenue_quality_components
    }
    for key, val in revenue_quality_risk_contrib.items():
        score_df[f"revenue_quality_risk_contrib_{key}"] = val

    revenue_driver_map = {
        "revenue_quality_risk_contrib_pricing_realization": "Weak realized pricing",
        "revenue_quality_risk_contrib_discount_discipline": "Poor discount discipline",
        "revenue_quality_risk_contrib_retention_momentum": "Weak retention momentum",
        "revenue_quality_risk_contrib_account_health_quality": "Poor account health quality",
        "revenue_quality_risk_contrib_stability_governance": "Stability/renewal governance risk",
    }
    revenue_keys = list(revenue_driver_map.keys())
    score_df["revenue_quality_main_driver"] = score_df[revenue_keys].apply(
        lambda r: argmax_driver(r, revenue_driver_map, revenue_keys),
        axis=1,
    )

    # -------------------------
    # 3) Discount Dependency Score (0-100, higher = more dependency risk)
    # -------------------------
    discount_components = {
        "discount_level": clip01((score_df["trailing_3m_discount_avg"] - 0.12) / 0.25),
        "discount_persistence": clip01(score_df["heavy_discount_frequency_12m"] / 0.70),
        "discounted_expansion_pressure": clip01(score_df["discounted_expansion_share_12m"] / 0.80),
        "price_realization_erosion": clip01((0.90 - score_df["realized_price_index"]) / 0.35),
        "policy_signal": np.maximum(score_df["discount_dependency_flag"], score_df["manager_discount_outlier_flag"]),
    }
    discount_weights = {
        "discount_level": 0.40,
        "discount_persistence": 0.25,
        "discounted_expansion_pressure": 0.15,
        "price_realization_erosion": 0.15,
        "policy_signal": 0.05,
    }
    score_df["discount_dependency_score"] = score_from_components(discount_components, discount_weights)
    score_df["discount_dependency_tier"] = score_df["discount_dependency_score"].apply(risk_tier)

    discount_contrib = component_contributions(discount_components, discount_weights)
    for key, val in discount_contrib.items():
        score_df[f"discount_contrib_{key}"] = val

    discount_driver_map = {
        "discount_contrib_discount_level": "High discount level",
        "discount_contrib_discount_persistence": "Persistent discounting",
        "discount_contrib_discounted_expansion_pressure": "Discount-driven expansion",
        "discount_contrib_price_realization_erosion": "Price realization erosion",
        "discount_contrib_policy_signal": "Policy governance signal",
    }
    discount_keys = list(discount_driver_map.keys())
    score_df["discount_dependency_main_driver"] = score_df[discount_keys].apply(
        lambda r: argmax_driver(r, discount_driver_map, discount_keys),
        axis=1,
    )

    # -------------------------
    # 4) Expansion Quality Score (0-100, higher = healthier expansion quality)
    # -------------------------
    expansion_components = {
        "healthy_expansion_mix": clip01(score_df["healthy_expansion_ratio_12m"] / 0.80),
        "fragility_control": 1 - clip01(score_df["fragile_expansion_ratio_12m"] / 0.80),
        "expansion_discount_discipline": 1 - clip01((score_df["avg_expansion_discount_12m"] - 0.12) / 0.28),
        "expansion_payment_quality": 1 - clip01((score_df["avg_expansion_payment_delay_12m"] - 8) / 25),
        "post_expansion_durability": 1 - clip01(score_df["post_expansion_contraction_rate_3m"] / 0.70),
    }
    expansion_weights = {
        "healthy_expansion_mix": 0.35,
        "fragility_control": 0.20,
        "expansion_discount_discipline": 0.20,
        "expansion_payment_quality": 0.10,
        "post_expansion_durability": 0.15,
    }
    score_df["expansion_quality_score"] = score_from_components(expansion_components, expansion_weights)

    # No recent expansion: assign a neutral baseline adjusted by general health, not a hard penalty.
    no_expansion_mask = score_df["expansion_events_12"] == 0
    neutral_expansion_score = 45 + 10 * (clip01((score_df["trailing_3m_usage_avg"] - 50) / 30) - 0.5) + 10 * (
        0.5 - clip01(score_df["contraction_frequency"] / 0.35)
    )
    score_df.loc[no_expansion_mask, "expansion_quality_score"] = np.clip(neutral_expansion_score[no_expansion_mask], 20, 60)

    score_df["expansion_quality_risk_tier"] = score_df["expansion_quality_score"].apply(quality_to_risk_tier)

    expansion_risk_contrib = {
        key: expansion_weights[key] * (1 - expansion_components[key])
        for key in expansion_components
    }
    for key, val in expansion_risk_contrib.items():
        score_df[f"expansion_risk_contrib_{key}"] = val

    expansion_driver_map = {
        "expansion_risk_contrib_healthy_expansion_mix": "Low healthy expansion mix",
        "expansion_risk_contrib_fragility_control": "High fragile expansion mix",
        "expansion_risk_contrib_expansion_discount_discipline": "Discount pressure in expansion",
        "expansion_risk_contrib_expansion_payment_quality": "Payment stress during expansion",
        "expansion_risk_contrib_post_expansion_durability": "Post-expansion contraction risk",
    }
    expansion_keys = list(expansion_driver_map.keys())

    score_df["expansion_quality_main_driver"] = score_df[expansion_keys].apply(
        lambda r: argmax_driver(r, expansion_driver_map, expansion_keys),
        axis=1,
    )
    score_df.loc[no_expansion_mask, "expansion_quality_main_driver"] = "No recent expansion signal"

    # -------------------------
    # 5) Governance Priority Score (0-100, higher = higher governance urgency)
    # -------------------------
    p99_mrr = float(score_df["current_mrr"].quantile(0.99))
    exposure_component = 0.70 * clip01(np.log1p(score_df["current_mrr"]) / np.log1p(max(p99_mrr, 1.0))) + 0.30 * clip01(
        score_df["concentration_weight"] / 0.01
    )

    governance_components = {
        "churn_risk": score_df["churn_risk_score"] / 100.0,
        "revenue_quality_risk": (100.0 - score_df["revenue_quality_score"]) / 100.0,
        "discount_dependency": score_df["discount_dependency_score"] / 100.0,
        "expansion_fragility": (100.0 - score_df["expansion_quality_score"]) / 100.0,
        "exposure_concentration": exposure_component,
        "renewal_urgency": clip01(score_df["renewal_due_flag"] * score_df["renewal_risk_proxy"] + score_df["renewal_due_flag"] * 0.15),
    }
    governance_weights = {
        "churn_risk": 0.32,
        "revenue_quality_risk": 0.18,
        "discount_dependency": 0.15,
        "expansion_fragility": 0.10,
        "exposure_concentration": 0.20,
        "renewal_urgency": 0.05,
    }

    score_df["governance_priority_score"] = score_from_components(governance_components, governance_weights)

    # Escalate a limited set of high-exposure/high-churn accounts.
    escalation_mask = (score_df["churn_risk_score"] >= 80) & (exposure_component >= 0.80)
    score_df.loc[escalation_mask, "governance_priority_score"] = np.clip(
        score_df.loc[escalation_mask, "governance_priority_score"] + 5,
        0,
        100,
    )

    score_df["governance_priority_tier"] = score_df["governance_priority_score"].apply(risk_tier)

    governance_contrib = component_contributions(governance_components, governance_weights)
    for key, val in governance_contrib.items():
        score_df[f"governance_contrib_{key}"] = val

    governance_driver_map = {
        "governance_contrib_churn_risk": "Churn risk pressure",
        "governance_contrib_revenue_quality_risk": "Revenue quality weakness",
        "governance_contrib_discount_dependency": "Discount dependency",
        "governance_contrib_expansion_fragility": "Expansion fragility",
        "governance_contrib_exposure_concentration": "Exposure concentration",
        "governance_contrib_renewal_urgency": "Renewal urgency",
    }
    governance_keys = list(governance_driver_map.keys())
    score_df["governance_main_driver"] = score_df[governance_keys].apply(
        lambda r: argmax_driver(r, governance_driver_map, governance_keys),
        axis=1,
    )

    # Recommended action.
    score_df["recommended_action"] = score_df.apply(assign_recommended_action, axis=1)

    score_df["recommended_action_reason"] = (
        score_df["governance_priority_tier"]
        + " priority driven by "
        + score_df["governance_main_driver"]
        + "; churn="
        + score_df["churn_risk_score"].round(1).astype(str)
        + ", quality="
        + score_df["revenue_quality_score"].round(1).astype(str)
        + ", discount="
        + score_df["discount_dependency_score"].round(1).astype(str)
    )

    # Component output for traceability.
    component_cols = [c for c in score_df.columns if c.startswith("churn_contrib_") or c.startswith("revenue_quality_risk_contrib_") or c.startswith("discount_contrib_") or c.startswith("expansion_risk_contrib_") or c.startswith("governance_contrib_")]

    components_table = score_df[["customer_id"] + component_cols].copy()

    # Primary score table.
    score_columns = [
        "customer_id",
        "segment",
        "region",
        "industry",
        "acquisition_channel",
        "account_manager_id",
        "current_mrr",
        "concentration_weight",
        "renewal_due_flag",
        "churn_risk_score",
        "churn_risk_tier",
        "churn_risk_main_driver",
        "revenue_quality_score",
        "revenue_quality_risk_tier",
        "revenue_quality_main_driver",
        "discount_dependency_score",
        "discount_dependency_tier",
        "discount_dependency_main_driver",
        "expansion_quality_score",
        "expansion_quality_risk_tier",
        "expansion_quality_main_driver",
        "governance_priority_score",
        "governance_priority_tier",
        "governance_main_driver",
        "recommended_action",
        "recommended_action_reason",
    ]
    score_output = score_df[score_columns].copy().sort_values(
        ["governance_priority_score", "current_mrr"],
        ascending=[False, False],
    )

    # Highest-priority shortlist.
    shortlist = score_output[score_output["current_mrr"] > 0].head(30).copy()
    shortlist = shortlist.merge(
        score_df[["customer_id", "forward_risk_flags_list"]],
        on="customer_id",
        how="left",
    )
    shortlist["forward_risk_flags"] = shortlist["forward_risk_flags_list"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
    shortlist = shortlist.drop(columns=["forward_risk_flags_list"])

    # Sensitivity analysis for governance priority weighting.
    def governance_score_variant(weights: Dict[str, float]) -> pd.Series:
        return (
            100
            * (
                weights["churn_risk"] * governance_components["churn_risk"]
                + weights["revenue_quality_risk"] * governance_components["revenue_quality_risk"]
                + weights["discount_dependency"] * governance_components["discount_dependency"]
                + weights["expansion_fragility"] * governance_components["expansion_fragility"]
                + weights["exposure_concentration"] * governance_components["exposure_concentration"]
                + weights["renewal_urgency"] * governance_components["renewal_urgency"]
            )
        )

    return score_output, components_table, shortlist


def write_scoring_docs(base_dir: Path) -> None:
    methodology_path = base_dir / "docs/core/scoring_model_design.md"

    methodology = """# Scoring System Design (RevOps / Finance / CS Operating Model)

## Design Principles
- Transparent and explainable weighted scoring (no black-box ML).
- Common 0-100 scale for all scores.
- Risk tiers standardized as `Low / Moderate / High / Critical`.
- Each account receives a main risk driver and a recommended action.

## Score Definitions
1. `churn_risk_score` (higher = greater churn risk)
- Components: usage deterioration, sentiment/support, payment stress, contraction pattern, discount pressure, renewal exposure, history/tenure.
- Weighting: 25%, 15%, 20%, 15%, 10%, 10%, 5%.

2. `revenue_quality_score` (higher = healthier recurring revenue quality)
- Components: realized pricing, discount discipline, retention momentum, account health quality, stability/governance.
- Weighting: 30%, 20%, 20%, 20%, 10%.
- Tiering uses inverse-risk interpretation (`100 - score`) to keep common risk labels.

3. `discount_dependency_score` (higher = more dependency risk)
- Components: discount level, persistence, discount-led expansion share, realization erosion, governance policy signal.
- Weighting: 40%, 25%, 15%, 15%, 5%.

4. `expansion_quality_score` (higher = healthier expansion quality)
- Components: healthy expansion mix, fragile expansion control, expansion discount discipline, expansion payment quality, post-expansion durability.
- Weighting: 35%, 20%, 20%, 10%, 15%.
- Accounts with no recent expansion receive a neutral baseline score adjusted by health/contraction context.

5. `governance_priority_score` (higher = more urgent leadership attention)
- Components: churn risk, revenue quality risk, discount dependency, expansion fragility, exposure concentration, renewal urgency.
- Weighting: 32%, 18%, 15%, 10%, 20%, 5%.
- High-exposure and very high churn-risk accounts receive a limited escalation uplift.

## Weighting Rationale
- Churn and revenue quality are weighted highest because they directly govern recurring revenue durability.
- Discount and expansion dimensions are separate to prevent strong expansion volume from masking fragile expansion quality.
- Exposure concentration is explicitly included so score prioritization reflects downside materiality, not just risk probability.
- Renewal urgency remains explicit but lower-weight because it is often a timing amplifier rather than a root cause.

## Action Mapping Logic
- `Low`: monitor only.
- High-risk/high-exposure: reduce exposure concentration.
- High churn-health risk: escalate to customer success or investigate deterioration.
- Renewal-critical: prepare renewal intervention or reprice at renewal.
- High discount dependency: review discount policy (and manager behavior where governance outlier exists).

## Trade-offs
- Rule-based design favors explainability over maximum predictive fit.
- Threshold choices (for example, heavy discount >=25%) are policy choices and should be recalibrated when business context changes.
- A common 0-100 scale improves comparability but compresses nuance; component tables should always accompany score usage.

"""
    methodology_path.write_text(methodology)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build interpretable commercial scoring system outputs.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--output-dir", type=str, default="data/processed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    output_dir = (base_dir / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    score_output, components_table, shortlist = build_scores(base_dir)

    score_output.to_csv(output_dir / "account_scoring_model_output.csv", index=False)
    components_table.to_csv(output_dir / "account_scoring_components.csv", index=False)
    shortlist.to_csv(output_dir / "scoring_priority_shortlist.csv", index=False)
    write_scoring_docs(base_dir)

    print("Scoring system build complete.")
    print(f"account_scoring_model_output: {len(score_output):,} rows")
    print(f"account_scoring_components: {len(components_table):,} rows")
    print(f"scoring_priority_shortlist: {len(shortlist):,} rows")


if __name__ == "__main__":
    main()

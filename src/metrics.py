"""Canonical recurring-revenue retention metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_retention_panel(
    monthly_quality: pd.DataFrame,
    monthly_metrics: pd.DataFrame,
) -> pd.DataFrame:
    """Return an account-month panel with beginning-base retention fields.

    New logos acquired after the first observed month are excluded from the
    GRR/NRR and logo-churn denominators. Beginning MRR is reconstructed before
    the current month's expansion and contraction movements.
    """
    required_quality = {
        "customer_id",
        "month",
        "active_mrr",
        "expansion_mrr",
        "contraction_mrr",
    }
    required_metrics = {"customer_id", "month", "active_flag", "churn_flag"}
    missing_quality = required_quality - set(monthly_quality.columns)
    missing_metrics = required_metrics - set(monthly_metrics.columns)
    if missing_quality or missing_metrics:
        raise ValueError(
            "Retention inputs are missing required columns: "
            f"monthly_quality={sorted(missing_quality)}, "
            f"monthly_metrics={sorted(missing_metrics)}"
        )

    panel = monthly_quality.merge(
        monthly_metrics[["customer_id", "month", "active_flag", "churn_flag"]],
        on=["customer_id", "month"],
        how="left",
        validate="one_to_one",
    ).sort_values(["customer_id", "month"])

    if panel[["active_flag", "churn_flag"]].isna().any().any():
        raise ValueError("Retention inputs do not share the same customer-month key universe")

    active = panel["active_flag"].eq(1)
    first_active_month = panel.loc[active].groupby("customer_id")["month"].min().rename("first_active_month")
    panel = panel.merge(first_active_month, on="customer_id", how="left", validate="many_to_one")

    first_observed_month = panel["month"].min()
    panel["is_new_logo"] = (
        active & panel["month"].eq(panel["first_active_month"]) & panel["month"].gt(first_observed_month)
    )
    panel["retention_eligible"] = active & ~panel["is_new_logo"]

    reconstructed_start = (
        panel["active_mrr"].fillna(0.0) - panel["expansion_mrr"].fillna(0.0) + panel["contraction_mrr"].fillna(0.0)
    ).clip(lower=0.0)
    panel["starting_mrr"] = np.where(panel["retention_eligible"], reconstructed_start, 0.0)
    panel["retention_expansion_mrr"] = np.where(
        panel["retention_eligible"],
        panel["expansion_mrr"].fillna(0.0),
        0.0,
    )
    panel["retention_contraction_mrr"] = np.where(
        panel["retention_eligible"],
        panel["contraction_mrr"].fillna(0.0),
        0.0,
    )
    panel["eligible_churn_flag"] = (panel["retention_eligible"] & panel["churn_flag"].eq(1)).astype(int)
    panel["churn_mrr"] = np.where(
        panel["eligible_churn_flag"].eq(1),
        panel["active_mrr"].fillna(0.0),
        0.0,
    )
    return panel


def build_monthly_retention(
    monthly_quality: pd.DataFrame,
    monthly_metrics: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate standard logo churn, revenue churn, GRR, and NRR by month."""
    panel = build_retention_panel(monthly_quality, monthly_metrics)
    monthly = (
        panel.groupby("month", as_index=False)
        .agg(
            mrr=("active_mrr", "sum"),
            active_logos=("active_flag", "sum"),
            starting_mrr=("starting_mrr", "sum"),
            starting_logos=("retention_eligible", "sum"),
            new_logos=("is_new_logo", "sum"),
            expansion_mrr=("retention_expansion_mrr", "sum"),
            contraction_mrr=("retention_contraction_mrr", "sum"),
            churn_mrr=("churn_mrr", "sum"),
            churn_events=("eligible_churn_flag", "sum"),
        )
        .sort_values("month")
        .reset_index(drop=True)
    )

    valid_mrr = monthly["starting_mrr"] > 0
    valid_logos = monthly["starting_logos"] > 0
    monthly["logo_churn_rate"] = np.where(
        valid_logos,
        monthly["churn_events"] / monthly["starting_logos"],
        np.nan,
    )
    monthly["revenue_churn_rate"] = np.where(
        valid_mrr,
        monthly["churn_mrr"] / monthly["starting_mrr"],
        np.nan,
    )
    monthly["grr"] = np.where(
        valid_mrr,
        (monthly["starting_mrr"] - monthly["contraction_mrr"] - monthly["churn_mrr"]) / monthly["starting_mrr"],
        np.nan,
    )
    monthly["nrr"] = np.where(
        valid_mrr,
        (monthly["starting_mrr"] + monthly["expansion_mrr"] - monthly["contraction_mrr"] - monthly["churn_mrr"])
        / monthly["starting_mrr"],
        np.nan,
    )
    return monthly

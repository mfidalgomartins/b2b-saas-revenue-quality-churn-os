from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


RAW_FILES = {
    "customers": "customers.csv",
    "plans": "plans.csv",
    "subscriptions": "subscriptions.csv",
    "monthly_account_metrics": "monthly_account_metrics.csv",
    "invoices": "invoices.csv",
    "account_managers": "account_managers.csv",
}


def load_raw_tables(raw_dir: Path) -> Dict[str, pd.DataFrame]:
    tables = {
        "customers": pd.read_csv(raw_dir / RAW_FILES["customers"], parse_dates=["signup_date"]),
        "plans": pd.read_csv(raw_dir / RAW_FILES["plans"]),
        "subscriptions": pd.read_csv(
            raw_dir / RAW_FILES["subscriptions"],
            parse_dates=["subscription_start_date", "subscription_end_date"],
        ),
        "monthly_account_metrics": pd.read_csv(
            raw_dir / RAW_FILES["monthly_account_metrics"],
            parse_dates=["month"],
        ),
        "invoices": pd.read_csv(raw_dir / RAW_FILES["invoices"], parse_dates=["invoice_month"]),
        "account_managers": pd.read_csv(raw_dir / RAW_FILES["account_managers"]),
    }
    return tables


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return np.where(denominator > 0, numerator / denominator, 0.0)


def _compute_trend(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    x = np.arange(len(values))
    y = np.array(values, dtype=float)
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


def build_account_monthly_revenue_quality(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    customers = tables["customers"]
    plans = tables["plans"]
    subs = tables["subscriptions"].copy()
    monthly = tables["monthly_account_metrics"].copy()
    invoices = tables["invoices"].copy()

    subs = subs.rename(columns={"subscription_start_date": "month"})
    invoices = invoices.rename(columns={"invoice_month": "month"})

    base = monthly.merge(
        subs[
            [
                "customer_id",
                "month",
                "plan_id",
                "status",
                "contracted_mrr",
                "realized_mrr",
                "discount_pct",
                "renewal_flag",
            ]
        ],
        on=["customer_id", "month"],
        how="left",
    )

    base = base.merge(
        invoices[["customer_id", "month", "billed_mrr", "realized_mrr", "discount_amount", "days_to_pay"]],
        on=["customer_id", "month"],
        how="left",
        suffixes=("", "_invoice"),
    )

    base = base.merge(customers[["customer_id", "segment", "region", "signup_date"]], on="customer_id", how="left")
    base = base.merge(plans[["plan_id", "plan_tier", "billing_cycle", "list_mrr", "included_seats"]], on="plan_id", how="left")

    base["active_mrr"] = np.where(base["active_flag"] == 1, base["contracted_mrr"].fillna(0.0), 0.0)

    realized_mrr_effective = base["realized_mrr_invoice"].fillna(base["realized_mrr"])
    base["realized_price_index"] = _safe_div(realized_mrr_effective.fillna(0.0), base["active_mrr"].fillna(0.0))
    base["realized_price_index"] = np.clip(base["realized_price_index"], 0.0, 1.2)

    discount_from_invoice = _safe_div(base["discount_amount"].fillna(0.0), base["billed_mrr"].fillna(0.0))
    base["avg_discount_pct"] = np.where(
        base["active_flag"] == 1,
        np.where(base["billed_mrr"].fillna(0.0) > 0, discount_from_invoice, base["discount_pct"].fillna(0.0)),
        0.0,
    )

    base = base.sort_values(["customer_id", "month"]).reset_index(drop=True)
    base["net_mrr_change"] = base.groupby("customer_id")["active_mrr"].diff().fillna(base["active_mrr"])

    base["trailing_3m_discount_avg"] = (
        base.groupby("customer_id")["avg_discount_pct"]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    base["discount_dependency_flag"] = np.where(
        (base["trailing_3m_discount_avg"] >= 0.25)
        | ((base["avg_discount_pct"] >= 0.30) & (base["expansion_mrr"] > 0)),
        1,
        0,
    )

    usage_risk = np.clip((55 - base["product_usage_score"].fillna(0)) / 55, 0, 1)
    nps_risk = np.clip((10 - base["nps_score"].fillna(0)) / 110, 0, 1)
    delay_risk = np.clip(base["payment_delay_days"].fillna(0) / 60, 0, 1)
    support_risk = np.clip((base["support_tickets"].fillna(0) - 4) / 20, 0, 1)
    discount_risk = np.clip((base["avg_discount_pct"] - 0.18) / 0.35, 0, 1)

    base["renewal_risk_proxy"] = np.clip(
        0.35 * base["renewal_due_flag"].fillna(0)
        + 0.20 * usage_risk
        + 0.15 * nps_risk
        + 0.15 * delay_risk
        + 0.10 * support_risk
        + 0.05 * discount_risk,
        0,
        1,
    )

    conditions = [
        base["active_flag"] == 0,
        (base["discount_dependency_flag"] == 1)
        | (base["realized_price_index"] < 0.72)
        | (base["renewal_risk_proxy"] >= 0.60),
        (base["expansion_mrr"] > base["contraction_mrr"])
        & (base["avg_discount_pct"] <= 0.20)
        & (base["realized_price_index"] >= 0.85)
        & (base["product_usage_score"].fillna(0) >= 60)
        & (base["nps_score"].fillna(0) >= 20),
    ]
    choices = ["inactive", "fragile", "healthy"]
    base["revenue_quality_flag"] = np.select(conditions, choices, default="watch")

    out = base[
        [
            "customer_id",
            "month",
            "active_mrr",
            "realized_price_index",
            "avg_discount_pct",
            "expansion_mrr",
            "contraction_mrr",
            "net_mrr_change",
            "discount_dependency_flag",
            "revenue_quality_flag",
            "renewal_risk_proxy",
        ]
    ].copy()

    return out


def build_customer_health_features(
    tables: Dict[str, pd.DataFrame],
    account_monthly_revenue_quality: pd.DataFrame,
) -> pd.DataFrame:
    customers = tables["customers"].copy()
    monthly = tables["monthly_account_metrics"].copy()

    current_month = monthly["month"].max()
    trailing_12_start = current_month - pd.DateOffset(months=11)

    merged = monthly.merge(
        account_monthly_revenue_quality[
            ["customer_id", "month", "active_mrr", "avg_discount_pct", "discount_dependency_flag", "renewal_risk_proxy"]
        ],
        on=["customer_id", "month"],
        how="left",
    )

    rows = []
    for cid, g in merged.sort_values("month").groupby("customer_id"):
        g_active = g[g["active_flag"] == 1]
        g_active_to_current = g_active[g_active["month"] <= current_month]

        last_3_active = g_active_to_current.tail(3)
        last_12_active = g_active_to_current[g_active_to_current["month"] >= trailing_12_start]

        usage_vals = last_3_active["product_usage_score"].dropna().tolist()
        support_vals = last_3_active["support_tickets"].dropna().tolist()
        nps_vals = last_3_active["nps_score"].dropna().tolist()
        delay_vals = last_3_active["payment_delay_days"].dropna().tolist()
        discount_vals = last_3_active["avg_discount_pct"].dropna().tolist()

        trailing_3m_usage_avg = float(np.mean(usage_vals)) if usage_vals else 0.0
        trailing_3m_usage_trend = _compute_trend(usage_vals)
        trailing_3m_support_ticket_avg = float(np.mean(support_vals)) if support_vals else 0.0
        trailing_3m_nps_avg = float(np.mean(nps_vals)) if nps_vals else 0.0
        trailing_3m_payment_delay_avg = float(np.mean(delay_vals)) if delay_vals else 0.0
        trailing_3m_discount_avg = float(np.mean(discount_vals)) if discount_vals else 0.0

        if len(last_3_active) >= 2:
            seat_start = float(last_3_active.iloc[0]["seats_active"])
            seat_end = float(last_3_active.iloc[-1]["seats_active"])
            seat_growth_rate = float((seat_end - seat_start) / max(seat_start, 1.0))
        else:
            seat_growth_rate = 0.0

        expansion_frequency = float((last_12_active["expansion_mrr"] > 0).mean()) if len(last_12_active) > 0 else 0.0
        contraction_frequency = float((last_12_active["contraction_mrr"] > 0).mean()) if len(last_12_active) > 0 else 0.0
        churn_history_flag = int((g["churn_flag"] == 1).any())

        current_row = g[g["month"] == current_month]
        if len(current_row) > 0:
            current_mrr = float(current_row.iloc[0]["active_mrr"])
            renewal_due_flag = int(current_row.iloc[0]["renewal_due_flag"])
        else:
            current_mrr = 0.0
            renewal_due_flag = 0

        signup_date = customers.loc[customers["customer_id"] == cid, "signup_date"].iloc[0]
        tenure_months = int((current_month.year - signup_date.year) * 12 + (current_month.month - signup_date.month) + 1)
        tenure_months = max(tenure_months, 0)

        rows.append(
            {
                "customer_id": cid,
                "current_mrr": round(current_mrr, 2),
                "trailing_3m_usage_avg": round(trailing_3m_usage_avg, 3),
                "trailing_3m_usage_trend": round(trailing_3m_usage_trend, 3),
                "trailing_3m_support_ticket_avg": round(trailing_3m_support_ticket_avg, 3),
                "trailing_3m_nps_avg": round(trailing_3m_nps_avg, 3),
                "trailing_3m_payment_delay_avg": round(trailing_3m_payment_delay_avg, 3),
                "trailing_3m_discount_avg": round(trailing_3m_discount_avg, 4),
                "seat_growth_rate": round(seat_growth_rate, 4),
                "expansion_frequency": round(expansion_frequency, 4),
                "contraction_frequency": round(contraction_frequency, 4),
                "churn_history_flag": churn_history_flag,
                "renewal_due_flag": renewal_due_flag,
                "tenure_months": tenure_months,
            }
        )

    out = pd.DataFrame(rows)
    total_current_mrr = out["current_mrr"].sum()
    out["concentration_weight"] = np.where(total_current_mrr > 0, out["current_mrr"] / total_current_mrr, 0.0)
    out["concentration_weight"] = out["concentration_weight"].round(6)

    ordered_cols = [
        "customer_id",
        "current_mrr",
        "trailing_3m_usage_avg",
        "trailing_3m_usage_trend",
        "trailing_3m_support_ticket_avg",
        "trailing_3m_nps_avg",
        "trailing_3m_payment_delay_avg",
        "trailing_3m_discount_avg",
        "seat_growth_rate",
        "expansion_frequency",
        "contraction_frequency",
        "churn_history_flag",
        "renewal_due_flag",
        "concentration_weight",
        "tenure_months",
    ]
    return out[ordered_cols]


def build_cohort_retention_summary(
    tables: Dict[str, pd.DataFrame],
    account_monthly_revenue_quality: pd.DataFrame,
) -> pd.DataFrame:
    customers = tables["customers"][ ["customer_id", "segment", "region"] ].copy()
    subs = tables["subscriptions"].copy()

    first_month = (
        subs.groupby("customer_id", as_index=False)["subscription_start_date"].min()
        .rename(columns={"subscription_start_date": "cohort_month"})
    )

    first_revenue = account_monthly_revenue_quality.merge(
        first_month,
        left_on=["customer_id", "month"],
        right_on=["customer_id", "cohort_month"],
        how="inner",
    )[["customer_id", "active_mrr"]].rename(columns={"active_mrr": "cohort_mrr"})

    panel = account_monthly_revenue_quality.merge(first_month, on="customer_id", how="left")
    panel = panel.merge(first_revenue, on="customer_id", how="left")
    panel = panel.merge(customers, on="customer_id", how="left")

    panel["month_number"] = (
        (panel["month"].dt.year - panel["cohort_month"].dt.year) * 12
        + (panel["month"].dt.month - panel["cohort_month"].dt.month)
    )
    panel = panel[panel["month_number"] >= 0].copy()

    panel["retained_base_revenue"] = np.minimum(panel["active_mrr"], panel["cohort_mrr"].fillna(0.0))

    grouped = panel.groupby(["cohort_month", "segment", "region", "month_number"], as_index=False).agg(
        active_customers=("active_mrr", lambda s: int((s > 0).sum())),
        retained_revenue=("active_mrr", "sum"),
        retained_base_revenue=("retained_base_revenue", "sum"),
        cohort_revenue=("cohort_mrr", "sum"),
    )

    grouped["gross_retention_rate"] = np.where(
        grouped["cohort_revenue"] > 0,
        grouped["retained_base_revenue"] / grouped["cohort_revenue"],
        0.0,
    )
    grouped["net_retention_rate"] = np.where(
        grouped["cohort_revenue"] > 0,
        grouped["retained_revenue"] / grouped["cohort_revenue"],
        0.0,
    )

    out = grouped[
        [
            "cohort_month",
            "segment",
            "region",
            "month_number",
            "active_customers",
            "retained_revenue",
            "gross_retention_rate",
            "net_retention_rate",
        ]
    ].sort_values(["cohort_month", "segment", "region", "month_number"])

    return out


def build_account_risk_base(
    tables: Dict[str, pd.DataFrame],
    account_monthly_revenue_quality: pd.DataFrame,
    customer_health_features: pd.DataFrame,
) -> pd.DataFrame:
    monthly = tables["monthly_account_metrics"].copy()
    current_month = monthly["month"].max()

    latest_quality = account_monthly_revenue_quality[
        account_monthly_revenue_quality["month"] == current_month
    ][
        [
            "customer_id",
            "active_mrr",
            "realized_price_index",
            "avg_discount_pct",
            "discount_dependency_flag",
            "revenue_quality_flag",
            "renewal_risk_proxy",
        ]
    ].copy()

    risk = customer_health_features.merge(latest_quality, on="customer_id", how="left")
    risk["active_mrr"] = risk["active_mrr"].fillna(0.0)
    risk["realized_price_index"] = risk["realized_price_index"].fillna(0.0)
    risk["avg_discount_pct"] = risk["avg_discount_pct"].fillna(risk["trailing_3m_discount_avg"])
    risk["discount_dependency_flag"] = risk["discount_dependency_flag"].fillna(0).astype(int)
    risk["revenue_quality_flag"] = risk["revenue_quality_flag"].fillna("inactive")
    risk["renewal_risk_proxy"] = risk["renewal_risk_proxy"].fillna(0.0)

    usage_risk = np.clip((55 - risk["trailing_3m_usage_avg"]) / 55, 0, 1)
    nps_risk = np.clip((10 - risk["trailing_3m_nps_avg"]) / 110, 0, 1)
    delay_risk = np.clip(risk["trailing_3m_payment_delay_avg"] / 60, 0, 1)
    discount_risk = np.clip((risk["trailing_3m_discount_avg"] - 0.18) / 0.35, 0, 1)
    contraction_risk = np.clip(risk["contraction_frequency"] / 0.5, 0, 1)

    risk["account_risk_score"] = 100 * np.clip(
        0.25 * usage_risk
        + 0.20 * nps_risk
        + 0.20 * delay_risk
        + 0.15 * discount_risk
        + 0.10 * contraction_risk
        + 0.10 * (risk["renewal_due_flag"] * risk["renewal_risk_proxy"]),
        0,
        1,
    )

    records = []
    for _, row in risk.iterrows():
        churn_inputs = {
            "trailing_3m_usage_trend": float(row["trailing_3m_usage_trend"]),
            "trailing_3m_nps_avg": float(row["trailing_3m_nps_avg"]),
            "trailing_3m_support_ticket_avg": float(row["trailing_3m_support_ticket_avg"]),
            "trailing_3m_payment_delay_avg": float(row["trailing_3m_payment_delay_avg"]),
            "contraction_frequency": float(row["contraction_frequency"]),
            "renewal_due_flag": int(row["renewal_due_flag"]),
        }

        revenue_inputs = {
            "current_mrr": float(row["current_mrr"]),
            "realized_price_index": float(row["realized_price_index"]),
            "avg_discount_pct": float(row["avg_discount_pct"]),
            "discount_dependency_flag": int(row["discount_dependency_flag"]),
            "revenue_quality_flag": str(row["revenue_quality_flag"]),
        }

        fragility_inputs = {
            "seat_growth_rate": float(row["seat_growth_rate"]),
            "expansion_frequency": float(row["expansion_frequency"]),
            "contraction_frequency": float(row["contraction_frequency"]),
            "concentration_weight": float(row["concentration_weight"]),
            "churn_history_flag": int(row["churn_history_flag"]),
            "renewal_risk_proxy": float(row["renewal_risk_proxy"]),
        }

        flags: List[str] = []
        if row["trailing_3m_usage_trend"] < -2.5:
            flags.append("usage_declining")
        if row["trailing_3m_nps_avg"] < 10:
            flags.append("low_nps")
        if row["trailing_3m_payment_delay_avg"] > 20:
            flags.append("payment_delay_stress")
        if row["trailing_3m_discount_avg"] >= 0.25 or row["discount_dependency_flag"] == 1:
            flags.append("discount_dependency")
        if row["contraction_frequency"] > 0.25:
            flags.append("frequent_contraction")
        if row["renewal_due_flag"] == 1 and row["renewal_risk_proxy"] >= 0.60:
            flags.append("renewal_at_risk")
        if row["concentration_weight"] > 0.01:
            flags.append("high_concentration_exposure")
        if row["account_risk_score"] >= 70:
            flags.append("high_risk_score")

        records.append(
            {
                "customer_id": row["customer_id"],
                "current_month": current_month,
                "churn_risk_inputs": json.dumps(churn_inputs, sort_keys=True),
                "revenue_quality_inputs": json.dumps(revenue_inputs, sort_keys=True),
                "account_fragility_inputs": json.dumps(fragility_inputs, sort_keys=True),
                "forward_risk_flags": json.dumps(flags),
            }
        )

    out = pd.DataFrame(records)
    return out


def build_account_manager_summary(
    tables: Dict[str, pd.DataFrame],
    customer_health_features: pd.DataFrame,
    account_monthly_revenue_quality: pd.DataFrame,
) -> pd.DataFrame:
    customers = tables["customers"][["customer_id", "account_manager_id"]].copy()
    monthly = tables["monthly_account_metrics"].copy()

    current_month = monthly["month"].max()
    trailing_12_start = current_month - pd.DateOffset(months=11)

    chf = customer_health_features.merge(customers, on="customer_id", how="left")

    quality_window = account_monthly_revenue_quality[
        (account_monthly_revenue_quality["month"] >= trailing_12_start)
        & (account_monthly_revenue_quality["month"] <= current_month)
    ].copy()

    mm_window = monthly[(monthly["month"] >= trailing_12_start) & (monthly["month"] <= current_month)].copy()
    mm_window = mm_window.merge(customers, on="customer_id", how="left")

    start_month = trailing_12_start
    start_base = mm_window[mm_window["month"] == start_month]
    end_base = mm_window[mm_window["month"] == current_month]

    start_active = start_base[start_base["active_flag"] == 1][["account_manager_id", "customer_id"]]
    end_active = end_base[end_base["active_flag"] == 1][["account_manager_id", "customer_id"]]

    retained = start_active.merge(
        end_active,
        on=["account_manager_id", "customer_id"],
        how="inner",
    )

    churn_events = mm_window[mm_window["churn_flag"] == 1][["account_manager_id", "customer_id"]].drop_duplicates()

    expansion_rate_df = quality_window.merge(customers, on="customer_id", how="left").groupby("account_manager_id", as_index=False).agg(
        expansion_mrr_sum=("expansion_mrr", "sum"),
        base_mrr_sum=("active_mrr", "sum"),
    )
    expansion_rate_df["expansion_rate"] = np.where(
        expansion_rate_df["base_mrr_sum"] > 0,
        expansion_rate_df["expansion_mrr_sum"] / expansion_rate_df["base_mrr_sum"],
        0.0,
    )

    manager_rows = []
    for am_id, g in chf.groupby("account_manager_id"):
        portfolio_mrr = float(g["current_mrr"].sum())

        weighted_discount = 0.0
        mrr_weight_sum = float(g["current_mrr"].sum())
        if mrr_weight_sum > 0:
            weighted_discount = float((g["trailing_3m_discount_avg"] * g["current_mrr"]).sum() / mrr_weight_sum)
        else:
            weighted_discount = float(g["trailing_3m_discount_avg"].mean()) if len(g) > 0 else 0.0

        start_count = int(start_active[start_active["account_manager_id"] == am_id]["customer_id"].nunique())
        retained_count = int(retained[retained["account_manager_id"] == am_id]["customer_id"].nunique())
        churn_count = int(churn_events[churn_events["account_manager_id"] == am_id]["customer_id"].nunique())

        retention_rate = float(retained_count / start_count) if start_count > 0 else 0.0
        churn_rate = float(churn_count / start_count) if start_count > 0 else 0.0

        expansion_rate = float(
            expansion_rate_df.loc[expansion_rate_df["account_manager_id"] == am_id, "expansion_rate"].iloc[0]
        ) if (expansion_rate_df["account_manager_id"] == am_id).any() else 0.0

        usage_risk = np.clip((55 - g["trailing_3m_usage_avg"]) / 55, 0, 1)
        nps_risk = np.clip((10 - g["trailing_3m_nps_avg"]) / 110, 0, 1)
        delay_risk = np.clip(g["trailing_3m_payment_delay_avg"] / 60, 0, 1)
        discount_risk = np.clip((g["trailing_3m_discount_avg"] - 0.18) / 0.35, 0, 1)
        cust_risk = 100 * np.clip(0.35 * usage_risk + 0.25 * nps_risk + 0.25 * delay_risk + 0.15 * discount_risk, 0, 1)

        if mrr_weight_sum > 0:
            risk_weighted_portfolio_score = float((cust_risk * g["current_mrr"]).sum() / mrr_weight_sum)
        else:
            risk_weighted_portfolio_score = float(cust_risk.mean()) if len(g) > 0 else 0.0

        manager_rows.append(
            {
                "account_manager_id": am_id,
                "portfolio_mrr": round(portfolio_mrr, 2),
                "avg_discount": round(weighted_discount, 4),
                "retention_rate": round(retention_rate, 4),
                "churn_rate": round(churn_rate, 4),
                "expansion_rate": round(expansion_rate, 4),
                "risk_weighted_portfolio_score": round(risk_weighted_portfolio_score, 3),
            }
        )

    out = pd.DataFrame(manager_rows).sort_values("portfolio_mrr", ascending=False)
    return out


def write_feature_dictionary(output_path: Path) -> None:
    dictionary_text = """# Analytical Layer Feature Dictionary

## Table: account_monthly_revenue_quality
- `customer_id`: Account identifier. Source: `customers.customer_id`.
- `month`: Month grain (first day of month). Source: `monthly_account_metrics.month`.
- `active_mrr`: Contracted MRR in active months, else 0. Source: `subscriptions.contracted_mrr` + `monthly_account_metrics.active_flag`.
- `realized_price_index`: `realized_mrr / active_mrr`, clipped to [0, 1.2]. Source: `invoices.realized_mrr` fallback `subscriptions.realized_mrr`.
- `avg_discount_pct`: Commercial invoice discount ratio (`discount_amount / billed_mrr`) fallback `subscriptions.discount_pct`.
- `expansion_mrr`: Monthly expansion amount. Source: `monthly_account_metrics.expansion_mrr`.
- `contraction_mrr`: Monthly contraction amount. Source: `monthly_account_metrics.contraction_mrr`.
- `net_mrr_change`: Month-over-month delta in `active_mrr` by customer.
- `discount_dependency_flag`: 1 if trailing 3M discount >= 25% or high-discount expansion month.
- `revenue_quality_flag`: `healthy`, `watch`, `fragile`, `inactive` rule-based classification.
- `renewal_risk_proxy`: 0-1 composite proxy combining renewal due, usage/NPS, payment delay, support burden, and discount pressure.

Assumptions/caveats:
- Price index mixes pricing and collections effects (`collection_loss_amount` affects realized MRR).
- Discount dependency threshold (25%) is policy-driven and should be tuned.
- `revenue_quality_flag` is a diagnostic rule, not a causal label.

## Table: customer_health_features
- `customer_id`: Account identifier.
- `current_mrr`: MRR at latest calendar month; 0 if inactive.
- `trailing_3m_usage_avg`: Average product usage over last 3 active months up to current month.
- `trailing_3m_usage_trend`: Linear slope of usage over last 3 active months.
- `trailing_3m_support_ticket_avg`: Average support tickets over last 3 active months.
- `trailing_3m_nps_avg`: Average NPS over last 3 active months.
- `trailing_3m_payment_delay_avg`: Average payment delay over last 3 active months.
- `trailing_3m_discount_avg`: Average effective discount over last 3 active months.
- `seat_growth_rate`: Relative seat change from earliest to latest point in last 3 active months.
- `expansion_frequency`: Share of active months with expansion in trailing 12 months.
- `contraction_frequency`: Share of active months with contraction in trailing 12 months.
- `churn_history_flag`: 1 if account has ever churned historically.
- `renewal_due_flag`: Renewal due at current month from operational panel.
- `concentration_weight`: `current_mrr / total_current_mrr`.
- `tenure_months`: Months from signup to current month.

Assumptions/caveats:
- Trailing features use last active observations (not strictly contiguous calendar months for churned accounts).
- Concentration weight is sensitive to current snapshot timing.

## Table: cohort_retention_summary
- `cohort_month`: First subscription month.
- `segment`, `region`: Cohort dimensions from customer master.
- `month_number`: Months since cohort start (0-indexed).
- `active_customers`: Count of customers with `active_mrr > 0` in that cohort-month.
- `retained_revenue`: Sum of active MRR in cohort-month.
- `gross_retention_rate`: `sum(min(active_mrr_t, cohort_mrr)) / sum(cohort_mrr)`.
- `net_retention_rate`: `sum(active_mrr_t) / sum(cohort_mrr)`.

Assumptions/caveats:
- Cohort baseline revenue uses first active month MRR.
- GRR formulation caps retained revenue at baseline per customer.

## Table: account_risk_base
- `customer_id`: Account identifier.
- `current_month`: Snapshot month used for risk inputs.
- `churn_risk_inputs`: JSON payload of leading churn inputs.
- `revenue_quality_inputs`: JSON payload of monetization/quality inputs.
- `account_fragility_inputs`: JSON payload of fragility and exposure inputs.
- `forward_risk_flags`: JSON list of triggered operational risk flags.

Assumptions/caveats:
- Flags are rule-based heuristics for triage, not model outputs.
- JSON payloads improve traceability but require parsing in BI tools.

## Table: account_manager_summary
- `account_manager_id`: Owner identifier.
- `portfolio_mrr`: Sum of current MRR across owned accounts.
- `avg_discount`: Current-MRR-weighted average trailing discount.
- `retention_rate`: 12M logo retention from starting active base to current month.
- `churn_rate`: 12M churned logos / starting active logos.
- `expansion_rate`: Trailing 12M expansion MRR / trailing 12M base MRR.
- `risk_weighted_portfolio_score`: Current-MRR-weighted average account risk score (higher = riskier portfolio).

Assumptions/caveats:
- Manager assignment is treated as static over time.
- Rate metrics depend on a 12M window anchored to latest month.
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(dictionary_text)


def write_table_purpose_note(output_path: Path) -> None:
    note = """# Analytical Layer Purpose and Design Notes

## Why each analytical table exists
- `account_monthly_revenue_quality`: monthly revenue quality lens at account grain for trend diagnostics, retention decomposition, and dashboard time-series.
- `customer_health_features`: latest account feature vector for churn/risk scoring and account prioritization.
- `cohort_retention_summary`: standardized GRR/NRR cohort tracking by segment and region.
- `account_risk_base`: scoring-ready payload with auditable inputs and forward risk flags.
- `account_manager_summary`: portfolio governance layer for frontline ownership performance and risk concentration.

## Leakage controls applied
- Trailing feature windows use data up to snapshot month only.
- Cohort metrics are computed from month-index progression without future-period backfill.
- Risk inputs are assembled from current/trailing states only, with no forward outcome labels embedded.

## Reproducibility
- One deterministic script builds all outputs from `data/raw` and writes to `data/processed`.
- All rule thresholds are explicit in code and can be versioned.

## Traceability
- Engineered fields are direct transforms from raw columns with documented formulas in the feature dictionary.
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(note)


def save_tables(processed_dir: Path, tables: Dict[str, pd.DataFrame]) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(processed_dir / f"{name}.csv", index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analytical layer tables for revenue quality and churn analytics.")
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--processed-dir", type=str, default="data/processed")
    parser.add_argument("--feature-dictionary-path", type=str, default="docs/core/feature_dictionary.md")
    parser.add_argument("--notes-path", type=str, default="docs/core/analytical_layer_notes.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)

    tables = load_raw_tables(raw_dir)

    account_monthly_revenue_quality = build_account_monthly_revenue_quality(tables)
    customer_health_features = build_customer_health_features(tables, account_monthly_revenue_quality)
    cohort_retention_summary = build_cohort_retention_summary(tables, account_monthly_revenue_quality)
    account_risk_base = build_account_risk_base(tables, account_monthly_revenue_quality, customer_health_features)
    account_manager_summary = build_account_manager_summary(tables, customer_health_features, account_monthly_revenue_quality)

    out_tables = {
        "account_monthly_revenue_quality": account_monthly_revenue_quality,
        "customer_health_features": customer_health_features,
        "cohort_retention_summary": cohort_retention_summary,
        "account_risk_base": account_risk_base,
        "account_manager_summary": account_manager_summary,
    }
    save_tables(processed_dir, out_tables)

    write_feature_dictionary(Path(args.feature_dictionary_path))
    write_table_purpose_note(Path(args.notes_path))

    print("Analytical layer build complete.")
    for name, df in out_tables.items():
        print(f"{name}: {len(df):,} rows x {len(df.columns)} cols")


if __name__ == "__main__":
    main()

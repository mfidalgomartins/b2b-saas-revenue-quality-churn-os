"""Schema contracts enforced at every CSV load boundary.

The pipeline reads from CSVs at several stages. A renamed or dropped upstream
column would otherwise propagate silently as NaN through downstream joins.
`validate_schema` fails loudly when the columns we depend on are missing.
"""

from __future__ import annotations

import pandas as pd

REQUIRED_RAW_SCHEMAS: dict[str, frozenset[str]] = {
    "customers": frozenset(
        {
            "customer_id",
            "signup_date",
            "region",
            "segment",
            "company_size",
            "industry",
            "acquisition_channel",
            "account_manager_id",
            "lifecycle_stage",
        }
    ),
    "plans": frozenset(
        {
            "plan_id",
            "plan_name",
            "plan_tier",
            "billing_cycle",
            "list_mrr",
            "included_seats",
        }
    ),
    "subscriptions": frozenset(
        {
            "subscription_id",
            "customer_id",
            "plan_id",
            "subscription_start_date",
            "subscription_end_date",
            "status",
            "seats_purchased",
            "contracted_mrr",
            "realized_mrr",
            "discount_pct",
            "renewal_flag",
        }
    ),
    "monthly_account_metrics": frozenset(
        {
            "customer_id",
            "month",
            "active_flag",
            "seats_active",
            "product_usage_score",
            "support_tickets",
            "nps_score",
            "payment_delay_days",
            "expansion_mrr",
            "contraction_mrr",
            "churn_flag",
            "downgrade_flag",
            "renewal_due_flag",
        }
    ),
    "invoices": frozenset(
        {
            "invoice_id",
            "customer_id",
            "invoice_month",
            "billed_mrr",
            "realized_mrr",
            "discount_amount",
            "collection_loss_amount",
            "effective_revenue_adjustment_amount",
            "payment_status",
            "days_to_pay",
        }
    ),
    "account_managers": frozenset(
        {
            "account_manager_id",
            "team",
            "region",
            "tenure_months",
        }
    ),
}

REQUIRED_PROCESSED_SCHEMAS: dict[str, frozenset[str]] = {
    "account_monthly_revenue_quality": frozenset(
        {
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
        }
    ),
    "customer_health_features": frozenset(
        {
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
        }
    ),
    "account_scoring_model_output": frozenset(
        {
            "customer_id",
            "current_mrr",
            "churn_risk_score",
            "churn_risk_tier",
            "revenue_quality_score",
            "discount_dependency_score",
            "expansion_quality_score",
            "governance_priority_score",
            "governance_priority_tier",
            "recommended_action",
        }
    ),
}


def validate_schema(df: pd.DataFrame, name: str, required: frozenset[str]) -> pd.DataFrame:
    """Raise if any required column is missing. Returns df unchanged for chaining."""
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Schema contract violated for '{name}': missing columns {sorted(missing)}. Found: {sorted(df.columns)}"
        )
    return df

# Analytical Layer Feature Dictionary

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

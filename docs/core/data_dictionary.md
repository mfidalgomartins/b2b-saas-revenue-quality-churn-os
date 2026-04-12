# Data Dictionary

## Raw Tables

### 1) `customers` (grain: one row per customer)
Key: `customer_id`

| Column | Type | Definition |
|---|---|---|
| customer_id | string | Unique account identifier |
| signup_date | date | Initial signup date |
| region | string | Geographic region |
| segment | string | Commercial segment |
| company_size | string | Company-size bucket |
| industry | string | Industry vertical |
| acquisition_channel | string | Primary acquisition source |
| account_manager_id | string | Assigned account manager |
| lifecycle_stage | string | Lifecycle state |

### 2) `plans` (grain: one row per plan)
Key: `plan_id`

| Column | Type | Definition |
|---|---|---|
| plan_id | string | Unique plan identifier |
| plan_name | string | Plan name |
| plan_tier | string | Tier (Basic/Growth/Pro/Enterprise) |
| billing_cycle | string | Monthly or annual |
| list_mrr | float | List-price MRR |
| included_seats | int | Included seats |

### 3) `subscriptions` (grain: one row per customer-month subscription snapshot)
Key: `subscription_id`

| Column | Type | Definition |
|---|---|---|
| subscription_id | string | Unique subscription snapshot ID |
| customer_id | string | Account identifier |
| plan_id | string | Plan identifier |
| subscription_start_date | date | Snapshot month start |
| subscription_end_date | date/null | End date if churned |
| status | string | Active/churned |
| seats_purchased | int | Purchased seat count |
| contracted_mrr | float | Contracted monthly revenue |
| realized_mrr | float | Realized monthly revenue |
| discount_pct | float | Discount percent |
| renewal_flag | int | Renewal indicator |

### 4) `monthly_account_metrics` (grain: one row per customer-month)
Key: (`customer_id`, `month`)

| Column | Type | Definition |
|---|---|---|
| customer_id | string | Account identifier |
| month | date | Calendar month |
| active_flag | int | Active account flag |
| seats_active | int | Active seats |
| product_usage_score | float | Product usage score |
| support_tickets | int | Ticket count |
| nps_score | float | NPS-like sentiment score |
| payment_delay_days | float | Average payment delay |
| expansion_mrr | float | Expansion MRR in month |
| contraction_mrr | float | Contraction MRR in month |
| churn_flag | int | Churn event flag |
| downgrade_flag | int | Downgrade flag |
| renewal_due_flag | int | Renewal due in month |

### 5) `invoices` (grain: one row per customer-month invoice)
Key: `invoice_id`

| Column | Type | Definition |
|---|---|---|
| invoice_id | string | Invoice identifier |
| customer_id | string | Account identifier |
| invoice_month | date | Invoice month |
| billed_mrr | float | Billed MRR |
| realized_mrr | float | Realized MRR |
| discount_amount | float | Commercial discount amount only |
| collection_loss_amount | float | Collection/default loss adjustment amount |
| effective_revenue_adjustment_amount | float | `discount_amount + collection_loss_amount` (capped at billed) |
| payment_status | string | Payment status |
| days_to_pay | float | Days to payment |

### 6) `account_managers` (grain: one row per manager)
Key: `account_manager_id`

| Column | Type | Definition |
|---|---|---|
| account_manager_id | string | Manager identifier |
| team | string | Team assignment |
| region | string | Manager region |
| tenure_months | int | Tenure in months |

## Processed Tables

### 1) `account_monthly_revenue_quality` (grain: customer-month)
Key: (`customer_id`, `month`)

| Column | Type | Definition |
|---|---|---|
| customer_id | string | Account identifier |
| month | date | Month |
| active_mrr | float | Active MRR |
| realized_price_index | float | Realized/active MRR ratio |
| avg_discount_pct | float | Effective discount percent |
| expansion_mrr | float | Expansion MRR |
| contraction_mrr | float | Contraction MRR |
| net_mrr_change | float | MoM active MRR change |
| discount_dependency_flag | int | Discount dependence indicator |
| revenue_quality_flag | string | Healthy/watch/fragile/inactive |
| renewal_risk_proxy | float | Composite renewal risk proxy |

### 2) `customer_health_features` (grain: customer snapshot)
Key: `customer_id`

| Column | Type | Definition |
|---|---|---|
| customer_id | string | Account identifier |
| current_mrr | float | Latest-month MRR |
| trailing_3m_usage_avg | float | 3M average usage |
| trailing_3m_usage_trend | float | 3M usage trend slope |
| trailing_3m_support_ticket_avg | float | 3M avg support tickets |
| trailing_3m_nps_avg | float | 3M avg NPS |
| trailing_3m_payment_delay_avg | float | 3M avg payment delay |
| trailing_3m_discount_avg | float | 3M avg discount |
| seat_growth_rate | float | Seat growth over trailing window |
| expansion_frequency | float | 12M expansion frequency |
| contraction_frequency | float | 12M contraction frequency |
| churn_history_flag | int | Any historical churn |
| renewal_due_flag | int | Renewal due in latest month |
| concentration_weight | float | MRR share weight |
| tenure_months | int | Account tenure |

### 3) `cohort_retention_summary` (grain: cohort-month by segment-region)
Key: (`cohort_month`, `segment`, `region`, `month_number`)

| Column | Type | Definition |
|---|---|---|
| cohort_month | date | Cohort start month |
| segment | string | Segment |
| region | string | Region |
| month_number | int | Months since cohort start |
| active_customers | int | Active customers in cohort-month |
| retained_revenue | float | Retained revenue in cohort-month |
| gross_retention_rate | float | Gross retention rate |
| net_retention_rate | float | Net retention rate |

### 4) `account_risk_base` (grain: customer snapshot)
Key: `customer_id`

| Column | Type | Definition |
|---|---|---|
| customer_id | string | Account identifier |
| current_month | date | Snapshot month |
| churn_risk_inputs | json-string | Churn-related inputs |
| revenue_quality_inputs | json-string | Revenue quality inputs |
| account_fragility_inputs | json-string | Fragility/exposure inputs |
| forward_risk_flags | json-string | Forward risk flags list |

### 5) `account_manager_summary` (grain: manager snapshot)
Key: `account_manager_id`

| Column | Type | Definition |
|---|---|---|
| account_manager_id | string | Manager identifier |
| portfolio_mrr | float | Portfolio MRR |
| avg_discount | float | Weighted discount |
| retention_rate | float | Retention rate |
| churn_rate | float | Churn rate |
| expansion_rate | float | Expansion rate |
| risk_weighted_portfolio_score | float | Portfolio risk score |

### 6) `account_scoring_model_output` (grain: customer snapshot)
Key: `customer_id`

Contains scoring outputs and action fields:
- churn risk score/tier/driver
- revenue quality score/tier/driver
- discount dependency score/tier/driver
- expansion quality score/tier/driver
- governance priority score/tier/driver
- recommended action and reason

### 7) `account_scoring_components` (grain: customer snapshot)
Key: `customer_id`

Contains weighted component contributions for score explainability.

### 8) Forecast outputs
- `baseline_mrr_forecast` (grain: forecast month)
- `risk_adjusted_mrr_forecast` (grain: forecast month)
- `scenario_mrr_trajectories` (grain: scenario-month)
- `mrr_scenario_table` (grain: scenario summary)
- `commercial_risk_impact_estimates` (grain: impact metric)

## Key Metric Definitions
- `MRR`: sum of active monthly recurring revenue.
- `ARR`: annualized run-rate (`12 * MRR`).
- `Logo churn rate`: churn events divided by active account rows.
- `Revenue churn rate`: churned MRR divided by active MRR.
- `GRR`: `(starting_mrr - contraction_mrr - churn_mrr) / starting_mrr`.
- `NRR`: `(starting_mrr + expansion_mrr - contraction_mrr - churn_mrr) / starting_mrr`.
- `Realized price index`: realized MRR divided by active MRR.
- `Discount-dependent share`: discounted-dependent MRR divided by total MRR.

## Grain Summary
- Entity-level master: `customers`, `plans`, `account_managers`
- Event/panel-level raw: `subscriptions` (customer-month snapshot), `monthly_account_metrics`, `invoices`
- Analytical panel: `account_monthly_revenue_quality` (customer-month)
- Analytical snapshots: `customer_health_features`, `account_risk_base`, `account_scoring_model_output`
- Aggregations: `cohort_retention_summary`, `account_manager_summary`, forecast summary tables

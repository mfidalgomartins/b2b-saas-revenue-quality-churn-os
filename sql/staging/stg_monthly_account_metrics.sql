-- Staging model: monthly account metrics
-- Enforces customer-month panel typing.

select
    customer_id,
    cast(month as date) as month,
    cast(active_flag as integer) as active_flag,
    cast(seats_active as integer) as seats_active,
    cast(product_usage_score as double) as product_usage_score,
    cast(support_tickets as integer) as support_tickets,
    cast(nps_score as double) as nps_score,
    cast(payment_delay_days as double) as payment_delay_days,
    cast(expansion_mrr as double) as expansion_mrr,
    cast(contraction_mrr as double) as contraction_mrr,
    cast(churn_flag as integer) as churn_flag,
    cast(downgrade_flag as integer) as downgrade_flag,
    cast(renewal_due_flag as integer) as renewal_due_flag
from monthly_account_metrics;

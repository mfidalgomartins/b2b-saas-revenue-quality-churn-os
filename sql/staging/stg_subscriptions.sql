-- Staging model: subscriptions
-- Normalizes grain and core revenue fields for downstream marts.

select
    subscription_id,
    customer_id,
    plan_id,
    cast(subscription_start_date as date) as month,
    cast(subscription_end_date as date) as subscription_end_date,
    lower(status) as status,
    cast(seats_purchased as integer) as seats_purchased,
    cast(contracted_mrr as double) as contracted_mrr,
    cast(realized_mrr as double) as realized_mrr,
    cast(discount_pct as double) as discount_pct,
    cast(renewal_flag as integer) as renewal_flag
from subscriptions;

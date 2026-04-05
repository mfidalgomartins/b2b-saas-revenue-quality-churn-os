-- Mart model: account monthly revenue quality
-- Reference SQL equivalent of the Python feature-engineering logic at customer-month grain.

with monthly as (
    select * from stg_monthly_account_metrics
),
subs as (
    select
        customer_id,
        month,
        contracted_mrr,
        realized_mrr as realized_mrr_subscription,
        discount_pct,
        renewal_flag
    from stg_subscriptions
),
inv as (
    select
        customer_id,
        month,
        billed_mrr,
        realized_mrr as realized_mrr_invoice,
        commercial_discount_amount,
        effective_revenue_adjustment_amount
    from stg_invoices
),
base as (
    select
        m.customer_id,
        m.month,
        m.active_flag,
        m.expansion_mrr,
        m.contraction_mrr,
        m.product_usage_score,
        m.nps_score,
        m.support_tickets,
        m.payment_delay_days,
        m.renewal_due_flag,
        coalesce(s.contracted_mrr, 0.0) as contracted_mrr,
        coalesce(i.billed_mrr, 0.0) as billed_mrr,
        coalesce(i.realized_mrr_invoice, s.realized_mrr_subscription, 0.0) as realized_mrr_effective,
        coalesce(i.commercial_discount_amount, 0.0) as commercial_discount_amount,
        coalesce(s.discount_pct, 0.0) as discount_pct
    from monthly m
    left join subs s
        on m.customer_id = s.customer_id
       and m.month = s.month
    left join inv i
        on m.customer_id = i.customer_id
       and m.month = i.month
),
enriched as (
    select
        customer_id,
        month,
        case when active_flag = 1 then contracted_mrr else 0.0 end as active_mrr,
        case
            when active_flag = 1 and contracted_mrr > 0 then least(greatest(realized_mrr_effective / contracted_mrr, 0.0), 1.2)
            else 0.0
        end as realized_price_index,
        case
            when active_flag = 1 and billed_mrr > 0 then commercial_discount_amount / billed_mrr
            when active_flag = 1 then discount_pct
            else 0.0
        end as avg_discount_pct,
        expansion_mrr,
        contraction_mrr,
        (case when active_flag = 1 then contracted_mrr else 0.0 end)
            - lag((case when active_flag = 1 then contracted_mrr else 0.0 end), 1, 0.0) over (
                partition by customer_id order by month
            ) as net_mrr_change
    from base
)
select *
from enriched;

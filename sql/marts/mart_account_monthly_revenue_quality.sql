-- mart_account_monthly_revenue_quality
-- Reference SQL implementation of the Python feature layer at customer-month grain.
-- Mirrors src/features/build_analytical_layer.py:build_account_monthly_revenue_quality.

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
joined as (
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
      on m.customer_id = s.customer_id and m.month = s.month
    left join inv i
      on m.customer_id = i.customer_id and m.month = i.month
),
base as (
    select
        customer_id,
        month,
        active_flag,
        expansion_mrr,
        contraction_mrr,
        product_usage_score,
        nps_score,
        support_tickets,
        payment_delay_days,
        renewal_due_flag,
        case when active_flag = 1 then contracted_mrr else 0.0 end as active_mrr,
        case
            when active_flag = 1 and contracted_mrr > 0
                then least(greatest(realized_mrr_effective / contracted_mrr, 0.0), 1.2)
            else 0.0
        end as realized_price_index,
        case
            when active_flag = 1 and billed_mrr > 0
                then commercial_discount_amount / billed_mrr
            when active_flag = 1 then discount_pct
            else 0.0
        end as avg_discount_pct
    from joined
),
windowed as (
    select
        *,
        active_mrr - lag(active_mrr, 1, 0.0) over (
            partition by customer_id order by month
        ) as net_mrr_change,
        avg(avg_discount_pct) over (
            partition by customer_id order by month
            rows between 2 preceding and current row
        ) as trailing_3m_discount_avg
    from base
)
select
    customer_id,
    month,
    active_mrr,
    realized_price_index,
    avg_discount_pct,
    expansion_mrr,
    contraction_mrr,
    net_mrr_change,

    -- Discount-dependency flag mirrors the Python rule:
    -- trailing 3M effective discount >= 25%, OR
    -- a >=30% expansion month (expansion + heavy discount in the same month).
    case
        when trailing_3m_discount_avg >= 0.25 then 1
        when avg_discount_pct >= 0.30 and expansion_mrr > 0 then 1
        else 0
    end as discount_dependency_flag,

    -- Renewal-risk proxy mirrors the weighted blend in the Python layer:
    -- 35% renewal-due, 20% usage risk, 15% NPS risk, 15% delay risk,
    -- 10% support risk, 5% discount risk. All inputs clipped to [0,1].
    least(greatest(
        0.35 * coalesce(renewal_due_flag, 0)
        + 0.20 * least(greatest((55 - coalesce(product_usage_score, 0)) / 55, 0.0), 1.0)
        + 0.15 * least(greatest((10 - coalesce(nps_score, 0)) / 110, 0.0), 1.0)
        + 0.15 * least(greatest(coalesce(payment_delay_days, 0) / 60.0, 0.0), 1.0)
        + 0.10 * least(greatest((coalesce(support_tickets, 0) - 4) / 20.0, 0.0), 1.0)
        + 0.05 * least(greatest((avg_discount_pct - 0.18) / 0.35, 0.0), 1.0),
        0.0
    ), 1.0) as renewal_risk_proxy,

    -- revenue_quality_flag mirrors the Python np.select rule order:
    -- inactive < fragile < healthy < watch (default).
    case
        when active_flag = 0 then 'inactive'
        when (case when trailing_3m_discount_avg >= 0.25 then 1
                   when avg_discount_pct >= 0.30 and expansion_mrr > 0 then 1
                   else 0 end) = 1
             or realized_price_index < 0.72
             or (
                 0.35 * coalesce(renewal_due_flag, 0)
                 + 0.20 * least(greatest((55 - coalesce(product_usage_score, 0)) / 55, 0.0), 1.0)
                 + 0.15 * least(greatest((10 - coalesce(nps_score, 0)) / 110, 0.0), 1.0)
                 + 0.15 * least(greatest(coalesce(payment_delay_days, 0) / 60.0, 0.0), 1.0)
                 + 0.10 * least(greatest((coalesce(support_tickets, 0) - 4) / 20.0, 0.0), 1.0)
                 + 0.05 * least(greatest((avg_discount_pct - 0.18) / 0.35, 0.0), 1.0)
             ) >= 0.60 then 'fragile'
        when expansion_mrr > contraction_mrr
             and avg_discount_pct <= 0.20
             and realized_price_index >= 0.85
             and coalesce(product_usage_score, 0) >= 60
             and coalesce(nps_score, 0) >= 20 then 'healthy'
        else 'watch'
    end as revenue_quality_flag
from windowed;

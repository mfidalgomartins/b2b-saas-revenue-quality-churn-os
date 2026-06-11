-- Mart model: monthly retention KPIs.
-- Uses a beginning-of-month base and excludes new logos from retention.

with base as (
    select
        q.month,
        q.customer_id,
        q.active_mrr,
        q.expansion_mrr,
        q.contraction_mrr,
        m.active_flag,
        m.churn_flag,
        min(case when m.active_flag = 1 then q.month end) over (
            partition by q.customer_id
        ) as first_active_month,
        min(q.month) over () as first_observed_month
    from mart_account_monthly_revenue_quality q
    join stg_monthly_account_metrics m
      on q.customer_id = m.customer_id
     and q.month = m.month
),
eligible as (
    select
        *,
        case
            when active_flag = 1
             and not (month = first_active_month and month > first_observed_month)
            then 1 else 0
        end as retention_eligible
    from base
),
rollup as (
    select
        month,
        sum(case when retention_eligible = 1
            then greatest(active_mrr - expansion_mrr + contraction_mrr, 0.0)
            else 0.0 end) as starting_mrr,
        sum(case when retention_eligible = 1 then expansion_mrr else 0.0 end) as expansion_mrr,
        sum(case when retention_eligible = 1 then contraction_mrr else 0.0 end) as contraction_mrr,
        sum(case when retention_eligible = 1 and churn_flag = 1 then active_mrr else 0.0 end) as churn_mrr,
        sum(retention_eligible) as starting_logos,
        sum(case when retention_eligible = 1 and churn_flag = 1 then 1 else 0 end) as churned_logos
    from eligible
    group by 1
)
select
    month,
    starting_mrr,
    expansion_mrr,
    contraction_mrr,
    churn_mrr,
    starting_logos,
    churned_logos,
    case
        when starting_logos > 0 then churned_logos * 1.0 / starting_logos
        else null
    end as logo_churn_rate,
    case
        when starting_mrr > 0 then churn_mrr / starting_mrr
        else null
    end as revenue_churn_rate,
    case
        when starting_mrr > 0 then (starting_mrr - contraction_mrr - churn_mrr) / starting_mrr
        else null
    end as gross_revenue_retention,
    case
        when starting_mrr > 0 then (starting_mrr + expansion_mrr - contraction_mrr - churn_mrr) / starting_mrr
        else null
    end as net_revenue_retention
from rollup
order by month;

-- Mart model: monthly retention KPIs
-- Produces portfolio-level GRR and NRR from account-month revenue-quality panel.

with active as (
    select
        q.month,
        q.customer_id,
        q.active_mrr,
        q.expansion_mrr,
        q.contraction_mrr,
        m.churn_flag
    from mart_account_monthly_revenue_quality q
    join stg_monthly_account_metrics m
      on q.customer_id = m.customer_id
     and q.month = m.month
    where m.active_flag = 1
),
rollup as (
    select
        month,
        sum(active_mrr) as starting_mrr,
        sum(expansion_mrr) as expansion_mrr,
        sum(contraction_mrr) as contraction_mrr,
        sum(case when churn_flag = 1 then active_mrr else 0.0 end) as churn_mrr
    from active
    group by 1
)
select
    month,
    starting_mrr,
    expansion_mrr,
    contraction_mrr,
    churn_mrr,
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

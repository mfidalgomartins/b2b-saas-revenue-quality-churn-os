-- mart_account_scoring
-- Warehouse score composition from the weighted component mart.
-- Mirrors score_from_components, policy overrides and tier thresholds in
-- src/scoring/build_scoring_system.py.

with component_scores as (
    select
        customer_id,
        current_mrr,
        trailing_3m_usage_avg,
        contraction_frequency,
        expansion_events_12,
        round_even(100.0 * (
            churn_contrib_usage_deterioration
            + churn_contrib_sentiment_support
            + churn_contrib_payment_stress
            + churn_contrib_commercial_contraction
            + churn_contrib_discount_pressure
            + churn_contrib_renewal_exposure
            + churn_contrib_history_tenure
        ), 3) as churn_risk_score,
        round_even(100.0 - 100.0 * (
            revenue_quality_risk_contrib_pricing_realization
            + revenue_quality_risk_contrib_discount_discipline
            + revenue_quality_risk_contrib_retention_momentum
            + revenue_quality_risk_contrib_account_health_quality
            + revenue_quality_risk_contrib_stability_governance
        ), 3) as revenue_quality_score_raw,
        round_even(100.0 * (
            discount_contrib_discount_level
            + discount_contrib_discount_persistence
            + discount_contrib_discounted_expansion_pressure
            + discount_contrib_price_realization_erosion
            + discount_contrib_policy_signal
        ), 3) as discount_dependency_score,
        round_even(100.0 - 100.0 * (
            expansion_risk_contrib_healthy_expansion_mix
            + expansion_risk_contrib_fragility_control
            + expansion_risk_contrib_expansion_discount_discipline
            + expansion_risk_contrib_expansion_payment_quality
            + expansion_risk_contrib_post_expansion_durability
        ), 3) as expansion_quality_score_raw,
        round_even(100.0 * (
            governance_contrib_churn_risk
            + governance_contrib_revenue_quality_risk
            + governance_contrib_discount_dependency
            + governance_contrib_expansion_fragility
            + governance_contrib_exposure_concentration
            + governance_contrib_renewal_urgency
        ), 3) as governance_priority_score_base,
        governance_contrib_exposure_concentration
    from account_scoring_components
),
policy_adjusted as (
    select
        *,
        case
            when current_mrr <= 0 then least(revenue_quality_score_raw, 25.0)
            else revenue_quality_score_raw
        end as revenue_quality_score,
        case
            when expansion_events_12 = 0 then least(greatest(
                45.0
                + 10.0 * (least(greatest((trailing_3m_usage_avg - 50.0) / 30.0, 0.0), 1.0) - 0.5)
                + 10.0 * (0.5 - least(greatest(contraction_frequency / 0.35, 0.0), 1.0)),
                20.0
            ), 60.0)
            else expansion_quality_score_raw
        end as expansion_quality_score,
        case
            when churn_risk_score >= 80.0
             and governance_contrib_exposure_concentration >= 0.16
                then least(governance_priority_score_base + 5.0, 100.0)
            else governance_priority_score_base
        end as governance_priority_score
    from component_scores
)
select
    customer_id,
    churn_risk_score,
    case
        when churn_risk_score < 30.0 then 'Low'
        when churn_risk_score < 55.0 then 'Moderate'
        when churn_risk_score < 75.0 then 'High'
        else 'Critical'
    end as churn_risk_tier,
    revenue_quality_score,
    case
        when 100.0 - revenue_quality_score < 30.0 then 'Low'
        when 100.0 - revenue_quality_score < 55.0 then 'Moderate'
        when 100.0 - revenue_quality_score < 75.0 then 'High'
        else 'Critical'
    end as revenue_quality_risk_tier,
    discount_dependency_score,
    case
        when discount_dependency_score < 30.0 then 'Low'
        when discount_dependency_score < 55.0 then 'Moderate'
        when discount_dependency_score < 75.0 then 'High'
        else 'Critical'
    end as discount_dependency_tier,
    expansion_quality_score,
    case
        when 100.0 - expansion_quality_score < 30.0 then 'Low'
        when 100.0 - expansion_quality_score < 55.0 then 'Moderate'
        when 100.0 - expansion_quality_score < 75.0 then 'High'
        else 'Critical'
    end as expansion_quality_risk_tier,
    governance_priority_score,
    case
        when governance_priority_score < 30.0 then 'Low'
        when governance_priority_score < 55.0 then 'Moderate'
        when governance_priority_score < 75.0 then 'High'
        else 'Critical'
    end as governance_priority_tier
from policy_adjusted;

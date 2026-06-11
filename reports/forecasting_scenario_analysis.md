# Forecasting and Scenario Analysis Memo

## Objective
Provide near-term, decision-useful commercial intelligence for MRR trajectory and downside exposure.

## Modeling Style
- Interpretable monthly rate-based model.
- Baseline rates derived from recency-weighted averages of the last 6 observed months.
- Forecast horizon: 6 months forward from 2026-02-01.
- No black-box machine learning; assumptions are explicit and scenario-adjustable.

## Baseline MRR Forecast
- Starting MRR: $9,523,590
- Baseline forecast end-MRR (6m): $10,647,476
- Baseline MRR growth over horizon: 11.8%

Baseline assumptions (monthly rates):
- Expansion rate: 0.68%
- Contraction rate: 0.30%
- Churn rate: 0.62%
- Net-new rate (residual): 2.12%

## Risk-Adjusted Forecast
- Risk-adjusted end-MRR: $10,277,564
- Difference vs base case: $-369,912 MRR

Risk-adjusted assumptions incorporate:
- Higher churn/contraction from high-risk concentration.
- Lower expansion and net-new rates due to fragility drag.

Risk-adjusted rates (monthly):
- Expansion rate: 0.68%
- Contraction rate: 0.51%
- Churn rate: 0.93%
- Net-new rate: 2.04%

## Scenario Comparison
- Base case (reference): end-MRR $10,647,476
- Downside / fragile-growth: end-MRR $9,923,687 (-723,790 vs base)
- Improvement / healthy-growth: end-MRR $11,022,495 (375,019 vs base)
- Discount-discipline improvement: end-MRR $10,647,732 (255 vs base)

Interpretation:
- The fragile-growth downside quantifies sensitivity to churn/contraction concentration.
- The healthy-growth improvement quantifies value from retention and expansion-quality execution.
- Discount-discipline improvement may slightly moderate short-term expansion but improves realized ARR quality.

## Business Impact Estimates
- ARR at risk: $4,514,312
- Expected contraction exposure (6m): $124,165 MRR
- Concentration-adjusted downside (6m): $100,198 MRR
- Stress test: top-20 high-risk full churn impact: $3,672,017 ARR
- Stress test: top-20 high-risk 20% contraction impact: $734,403 ARR
- Improvement scenario uplift vs base: $4,500,227 ARR

## Assumptions by Scenario
- Base case: continuation of recent rate regime.
- Downside case: churn +50%, contraction +35%, expansion -20%, net-new -30%.
- Improvement case: churn -20%, contraction -15%, expansion +15%, net-new +15%.
- Discount-discipline improvement: churn -12%, contraction -10%, expansion -6%, net-new -3%, realized price index +2pts.

## Caveats
- This is an operating forecast, not a statistical confidence-interval model.
- Net-new rate is a residual term and can absorb unobserved commercial drivers.
- Scenario outputs are assumption-sensitive and should be reviewed monthly.
- Use this layer for decision support and prioritization, not single-point budgeting certainty.

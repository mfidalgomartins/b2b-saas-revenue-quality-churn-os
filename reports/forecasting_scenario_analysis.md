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
- Baseline forecast end-MRR (6m): $10,184,529
- Baseline MRR growth over horizon: 6.9%

Baseline assumptions (monthly rates):
- Expansion rate: 0.67%
- Contraction rate: 0.30%
- Churn rate: 0.66%
- Net-new rate (residual): 1.41%

## Risk-Adjusted Forecast
- Risk-adjusted end-MRR: $9,825,796
- Difference vs base case: $-358,733 MRR

Risk-adjusted assumptions incorporate:
- Higher churn/contraction from high-risk concentration.
- Lower expansion and net-new rates due to fragility drag.

Risk-adjusted rates (monthly):
- Expansion rate: 0.67%
- Contraction rate: 0.51%
- Churn rate: 0.97%
- Net-new rate: 1.33%

## Scenario Comparison
- Base case (reference): end-MRR $10,184,529
- Downside / fragile-growth: end-MRR $9,598,067 (-586,462 vs base)
- Improvement / healthy-growth: end-MRR $10,484,501 (299,972 vs base)
- Discount-discipline improvement: end-MRR $10,200,634 (16,105 vs base)

Interpretation:
- The fragile-growth downside quantifies sensitivity to churn/contraction concentration.
- The healthy-growth improvement quantifies value from retention and expansion-quality execution.
- Discount-discipline improvement may slightly moderate short-term expansion but improves realized ARR quality.

## Business Impact Estimates
- ARR at risk: $4,567,961
- Expected contraction exposure (6m): $124,165 MRR
- Concentration-adjusted downside (6m): $100,608 MRR
- Stress test: top-20 high-risk full churn impact: $3,679,462 ARR
- Stress test: top-20 high-risk 20% contraction impact: $735,892 ARR
- Retention improvement opportunity (improvement vs base): $3,599,668 ARR

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

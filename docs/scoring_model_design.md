# Scoring System Design (RevOps / Finance / CS Operating Model)

## Design Principles
- Transparent and explainable weighted scoring (no black-box ML).
- Common 0-100 scale for all scores.
- Risk tiers standardized as `Low / Moderate / High / Critical`.
- Each account receives a main risk driver and a recommended action.

## Score Definitions
1. `churn_risk_score` (higher = greater churn risk)
- Components: usage deterioration, sentiment/support, payment stress, contraction pattern, discount pressure, renewal exposure, history/tenure.
- Weighting: 25%, 15%, 20%, 15%, 10%, 10%, 5%.

2. `revenue_quality_score` (higher = healthier recurring revenue quality)
- Components: realized pricing, discount discipline, retention momentum, account health quality, stability/governance.
- Weighting: 30%, 20%, 20%, 20%, 10%.
- Tiering uses inverse-risk interpretation (`100 - score`) to keep common risk labels.

3. `discount_dependency_score` (higher = more dependency risk)
- Components: discount level, persistence, discount-led expansion share, realization erosion, governance policy signal.
- Weighting: 40%, 25%, 15%, 15%, 5%.

4. `expansion_quality_score` (higher = healthier expansion quality)
- Components: healthy expansion mix, fragile expansion control, expansion discount discipline, expansion payment quality, post-expansion durability.
- Weighting: 35%, 20%, 20%, 10%, 15%.
- Accounts with no recent expansion receive a neutral baseline score adjusted by health/contraction context.

5. `governance_priority_score` (higher = more urgent leadership attention)
- Components: churn risk, revenue quality risk, discount dependency, expansion fragility, exposure concentration, renewal urgency.
- Weighting: 32%, 18%, 15%, 10%, 20%, 5%.
- High-exposure and very high churn-risk accounts receive a limited escalation uplift.

## Weighting Rationale
- Churn and revenue quality are weighted highest because they directly govern recurring revenue durability.
- Discount and expansion dimensions are separate to prevent strong expansion volume from masking fragile expansion quality.
- Exposure concentration is explicitly included so score prioritization reflects downside materiality, not just risk probability.
- Renewal urgency remains explicit but lower-weight because it is often a timing amplifier rather than a root cause.

## Action Mapping Logic
- `Low`: monitor only.
- High-risk/high-exposure: reduce exposure concentration.
- High churn-health risk: escalate to customer success or investigate deterioration.
- Renewal-critical: prepare renewal intervention or reprice at renewal.
- High discount dependency: review discount policy (and manager behavior where governance outlier exists).

## Trade-offs
- Rule-based design favors explainability over maximum predictive fit.
- Threshold choices (for example, heavy discount >=25%) are policy choices and should be recalibrated when business context changes.
- A common 0-100 scale improves comparability but compresses nuance; component tables should always accompany score usage.

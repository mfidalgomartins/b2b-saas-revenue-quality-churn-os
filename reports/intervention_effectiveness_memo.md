# Intervention effectiveness decision memo

## Decision

**Do Not Scale** for the success-plan outreach treatment. The estimated gross MRR retention uplift is -0.11% (95% CI -1.84% to 1.66%); annualized commercial ROI is -103.52%.

## Experimental contract

- Estimand: intent-to-treat assignment effect among eligible active high-risk accounts.
- Unit: customer account; treatment is assignment to success-plan outreach; control is no action.
- Assignment: 50/50 randomization blocked by segment and pre-treatment risk band.
- Outcome window: 3 months after assignment.
- Uncertainty: 95% stratified nonparametric bootstrap with 1,000 samples.
- Commercial model: uplift × treated baseline MRR × 12 months × 80% gross margin, less observed intervention cost.

## Evidence

- Accounts: 1,825 (910 treatment; 915 control).
- Logo retention uplift: 0.21% (95% CI -1.33% to 1.96%).
- Gross MRR retention uplift: -0.11% (95% CI -1.84% to 1.66%).
- Estimated incremental retained MRR: $-2,789.
- Intervention cost: $761,600; annualized incremental gross profit: $-26,774.
- Largest absolute standardized mean difference: 0.025 (within the 0.10 balance threshold).

## Interpretation boundary

This repository's experiment is assigned over synthetic operating data and demonstrates the measurement system, not external evidence that the treatment works in a real SaaS portfolio. Production use requires prospectively logged assignment, execution and cost data. The ITT estimate must remain primary even when some assigned accounts are not contacted; per-protocol cuts are diagnostic only.

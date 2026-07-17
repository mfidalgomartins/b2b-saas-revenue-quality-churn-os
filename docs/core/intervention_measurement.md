# Intervention measurement contract

## Decision question

Should a high-risk-account success-plan outreach programme be scaled, continued as a test, or stopped after accounting for retention uncertainty and delivery cost?

## Experimental design

- **Population:** active accounts at the assignment month whose pre-treatment risk is at or above the within-segment median.
- **Unit:** customer account.
- **Treatment:** assignment to success-plan outreach.
- **Counterfactual:** assignment to no action.
- **Assignment:** 50/50 randomization within segment and pre-treatment risk quartile.
- **Primary estimand:** intent-to-treat effect among eligible accounts.
- **Primary outcomes:** logo retention and capped gross MRR retention three months after assignment.
- **Uncertainty:** 95% blocked nonparametric bootstrap interval.

The eligibility score reads only the assignment-month revenue-quality mart. Forward outcomes are joined in a separate function and the tests assert that changing future values cannot change assignment.

## Commercial translation

The commercial model uses:

```text
incremental retained MRR = gross MRR retention uplift × treated baseline MRR
annualized incremental gross profit = incremental retained MRR × 12 × gross margin
ROI = (annualized incremental gross profit - intervention cost) / intervention cost
```

Gross margin defaults to 80% and is a configurable assumption, not an observed accounting value. Costs are attached only to treatment assignments. The decision logic is:

- `scale_candidate`: the lower retention-uplift interval is positive and point-estimate ROI is positive;
- `do_not_scale`: the upper uplift interval is non-positive or point-estimate ROI is non-positive;
- `continue_test`: the evidence is otherwise inconclusive.

## Production input

Use `--ledger-path` to evaluate a prospectively captured assignment ledger. It must contain unique experiment/customer assignments, assignment probability, pre-treatment score, baseline MRR, treatment/control group, intervention type, account owner, segment, risk band, and realized treatment cost. Baseline MRR is reconciled to the canonical assignment-month mart before any estimate is produced.

The repository's default ledger is a deterministic blocked randomization layered over synthetic operating outcomes. It proves the evaluation workflow; it is not evidence of real-world treatment effectiveness. The primary result deliberately remains the ITT estimate, including assigned accounts that may not complete the intervention.

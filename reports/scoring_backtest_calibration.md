# Scoring Backtest Calibration Report

- Forward horizon: `3` months
- Evaluation rows: `88,613`
- Evaluation accounts: `4,343`
- Overall forward churn rate: `2.46%`

## Tier Calibration

| risk_tier | observations | unique_accounts | avg_score | churn_events_3m | churn_rate_3m | lift_vs_overall |
|---|---|---|---|---|---|---|
| Low | 87993 | 4343 | 6.7750 | 2117 | 0.0241 | 0.9766 |
| Moderate | 579 | 154 | 38.2567 | 61 | 0.1054 | 4.2766 |
| High | 41 | 21 | 59.8470 | 5 | 0.1220 | 4.9503 |

## Decile Calibration

| score_decile | observations | avg_score | churn_rate_3m | lift_vs_overall |
|---|---|---|---|---|
| 1.0000 | 8862.0000 | 1.1730 | 0.0142 | 0.5771 |
| 2.0000 | 9928.0000 | 2.8444 | 0.0177 | 0.7196 |
| 3.0000 | 7794.0000 | 3.8255 | 0.0214 | 0.8698 |
| 4.0000 | 9344.0000 | 4.6508 | 0.0227 | 0.9210 |
| 5.0000 | 8379.0000 | 5.4762 | 0.0241 | 0.9786 |
| 6.0000 | 8861.0000 | 6.3582 | 0.0225 | 0.9116 |
| 7.0000 | 8861.0000 | 7.4298 | 0.0256 | 1.0399 |
| 8.0000 | 8861.0000 | 8.8718 | 0.0258 | 1.0491 |
| 9.0000 | 8861.0000 | 11.0827 | 0.0264 | 1.0720 |
| 10.0000 | 8862.0000 | 18.5033 | 0.0464 | 1.8826 |

## Interpretation

- Tier calibration is monotonic: higher risk tiers show higher realized forward churn.
- This is a rule-based calibration diagnostic, not a causal model or externally validated forecast.
- Use this report to tune tier thresholds and operational intervention triggers.

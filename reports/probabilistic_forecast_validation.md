# Probabilistic MRR forecast validation

## Forecast contract

- Method: local linear trend with moving-block residual bootstrap. Each path refits the trend on a bootstrapped training history, capturing trend-estimation and process uncertainty while preserving short-run residual dependence.
- Information set: only growth observations available at each forecast origin; latest 18 observations at most.
- Production horizon: 12 months; backtest horizon: 6 months.
- Simulation: 2,000 paths per origin, block length 3 months, deterministic seed 42.
- Intervals: P10–P90 is the central 80% range; P05–P95 is the central 90% range.

## Rolling-origin evidence

- Forecast-origin/horizon observations: 123.
- Median-path MAE: $85,990; MAPE: 1.07%; signed bias: $-2,003.
- Central 80% empirical coverage: 84.6%; central 90% coverage: 91.1%.
- Mean central-80% interval width: $295,960 (3.7% of actual MRR).

## Use boundary

These are empirical operating ranges, not guarantees. Thirty-six monthly observations limit tail estimation and regime-shift detection. Scenario forecasts remain the decision tool for explicit policy or commercial assumptions; probabilistic intervals quantify historical process uncertainty around the current trajectory. Coverage is reported rather than tuned on the final holdout.

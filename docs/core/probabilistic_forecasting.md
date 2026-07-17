# Probabilistic forecasting method

The probabilistic forecast answers a different question from the commercial scenario model. Scenarios quantify explicit management assumptions; forecast intervals quantify uncertainty if the recent MRR process continues.

## Model

1. Aggregate canonical account MRR by month and calculate actual month-over-month company MRR growth.
2. Fit a local linear trend to at most the latest 18 growth observations.
3. Calculate trend residuals and sample contiguous three-month residual blocks.
4. For every simulated path, bootstrap the training residuals, refit the trend, and sample future residuals. This includes trend-estimation uncertainty and preserves short-run dependence.
5. Compound simulated growth into MRR and report P05, P10, P50, P90 and P95 by horizon.

Growth draws are bounded between -25% and +25% per month as a numerical safeguard. The default production run uses 2,000 deterministic simulations for a 12-month horizon.

## Validation

The rolling-origin backtest requires at least 12 observed growth months and evaluates horizons one through six. Each forecast is fit only on data at or before its origin. Reported diagnostics include:

- median-path MAE, MAPE and signed bias;
- empirical coverage of the P10–P90 and P05–P95 intervals;
- absolute and relative interval width by horizon;
- origin-level forecasts and actuals for audit.

No final holdout is used to tune interval width. With only 36 observations, the model cannot estimate rare tail events or structural breaks reliably. It should be refreshed as history accumulates and challenged whenever pricing, packaging, acquisition capacity or market conditions change materially.

# Performance

The pipeline is I/O-light and CPU-bound on a handful of pandas group operations. This note records the one
hotspot that was profiled and optimised, the measured result, and the parity guarantee.

## Method

Per-stage wall-clock is logged by the orchestrator itself (`run_project_pipeline.py` prints a timing table at
the end of every run). The backtest hot functions are benchmarked in isolation by `make benchmark`
(`scripts/benchmark_backtest.py`), best-of-3 on the seeded panel (112,038 customer-months, seed 42).

Figures below were taken on Apple Silicon, Python 3.12, single machine — treat them as ratios, not absolutes.

## Hotspot: the calibration backtest

Profiling the full run pointed at `src/scoring/backtest_scoring_calibration.py`. Two patterns dominated:

1. **Trailing features** were built with `groupby(...).apply(lambda s: s.rolling(...).mean())` and, for the
   usage trend, `rolling(3).apply(np.polyfit, ...)` — roughly **36,000 per-window SVD solves** plus the
   per-group Python dispatch of `groupby.apply`.
2. **Forward-churn labels** were assigned with a per-row `pd.DateOffset` construction inside a double loop
   (~160k offset objects).

### Changes

- Trailing means now use the **native grouped-rolling** path (`groupby(col).rolling(...).mean()`), which is
  bit-identical to the old per-group rolling mean without the apply overhead.
- The usage trend uses a **closed-form trailing OLS slope** (`scoring_utils.rolling_trailing_slope`). For an
  equally-spaced window of ≤3 points the slope reduces to `(last − first) / (k − 1)` and is independent of the
  interior point, so it is the exact least-squares slope — no per-window solve.
- `attach_forward_churn` replaces the `DateOffset` loop with two vectorised `searchsorted` boundaries over a
  churn prefix-sum on the integer month index (`year*12 + month`), preserving the exact date-window semantics
  including customers with month gaps.

### Result

| Measurement | Before | After |
|---|---|---|
| `backtest` stage (subprocess wall-clock) | ~5.8 s | ~0.9 s |
| Full pipeline (12 stages + validate + gate) | ~33 s | ~25 s |
| `build_trailing_panel` (best-of-3, in-process) | — | ~455 ms |
| `attach_forward_churn` (best-of-3, in-process) | — | ~37 ms |

## Parity guarantee

The optimisation is **decision-invariant**, verified by rebuilding the full pipeline and diffing every
artifact:

- The backtest summary JSON and **every count, tier/decile membership, `forward_churn_rate`, `churn_events`,
  and `lift_vs_overall`** are byte-identical.
- The only numeric movement is the reported `avg_score` column in the two calibration tables, which shifts by
  ≤1.1e-7 — a single account's score crossing a 3rd-decimal rounding boundary because the exact closed-form
  slope replaces an SVD-based `np.polyfit`. This is below any reporting or decision precision.

The equivalence is locked by unit tests: `rolling_trailing_slope` is checked against `np.polyfit` over 200
random series, and `attach_forward_churn` is checked against the original date-offset implementation over
gapped panels (`tests/test_scoring_utils.py`, `tests/test_backtest_forward_churn.py`). The validation gate
(`make gate`) continues to pass at `technically valid`.

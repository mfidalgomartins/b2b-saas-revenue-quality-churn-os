# B2B SaaS Revenue Quality & Churn Early Warning OS

One-line description: Commercial analytics operating system that surfaces revenue quality, churn risk, and decision-ready intervention priorities for B2B SaaS leadership.

## Business Problem
Topline growth can hide weak retention, discount-driven expansion, and concentrated downside risk. Leadership needs a single view of revenue durability and early churn exposure before it shows up in financials.

## What the System Does
- Generates realistic SaaS commercial data and builds governed metric and feature layers.
- Scores churn risk, revenue quality, discount dependency, and governance priority with explainable logic.
- Produces scenario forecasts, executive charts, and an offline HTML dashboard.
- Runs formal validation to keep outputs defensible.

## Decisions Supported
- Where to focus renewal and CS intervention.
- Which segments/channels are driving healthy vs fragile expansion.
- How much ARR is at risk under current churn patterns.
- Whether discounting is buying short-term growth at long-term cost.

## Project Architecture
Raw data → profiling → metric & feature layer → scoring → analysis & scenarios → visualization → dashboard → validation.

## Repository Structure
```
src/         data/        outputs/     reports/
docs/core/   sql/         notebooks/   tests/
```

## Core Outputs
- `outputs/dashboard/executive_dashboard.html` (offline executive dashboard)
- `outputs/charts/` (leadership charts)
- `reports/` (profiling memo, business analysis memo, validation report)
- `data/processed/` (analytical tables, scoring outputs, scenario tables)

## Why This Project Is Strong
- End-to-end, decision-first analytics system, not isolated notebooks.
- Metrics and scores are transparent and traceable to raw data.
- Validation gates reduce false confidence.

## How to Run
```bash
python3 src/pipeline/run_project_pipeline.py --base-dir . --seed 42
```
Optional: run validation gate
```bash
python3 src/validation/check_validation_gate.py --summary-path reports/formal_validation_summary.json --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0
```

## Limitations
- Synthetic data by design.
- Rule-based scoring is interpretable, not causal.

Tools: Python, SQL, pandas, Plotly, HTML, CSS, JavaScript.

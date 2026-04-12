# B2B SaaS Revenue Quality & Churn Early Warning OS

An end-to-end commercial analytics system designed to answer a simple leadership question: is growth durable, or is it being propped up by discount-heavy expansion and hidden churn risk? It packages data, scoring, scenarios, and an executive dashboard into one decision-ready operating model.

## Why this exists
Topline MRR can look healthy while revenue quality quietly degrades. When discounting, weak adoption, and concentrated account risk stack up, leadership needs early, defensible signals—not post‑fact churn.

## What it delivers
Synthetic RevOps data is generated, profiled, engineered into a governed metric layer, scored into interpretable risk and quality signals, and turned into scenarios, charts, and a self‑contained dashboard. Everything is traceable to raw fields and checked through a formal validation gate.

## Decisions it supports
- Where to intervene before renewals turn into churn.
- Which segments and channels drive resilient expansion vs fragile growth.
- How much ARR is realistically at risk under current patterns.
- Whether discounting is buying short‑term growth at long‑term cost.

## Architecture in one line
Raw data → profiling → metric & feature layer → scoring → analysis & scenarios → visualization → dashboard → validation.

## Repository map (main folders)
```
src/         data/        outputs/     reports/
docs/core/   sql/         notebooks/   tests/
```

## Core outputs
- `outputs/dashboard/executive_dashboard.html` (offline executive dashboard)
- `outputs/charts/` (leadership‑grade visuals)
- `reports/` (profiling memo, business analysis memo, validation report)
- `data/processed/` (analytical tables, scoring outputs, scenario tables)

Live dashboard (GitHub Pages): https://mfidalgomartins.github.io/b2b-saas-revenue-quality-churn-os/

## Why this is above typical portfolio work
It’s not a notebook exercise. It’s a full operating system with metric governance, scoring explainability, scenario logic, and validation discipline—built to support decisions, not just visuals.

## Run
```bash
python3 src/pipeline/run_project_pipeline.py --base-dir . --seed 42
```
Optional strict gate:
```bash
python3 src/validation/check_validation_gate.py --summary-path reports/formal_validation_summary.json --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0
```

## Limitations
- Synthetic data by design.
- Scoring is interpretable and rule‑based, not causal.

Tools: Python, SQL, pandas, Plotly, HTML, CSS, JavaScript.

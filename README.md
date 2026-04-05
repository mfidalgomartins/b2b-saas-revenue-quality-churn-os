# B2B SaaS Revenue Quality & Churn Early Warning Operating System

## Project Overview
This project builds an end-to-end commercial analytics operating system for a B2B SaaS business. It combines synthetic revenue operations data, metric engineering, risk scoring, scenario forecasting, executive visualization, and an offline HTML dashboard.

The objective is to evaluate whether growth is healthy (durable recurring expansion and retention) or fragile (discount-led growth, weak acquisition quality, concentrated downside risk).

## Business Problem
Leadership needs to answer:
- Are we growing through healthy recurring revenue quality?
- Which segments/channels/managers drive resilient expansion vs fragile expansion?
- Where is churn and renewal risk concentrated?
- How exposed is ARR if current risk signals persist?

## Why This Matters
Topline growth can mask forward risk. If expansion is discount-dependent and churn risk is concentrated in high-value accounts, ARR volatility rises and planning quality degrades.

This operating model gives Finance, RevOps, CS, and commercial leadership a common system for:
- quality-adjusted growth governance,
- intervention prioritization,
- downside scenario planning.

## Repository Structure
```text
b2b-saas-revenue-quality-os/
  data/
    raw/
    processed/
  docs/
    synthetic_data_design.md
    synthetic_data_generation_note.md
    analytical_layer_notes.md
    feature_dictionary.md
    scoring_model_design.md
    reproducibility.md
    release_process.md
    data_contracts.md
    governance_readiness_policy.md
    dashboard_architecture.md
    release_readiness_checklist.md
  notebooks/
    01_operating_system_walkthrough.ipynb
  outputs/
    charts/
    dashboard/
  reports/
    data_profiling_memo.md
    main_business_analysis_memo.md
    forecasting_scenario_analysis.md
    scoring_priority_shortlist.md
    formal_validation_report.md
    formal_validation_findings.csv
  src/
    pipeline/
    profiling/
    data_generation/
    features/
    analysis/
    scoring/
    forecasting/
    visualization/
    dashboard/
    validation/
  sql/
    staging/
    marts/
  tests/
  .github/workflows/qa.yml
  LICENSE
  CONTRIBUTING.md
  CHANGELOG.md
  pyproject.toml
  README.md
  requirements-notebook.txt
  requirements-dev.txt
  methodology.md
  data_dictionary.md
  executive_summary.md
```

## Datasets Used
### Raw tables
- `customers.csv`
- `plans.csv`
- `subscriptions.csv`
- `monthly_account_metrics.csv`
- `invoices.csv`
- `account_managers.csv`

### Processed tables
- `account_monthly_revenue_quality.csv`
- `customer_health_features.csv`
- `cohort_retention_summary.csv`
- `account_risk_base.csv`
- `account_manager_summary.csv`
- `account_scoring_model_output.csv`
- `account_scoring_components.csv`
- `scoring_priority_shortlist.csv`
- `scoring_backtest_calibration_by_tier.csv`
- `scoring_backtest_calibration_by_decile.csv`
- `baseline_mrr_forecast.csv`
- `risk_adjusted_mrr_forecast.csv`
- `scenario_mrr_trajectories.csv`
- `mrr_scenario_table.csv`
- `commercial_risk_impact_estimates.csv`

## Methodology
High-level flow:
1. Synthetic data generation with embedded commercial behavior.
2. Reproducible profiling and quality checks.
3. Metric layer and feature engineering.
4. Reproducible descriptive and diagnostic business analysis.
5. Interpretable scoring system (0-100 with tiers and actions).
6. Interpretable scenario forecasting.
7. Executive chart package and offline dashboard.
8. Formal validation across data, metrics, scoring, forecasting, dashboard feed, and narrative claims.

Detailed methodology is in `methodology.md`.

Formal reproducibility instructions are in `docs/reproducibility.md`.

Data and artifact contract definitions are in `docs/data_contracts.md`.

Governance readiness policy and release-state taxonomy are in `docs/governance_readiness_policy.md`.

## Key Metrics
- MRR, ARR
- Logo churn, revenue churn
- Gross Revenue Retention (GRR)
- Net Revenue Retention (NRR)
- Expansion and contraction MRR
- Realized price index
- Average discount and discount-dependent revenue share
- Revenue concentration (top-account share)
- At-risk MRR and risk concentration

## Scoring Framework
Interpretable scores (0-100):
- `churn_risk_score` (higher = riskier)
- `revenue_quality_score` (higher = healthier)
- `discount_dependency_score` (higher = riskier)
- `expansion_quality_score` (higher = healthier)
- `governance_priority_score` (higher = more urgent)

Each account receives:
- risk/quality tiers (`Low`, `Moderate`, `High`, `Critical`),
- a primary risk driver,
- recommended action (for example renewal intervention, discount policy review, repricing at renewal).

## Dashboard Overview
Offline, self-contained executive dashboard:
- file: `outputs/dashboard/executive_dashboard.html`
- sections: Executive Overview, Revenue Quality, Retention & Churn, Account Risk, Portfolio/Manager, Scenario & Forecast, Methodology
- includes governed KPI cards, embedded chart artifacts, filters for account-level slicing, sortable/searchable account drilldown, methodology drawer, and source-map panel.
- dashboard technical architecture and update flow: `docs/dashboard_architecture.md`

## Key Findings (Current Run)
- MRR grew from ~$3.75M to ~$9.52M (+154%) over 36 months.
- Latest retention is strong on gross basis (GRR ~99.18%) but NRR remains near parity (~99.83%).
- Discount-dependent share is material (~15.9% latest month).
- SMB churn is materially higher than Enterprise.
- Expansion is positive overall but part of expansion is fragile in quality terms.
- Risk is concentrated: a small account subset drives disproportionate downside exposure.

## Recommendations
1. Tighten discount governance in high-discount channels and renewal motions.
2. Build renewal intervention playbooks for high/critical governance accounts.
3. Prioritize SMB quality improvements (usage adoption, support burden, payment behavior).
4. Track risk concentration (top accounts) as a formal operating KPI.
5. Use scenario layer in monthly planning to quantify downside and intervention upside.

## Limitations
- Data is synthetic by design.
- Latest formal validation run is fully green (`19 PASS / 0 WARN / 0 FAIL`) with governance tier `technically valid`.
- Scoring is rule-based and interpretable, not a causal/predictive ML model.

## How To Run
From project root:

```bash
# 1) Generate synthetic data
python3 src/data_generation/generate_synthetic_data.py --output-dir data/raw --note-path docs/synthetic_data_generation_note.md --seed 42

# 2) Build analytical layer
python3 src/features/build_analytical_layer.py --raw-dir data/raw --processed-dir data/processed --feature-dictionary-path docs/feature_dictionary.md --notes-path docs/analytical_layer_notes.md

# 3) Run data profiling
python3 src/profiling/build_data_profile.py --base-dir .

# 4) Build scoring outputs
python3 src/scoring/build_scoring_system.py --base-dir .

# 5) Build business analysis memo + metrics
python3 src/analysis/build_main_business_analysis.py --base-dir .

# 6) Build forecasting/scenarios
python3 src/forecasting/build_forecasting_scenarios.py --base-dir .

# 7) Backtest scoring calibration
python3 src/scoring/backtest_scoring_calibration.py --base-dir .

# 8) Build charts
python3 src/visualization/build_leadership_charts.py --base-dir .

# 9) Build executive dashboard
python3 src/dashboard/build_executive_dashboard.py --base-dir . --output outputs/dashboard/executive_dashboard.html

# 10) Run formal validation
python3 src/validation/run_full_project_validation.py --base-dir .

# 11) Enforce strict validation gate
python3 src/validation/check_validation_gate.py --summary-path reports/formal_validation_summary.json --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0

# 12) Optional: enforce governance readiness tier explicitly
python3 src/validation/check_validation_gate.py --summary-path reports/formal_validation_summary.json --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0 --min-readiness-tier "technically valid"
```

Convenience options:

```bash
# full pipeline with deterministic seed
python3 src/pipeline/run_project_pipeline.py --base-dir . --seed 42

# or with Makefile
make all

# run contract tests
make test

# run lint checks
make lint

# run full QA gate (tests + validation + gate)
make qa

# release-grade path (lint + tests + pipeline + strict gate)
make release-ready

# monthly refresh with manifest + checksums
make release-refresh
```

Notebook walkthrough:
```bash
# optional notebook dependency
pip install -r requirements-notebook.txt

jupyter notebook notebooks/01_operating_system_walkthrough.ipynb
```

## Future Extensions
- Add confidence intervals for scenario ranges.
- Add manager mix-adjusted benchmarking.
- Add account-level intervention outcome tracking loop (closed-loop analytics).
- Add temporal train/validation split with strict pre-event feature cutoffs for predictive-grade modeling.

## Contribution
- Contribution process and quality expectations: `CONTRIBUTING.md`
- Release history: `CHANGELOG.md`

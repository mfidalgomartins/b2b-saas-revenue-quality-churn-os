# Methodology

## 1) Data Generation Assumptions
The project uses synthetic data designed to emulate B2B SaaS commercial behavior over 36 months (2023-03 to 2026-02).

### Commercial behavior encoded
- Segment retention hierarchy: Enterprise retention > Mid-Market > SMB.
- Channel discount variance: higher discount pressure in paid-media/outbound cohorts.
- Plan and seat realism: segment-dependent plan-tier mix and seat distributions.
- Health-linked risk: higher churn likelihood when usage declines, NPS deteriorates, payment delays increase, and support burden rises.
- Mixed expansion quality: both healthy expansion and fragile (discount-led) expansion.
- Risk concentration: a moderate concentration profile with top-account downside exposure.

### Constraints
- Unique IDs across entities.
- Coherent keys across customer, subscription, monthly, and invoice tables.
- Monthly panel consistency for account metrics and revenue-quality tables.
- Invoice arithmetic coherence:
  - `discount_amount` is commercial discount only,
  - `collection_loss_amount` captures collection/default haircut,
  - `effective_revenue_adjustment_amount = discount_amount + collection_loss_amount`,
  - `billed_mrr - effective_revenue_adjustment_amount = realized_mrr` at row level.

## 2) Metric Definitions
Primary operating metrics:
- `MRR_t = sum(active_mrr_t)`
- `ARR_t = 12 * MRR_t`
- `Logo churn rate_t = churn_events_t / active_account_rows_t`
- `Revenue churn rate_t = churned_mrr_t / active_mrr_t`
- `GRR_t = (starting_mrr_t - contraction_mrr_t - churn_mrr_t) / starting_mrr_t`
- `NRR_t = (starting_mrr_t + expansion_mrr_t - contraction_mrr_t - churn_mrr_t) / starting_mrr_t`
- `Realized price index_t = realized_mrr_t / active_mrr_t`
- `Discounted revenue share_t = discounted_mrr_t / total_mrr_t`

Portfolio risk metrics:
- `At-risk MRR`: sum of `current_mrr` for high/critical governance-priority accounts.
- `Concentration`: cumulative share of MRR held by top-N accounts.

## 3) Feature Engineering Logic
Reference SQL semantic-layer equivalents are provided under:
- `sql/staging/`
- `sql/marts/`

These SQL models mirror core Python transformations to support warehouse-first migration and reviewability.

### account_monthly_revenue_quality
Purpose: month-level commercial quality panel.

Key derivations:
- `active_mrr`: contracted MRR only when active.
- `avg_discount_pct`: invoice discount ratio, fallback subscription discount.
- `realized_price_index`: realized/active ratio with clipping.
- `discount_dependency_flag`: trailing-3M discount and high-discount expansion rules.
- `revenue_quality_flag`: rule-based healthy/watch/fragile/inactive classification.
- `renewal_risk_proxy`: weighted risk blend (renewal timing, usage, NPS, payment, support, discount).

### customer_health_features
Purpose: account snapshot for scoring and interventions.

Key derivations:
- trailing 3M behavior averages and usage trend slope.
- seat growth, expansion/contraction frequencies (12M window).
- churn history, renewal due status, concentration weight, tenure months.

### cohort_retention_summary
Purpose: cohort durability tracking by segment/region.

Key derivations:
- `cohort_month`: first subscription month.
- `month_number`: months since cohort start.
- `gross_retention_rate`: retained base revenue / cohort baseline revenue.
- `net_retention_rate`: retained total revenue / cohort baseline revenue.

### account_risk_base
Purpose: explainable JSON-structured risk input table.

Outputs:
- `churn_risk_inputs`
- `revenue_quality_inputs`
- `account_fragility_inputs`
- `forward_risk_flags`

### account_manager_summary
Purpose: manager-level governance lens.

Metrics:
- portfolio MRR, weighted discount, retention/churn rates, expansion rate, risk-weighted portfolio score.

## 4) Scoring Logic
Interpretable weighted scoring with 0-100 scale.

### churn_risk_score (higher = riskier)
Component families:
- usage deterioration,
- sentiment/support stress,
- payment stress,
- contraction pattern,
- discount pressure,
- renewal exposure,
- history/tenure fragility.

### revenue_quality_score (higher = healthier)
Component families:
- pricing realization,
- discount discipline,
- retention momentum,
- account health quality,
- governance stability.

### discount_dependency_score (higher = riskier)
Component families:
- discount level,
- discount persistence,
- discounted expansion pressure,
- realization erosion,
- policy signal.

### expansion_quality_score (higher = healthier)
Component families:
- healthy expansion mix,
- fragile expansion control,
- expansion discount discipline,
- expansion payment quality,
- post-expansion durability.

### governance_priority_score (higher = more urgent)
Blends:
- churn risk,
- revenue quality risk,
- discount dependency,
- expansion fragility,
- exposure concentration,
- renewal urgency.

Each account gets:
- tier assignment,
- dominant risk driver,
- recommended action.

## 5) Scenario Logic
Forecasting is a transparent rate-based framework.

### Baseline
- Uses recency-weighted averages of expansion, contraction, churn, and net-new rates.

### Risk-adjusted
- Applies overlay from risk concentration and high-risk portfolio share.

### Scenarios
- base_case
- downside_case (fragile-growth stress)
- improvement_case (healthy-growth execution)
- discount_discipline_improvement_case
- risk_adjusted_case

Outputs include:
- monthly trajectory tables,
- scenario summary table,
- commercial impact estimates (ARR at risk, downside exposure, stress tests).

## 6) Validation Approach
A formal 20-check QA gate is implemented in:
- `src/validation/run_full_project_validation.py`

### Checks covered
1. row count sanity
2. null checks
3. duplicate checks
4. impossible values
5. date logic consistency
6. revenue reconciliation
7. discount logic consistency
8. retention denominator correctness
9. cohort logic correctness
10. score range correctness
11. risk tier consistency
12. scenario integrity
13. join inflation and dashboard feed integrity
14. leakage risk
15. narrative overclaiming risk
16. cross-output metric consistency (processed vs report vs dashboard)
17. score stability and calibration safeguards
18. financial/decision logic integrity for scenarios and impact table
19. release artifact readiness for the executive dashboard
20. test suite integrity (unittest discovery/execution)

Validation outputs:
- `reports/formal_validation_report.md`
- `reports/formal_validation_findings.csv`
- `reports/formal_validation_summary.json`

Governance readiness classification in validation summary:
- `publish-blocked`
- `not committee-grade`
- `screening-grade only`
- `decision-support only`
- `analytically acceptable`
- `technically valid`

## 6.1) Reproducible Profiling and Analysis Artifacts
To avoid static/stale narrative artifacts, profiling and business-analysis outputs are generated by scripts in the pipeline:
- `src/profiling/build_data_profile.py` -> `reports/data_profiling_memo.md`
- `src/analysis/build_main_business_analysis.py` -> `reports/main_business_analysis_metrics.json`, `reports/main_business_analysis_memo.md`

## 7) Interpretation Guardrails
- Findings are descriptive and diagnostic, not causal claims.
- Scenario outputs are decision-support ranges, not point-accurate forecasts.
- Scores are governance tools and should be periodically recalibrated.

## 8) Release Governance
Monthly refresh process is automated via:
- `src/pipeline/monthly_release_refresh.py`

Release artifacts:

Optional release tags can be requested during refresh when running inside a git repository.

# Release Readiness Checklist

Use this checklist before sharing outputs with leadership.

## 1) Technical Integrity
- [ ] `python3 src/pipeline/run_project_pipeline.py --base-dir . --seed 42` completes with no errors.
- [ ] `python3 -m unittest discover -s tests -p 'test_*.py'` passes.
- [ ] `python3 -m ruff check src tests` passes.

## 2) Validation and Governance
- [ ] `python3 src/validation/run_full_project_validation.py --base-dir .` completes.
- [ ] Strict gate passes:
  ```bash
  python3 src/validation/check_validation_gate.py \
    --summary-path reports/formal_validation_summary.json \
    --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0 \
    --min-readiness-tier "technically valid"
  ```
- [ ] `reports/formal_validation_summary.json` readiness tier is `technically valid`.

## 3) Cross-Output Consistency
- [ ] Dashboard KPI cards match `reports/main_business_analysis_metrics.json`.
- [ ] Dashboard readiness stamp matches validation summary.
- [ ] Scenario tables, risk impact table, and report narratives are from the same refresh cycle.

## 4) Executive Reliability
- [ ] `outputs/dashboard/executive_dashboard.html` opens offline and renders fully.
- [ ] Filters, table search/sort/pagination, and tabs work.
- [ ] No empty or stale sections in executive narrative, alerts, and methodology.

## 5) Release Traceability
- [ ] `reports/release_manifest.json` exists and is current.
- [ ] `reports/release_checksums.csv` exists and includes dashboard artifact.
- [ ] Optional release tag/process documented in `docs/release_process.md`.

## Hard Rule
If any check above fails, do not publish stakeholder-facing outputs.

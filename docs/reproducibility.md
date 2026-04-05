# Reproducibility Guide

## Environment
Recommended:
- Python 3.11+
- macOS/Linux shell

Install dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Deterministic Data Generation
Synthetic generation supports a fixed seed:
```bash
python3 src/data_generation/generate_synthetic_data.py --output-dir data/raw --note-path docs/synthetic_data_generation_note.md --seed 42
```

Changing seed will change the synthetic dataset while preserving generation logic.

## End-to-End Pipeline
Option A (single orchestrator):
```bash
python3 src/pipeline/run_project_pipeline.py --base-dir . --seed 42
```
This path includes validation, dashboard refresh after validation, and strict validation gate enforcement by default.

Option B (Makefile):
```bash
make all
```

## Data Contract Tests
```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

## Validation Gate
Run formal QA before sharing outputs:
```bash
python3 src/validation/run_full_project_validation.py --base-dir .
```

Enforce strict release gate:
```bash
python3 src/validation/check_validation_gate.py --summary-path reports/formal_validation_summary.json --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0 --min-readiness-tier "technically valid"
```

Required review artifacts:
- `reports/formal_validation_report.md`
- `reports/formal_validation_findings.csv`
- `reports/formal_validation_summary.json`
- `reports/data_profiling_memo.md`
- `reports/main_business_analysis_memo.md`

## Output Artifacts
- Processed tables: `data/processed/`
- Charts: `outputs/charts/`
- Dashboard: `outputs/dashboard/executive_dashboard.html`
- Business memos: `reports/`

## Practical Release Checklist
See `docs/release_readiness_checklist.md`.

## Monthly Release Automation
For monthly publication with run metadata and checksums:
```bash
python3 src/pipeline/monthly_release_refresh.py --base-dir . --seed 42 --release-tag vYYYY.MM
```

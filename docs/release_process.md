# Monthly Release Process

## Objective
Standardize a monthly refresh so leadership-facing analytics outputs are reproducible, traceable, and quality-gated.

## Command
```bash
python3 src/pipeline/monthly_release_refresh.py --base-dir . --seed 42 --release-tag vYYYY.MM
```

If the project directory is not a git repository, the script still completes and records that tag creation was skipped.

## What The Process Runs
1. End-to-end pipeline execution.
2. Formal validation run.
3. Strict governance gate enforcement (`technically valid` minimum tier).
4. Artifact checksum generation.
5. Release manifest generation.
6. Optional git tag creation.

## Artifacts Created
- `reports/release_manifest.json`
- `reports/release_checksums.csv`
- Updated raw/processed tables, charts, dashboard, and reports from the latest run.

## Manifest Contents
- UTC release timestamp
- Python/runtime metadata
- pipeline options and seed
- data coverage window
- row counts for raw and processed tables
- validation summary snapshot
- release-tag result status

## Quality Gate Policy
Release is considered ready only when:
- `WARN` findings = 0
- `FAIL` findings = 0
- `High` severity findings = 0
- `Critical` severity findings = 0
- governance readiness tier >= `technically valid`

Gate command:
```bash
python3 src/validation/check_validation_gate.py \
  --summary-path reports/formal_validation_summary.json \
  --max-warn 0 \
  --max-fail 0 \
  --max-high-severity 0 \
  --max-critical-severity 0 \
  --min-readiness-tier "technically valid"
```

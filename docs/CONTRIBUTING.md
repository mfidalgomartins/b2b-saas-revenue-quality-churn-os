# Contributing Guide

## Scope
This repository is a portfolio-grade analytics operating system. Contributions should preserve:
- business readability,
- metric traceability,
- reproducibility,
- validation-first discipline.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Expected Local Checks
Run before proposing changes:
```bash
make qa
make release-refresh
```

## Coding Rules
- Keep transformations deterministic and explicit.
- Avoid leakage in feature design and scoring logic.
- Use business-readable naming.
- Do not introduce black-box logic without strong justification.

## Documentation Rules
- Update docs when logic changes (`README`, `methodology`, `data_dictionary`, `docs/*`).
- Keep validation claims aligned with latest `reports/formal_validation_summary.json`.

## Pull Request Checklist
1. `make qa` passes.
2. Validation gate has zero fail/high/critical findings.
3. Release manifest and checksums are refreshed if output logic changed.
4. New metrics/features are documented.
5. Dashboard still renders offline.

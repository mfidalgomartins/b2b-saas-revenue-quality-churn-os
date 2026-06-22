# Contributing

Thanks for looking under the hood. This project values **reproducibility and traceability**: every number in
the dashboard and report is derived by the pipeline from seeded inputs, and every quality claim is enforced by
an automated gate rather than asserted in prose.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Python 3.12+ is required.

## The one command that matters

```bash
make qa
```

`qa` runs, in order: format check → lint → tests with the coverage gate → static security scan → dependency
posture (via the gate workflow) → validation gate. If `make qa` is green, CI will be green — the
[workflow](.github/workflows/qa.yml) runs the same checks.

Individual gates:

| Command | What it checks |
|---|---|
| `make lint` | Ruff rules `E,F,I,B,UP,SIM,C4` |
| `make format-check` | Ruff formatter (run `make format` equivalent: `ruff format`) |
| `make test` | Full `unittest` suite |
| `make coverage` | Tests **plus** the 100% branch-coverage gate on the core library |
| `make security` | Bandit static analysis |
| `make audit` | `pip-audit` dependency CVE scan |
| `make validate` / `make gate` | Governance validation and the publish-readiness gate |
| `make all` | Full end-to-end pipeline rebuild from seed |

## Conventions

- **Scoring math lives in one place.** Weights and component formulas are defined once in
  [`src/scoring/scoring_utils.py`](src/scoring/scoring_utils.py) and imported by both the production scorer and
  the calibration backtest. Never duplicate a weight.
- **Schema contracts at load boundaries.** Reading a CSV? Route it through
  [`src/io/contracts.py`](src/io/contracts.py) so a renamed upstream column fails loudly instead of becoming
  silent `NaN`.
- **Pure logic is unit-tested to 100%.** New logic in the core library
  (`metrics`, `scoring_utils`, `io/*`, the validation gate, `dashboard_contract`) must keep
  `make coverage` at `fail_under = 100`. Pull the testable logic out of CLI `main()` wrappers so it can be
  exercised directly — see `evaluate_gate` in `check_validation_gate.py` as the pattern.
- **CLI entrypoints** (`def main`, `parse_args`) are marked `# pragma: no cover` and covered by the integration
  build instead.
- **Determinism.** The pipeline is seeded (`--seed 42`). A change that alters headline numbers should be
  intentional and reflected in the report/dashboard, which are regenerated from the same tables.

## Before opening a PR

1. `make qa` is green locally.
2. New behaviour has a test; new logic keeps coverage at 100%.
3. If you touched scoring weights, the validation gate and backtest parity still pass.
4. If you added a dependency, `make audit` is clean.

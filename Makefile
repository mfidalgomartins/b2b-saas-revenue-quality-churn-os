PYTHON ?= python3
BASE_DIR ?= .
SEED ?= 42
RAW_DIR := $(BASE_DIR)/data/raw
PROCESSED_DIR := $(BASE_DIR)/data/processed
FEATURE_DICT := $(BASE_DIR)/docs/core/feature_dictionary.md
ANALYTICAL_NOTES := $(BASE_DIR)/docs/core/analytical_layer_notes.md
VALIDATION_SUMMARY := $(BASE_DIR)/reports/formal_validation_summary.json

.PHONY: all data profile features scoring backtest sensitivity analysis forecast graphs supplementary-graphs dashboard dashboard-refresh report validate gate format-check lint typecheck test coverage security audit benchmark qa release-ready release-refresh clean

# dashboard is built twice by design, not by accident: once before `validate`
# so the validation gate's cross-output consistency check (#16) can compare the
# dashboard's KPI payload against this run's fresh data, then again
# (dashboard-refresh) after validate so the shipped dashboard embeds this run's
# actual validation readiness tier instead of a stale one. See
# src/pipeline/run_project_pipeline.py, which mirrors the same two-pass order.
all: data profile features scoring backtest sensitivity analysis forecast graphs supplementary-graphs dashboard validate dashboard-refresh report

data:
	$(PYTHON) src/data_generation/generate_synthetic_data.py --output-dir $(RAW_DIR) --note-path $(BASE_DIR)/docs/core/synthetic_data.md --seed $(SEED)

profile:
	$(PYTHON) src/profiling/build_data_profile.py --base-dir $(BASE_DIR)

features:
	$(PYTHON) src/features/build_analytical_layer.py --raw-dir $(RAW_DIR) --processed-dir $(PROCESSED_DIR) --feature-dictionary-path $(FEATURE_DICT) --notes-path $(ANALYTICAL_NOTES)

scoring:
	$(PYTHON) src/scoring/build_scoring_system.py --base-dir $(BASE_DIR)

backtest:
	$(PYTHON) src/scoring/backtest_scoring_calibration.py --base-dir $(BASE_DIR)

sensitivity:
	$(PYTHON) src/scoring/run_weight_sensitivity.py --base-dir $(BASE_DIR)

analysis:
	$(PYTHON) src/analysis/build_main_business_analysis.py --base-dir $(BASE_DIR)

forecast:
	$(PYTHON) src/forecasting/build_forecasting_scenarios.py --base-dir $(BASE_DIR)

graphs:
	$(PYTHON) src/visualization/build_executive_graphs.py --base-dir $(BASE_DIR)

supplementary-graphs:
	$(PYTHON) src/visualization/build_supplementary_graphs.py --base-dir $(BASE_DIR)

# Same command as dashboard-refresh below — intentionally. See the comment on
# the `all` target for why the pipeline runs this build twice.
dashboard:
	$(PYTHON) src/dashboard/build_executive_dashboard.py --base-dir $(BASE_DIR) --output $(BASE_DIR)/outputs/dashboard/revenue-quality-command-center.html

# Rebuilds the dashboard after `validate` so it embeds this run's actual
# validation readiness tier, not the tier from whatever build produced the
# dashboard the pre-validate pass checked.
dashboard-refresh:
	$(PYTHON) src/dashboard/build_executive_dashboard.py --base-dir $(BASE_DIR) --output $(BASE_DIR)/outputs/dashboard/revenue-quality-command-center.html

report:
	$(PYTHON) scripts/build_pdf_report.py

validate:
	$(PYTHON) src/validation/run_full_project_validation.py --base-dir $(BASE_DIR)

gate:
	$(PYTHON) src/validation/check_validation_gate.py --summary-path $(VALIDATION_SUMMARY) --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0 --min-readiness-tier "technically valid"

format-check:
	$(PYTHON) -m ruff format --check src tests scripts

lint:
	$(PYTHON) -m ruff check src tests scripts

typecheck:
	$(PYTHON) -m mypy

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

coverage:
	$(PYTHON) -m coverage run -m unittest discover -s tests -p 'test_*.py'
	$(PYTHON) -m coverage report

security:
	$(PYTHON) -m bandit -c pyproject.toml -r src scripts -q

audit:
	$(PYTHON) -m pip_audit --skip-editable --progress-spinner off

benchmark:
	$(PYTHON) scripts/benchmark_backtest.py --base-dir $(BASE_DIR)

qa: format-check lint typecheck coverage security audit validate gate

release-ready: format-check lint test all gate

release-refresh:
	$(PYTHON) src/pipeline/monthly_release_refresh.py --base-dir $(BASE_DIR) --seed $(SEED)

clean:
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '.DS_Store' -type f -delete

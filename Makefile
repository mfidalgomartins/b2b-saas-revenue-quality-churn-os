PYTHON ?= python3
BASE_DIR ?= .
SEED ?= 42
RAW_DIR := $(BASE_DIR)/data/raw
PROCESSED_DIR := $(BASE_DIR)/data/processed
FEATURE_DICT := $(BASE_DIR)/docs/feature_dictionary.md
ANALYTICAL_NOTES := $(BASE_DIR)/docs/analytical_layer_notes.md
VALIDATION_SUMMARY := $(BASE_DIR)/reports/formal_validation_summary.json

.PHONY: all data profile features scoring analysis backtest forecast viz dashboard validate gate lint test qa release-ready release-refresh docs clean

all: data profile features scoring analysis backtest forecast viz dashboard validate

data:
	$(PYTHON) src/data_generation/generate_synthetic_data.py --output-dir $(RAW_DIR) --note-path $(BASE_DIR)/docs/synthetic_data_generation_note.md --seed $(SEED)

profile:
	$(PYTHON) src/profiling/build_data_profile.py --base-dir $(BASE_DIR)

features:
	$(PYTHON) src/features/build_analytical_layer.py --raw-dir $(RAW_DIR) --processed-dir $(PROCESSED_DIR) --feature-dictionary-path $(FEATURE_DICT) --notes-path $(ANALYTICAL_NOTES)

scoring:
	$(PYTHON) src/scoring/build_scoring_system.py --base-dir $(BASE_DIR)

analysis:
	$(PYTHON) src/analysis/build_main_business_analysis.py --base-dir $(BASE_DIR)

backtest:
	$(PYTHON) src/scoring/backtest_scoring_calibration.py --base-dir $(BASE_DIR)

forecast:
	$(PYTHON) src/forecasting/build_forecasting_scenarios.py --base-dir $(BASE_DIR)

viz:
	$(PYTHON) src/visualization/build_leadership_charts.py --base-dir $(BASE_DIR)

dashboard:
	$(PYTHON) src/dashboard/build_executive_dashboard.py --base-dir $(BASE_DIR) --output $(BASE_DIR)/outputs/dashboard/executive_dashboard.html

validate:
	$(PYTHON) src/validation/run_full_project_validation.py --base-dir $(BASE_DIR)

gate:
	$(PYTHON) src/validation/check_validation_gate.py --summary-path $(VALIDATION_SUMMARY) --max-warn 0 --max-fail 0 --max-high-severity 0 --max-critical-severity 0 --min-readiness-tier "technically valid"

lint:
	$(PYTHON) -m ruff check src tests

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

qa: lint test validate gate

release-ready: lint test all gate

release-refresh:
	$(PYTHON) src/pipeline/monthly_release_refresh.py --base-dir $(BASE_DIR) --seed $(SEED)

docs:
	@echo "Documentation is maintained in README.md, methodology.md, data_dictionary.md, executive_summary.md"

clean:
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '.DS_Store' -type f -delete

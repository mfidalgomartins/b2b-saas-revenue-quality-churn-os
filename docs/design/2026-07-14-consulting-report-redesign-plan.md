# Consulting-Grade Revenue Quality Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a second consulting-grade PDF report that preserves the current analytical content and existing PDF while applying the approved editorial system to layout, typography, charts and tables.

**Architecture:** Add one isolated builder that imports the validated current report module, injects a new theme and page furniture, regenerates the 15 referenced charts into a temporary directory, and builds to a temporary PDF before an atomic move to the new final filename. Add focused contract tests plus an integration build test; do not modify the existing report generator or existing PDF.

**Tech Stack:** Python 3.11+, ReportLab, Matplotlib, pandas, Pillow, Poppler CLI, pytest

## Global Constraints

- Preserve `outputs/reports/revenue_quality_os_analytical_report.pdf` byte-for-byte.
- Final output is `outputs/reports/revenue_quality_os_consulting_report.pdf`.
- Use `Source Serif 4` for display hierarchy and `Inter` for body, charts, tables and navigation.
- Use the approved palette: `#252323`, `#F5F4F1`, `#F04424`, `#AD2B1F`, `#F49A80`, `#D9D7D2`, `#9DA3A6` and white.
- Keep reference-PDF content, branding, photography, logo and proprietary marks out of the deliverable.
- Reuse only repository datasets and the validated narrative in the current report builder.
- Keep all chart and rendering intermediates under `tmp/pdfs/` and remove them before final handoff.
- Do not modify `scripts/build_pdf_report.py`, `src/visualization/build_executive_graphs.py`, `src/visualization/build_supplementary_graphs.py` or the existing PDF.

---

### Task 1: Builder contracts and source protection

**Files:**
- Create: `scripts/build_consulting_pdf_report.py`
- Create: `tests/test_consulting_pdf_report.py`

**Interfaces:**
- Consumes: repository root resolved from the script path.
- Produces: `sha256(path: Path) -> str`, `validate_inputs(base_dir: Path) -> None`, `parse_args(argv: list[str] | None = None) -> argparse.Namespace`.

- [ ] **Step 1: Write contract tests for distinct output and input validation**

```python
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "build_consulting_pdf_report.py"


def load_builder():
    spec = importlib.util.spec_from_file_location("consulting_report", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_output_path_is_distinct_from_existing_report() -> None:
    module = load_builder()
    assert module.DEFAULT_OUTPUT.name == "revenue_quality_os_consulting_report.pdf"
    assert module.DEFAULT_OUTPUT != module.EXISTING_REPORT


def test_validate_inputs_rejects_missing_project_assets(tmp_path: Path) -> None:
    module = load_builder()
    with pytest.raises(FileNotFoundError, match="Required report input"):
        module.validate_inputs(tmp_path)
```

- [ ] **Step 2: Run the two tests and confirm they fail because the builder does not exist**

Run: `pytest tests/test_consulting_pdf_report.py -q`  
Expected: collection fails with `FileNotFoundError` for `scripts/build_consulting_pdf_report.py`.

- [ ] **Step 3: Create the builder shell and deterministic input contract**

```python
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
from pathlib import Path


BASE = Path(__file__).resolve().parents[1]
EXISTING_REPORT = BASE / "outputs" / "reports" / "revenue_quality_os_analytical_report.pdf"
DEFAULT_OUTPUT = BASE / "outputs" / "reports" / "revenue_quality_os_consulting_report.pdf"
REQUIRED_INPUTS = (
    Path("scripts/build_pdf_report.py"),
    Path("assets/fonts/Inter-Regular.ttf"),
    Path("assets/fonts/Inter-SemiBold.ttf"),
    Path("assets/fonts/SourceSerif4-Regular.ttf"),
    Path("assets/fonts/SourceSerif4-Bold.ttf"),
    Path("reports/main_business_analysis_metrics.json"),
    Path("reports/intervention_effectiveness_summary.json"),
    Path("reports/probabilistic_forecast_validation.json"),
    Path("data/processed/account_monthly_revenue_quality.csv"),
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_inputs(base_dir: Path) -> None:
    for relative_path in REQUIRED_INPUTS:
        path = base_dir / relative_path
        if not path.is_file():
            raise FileNotFoundError(f"Required report input is missing: {path}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the consulting-grade revenue quality PDF.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)
```

- [ ] **Step 4: Run contract tests**

Run: `pytest tests/test_consulting_pdf_report.py -q`  
Expected: `2 passed`.

- [ ] **Step 5: Commit the isolated contract**

```bash
git add scripts/build_consulting_pdf_report.py tests/test_consulting_pdf_report.py
git commit -m "test: define consulting report builder contract"
```

---

### Task 2: Editorial theme and page primitives

**Files:**
- Modify: `scripts/build_consulting_pdf_report.py`
- Modify: `tests/test_consulting_pdf_report.py`

**Interfaces:**
- Consumes: imported `scripts.build_pdf_report` module.
- Produces: `configure_report_theme(report: ModuleType) -> None`, `consulting_cover_page(canvas, doc) -> None`, `consulting_footer(canvas, doc, header: bool = True) -> None`, `consulting_cover_story(report: ModuleType) -> list`.

- [ ] **Step 1: Add a theme test that checks the approved type hierarchy and palette**

```python
def test_configure_report_theme_applies_approved_typography() -> None:
    module = load_builder()
    report = module.load_report_module()
    module.configure_report_theme(report)
    assert report.styles["body"].fontName == "Inter"
    assert report.styles["h1"].fontName == "SourceSerif4"
    assert report.styles["h1"].textColor.hexval().lower() == "0xf04424"
    assert report.styles["caption"].fontName == "Inter"
```

- [ ] **Step 2: Run the theme test and confirm the missing interface failure**

Run: `pytest tests/test_consulting_pdf_report.py::test_configure_report_theme_applies_approved_typography -q`  
Expected: FAIL because `load_report_module` is not defined.

- [ ] **Step 3: Implement module loading and theme injection**

```python
from importlib import import_module
from types import ModuleType

from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics


CHARCOAL = colors.HexColor("#252323")
PAPER = colors.HexColor("#F5F4F1")
ORANGE = colors.HexColor("#F04424")
DEEP_RED = colors.HexColor("#AD2B1F")
CORAL = colors.HexColor("#F49A80")
WARM_GREY = colors.HexColor("#D9D7D2")
COOL_GREY = colors.HexColor("#9DA3A6")


def load_report_module() -> ModuleType:
    if str(BASE) not in sys.path:
        sys.path.insert(0, str(BASE))
    return import_module("scripts.build_pdf_report")


def configure_report_theme(report: ModuleType) -> None:
    pdfmetrics.registerFontFamily(
        "Inter",
        normal="Inter",
        bold="Inter-SemiBold",
        italic="Inter",
        boldItalic="Inter-SemiBold",
    )
    report.INK = CHARCOAL
    report.PAPER = PAPER
    report.GREEN = ORANGE
    report.GREEN_LT = DEEP_RED
    report.OX = DEEP_RED
    report.AMBER = ORANGE
    report.SLATE = COOL_GREY
    report.MUTE = colors.HexColor("#686765")
    report.LINE = WARM_GREY
    report.SOFT = PAPER
    body = report.styles["body"]
    body.fontName = "Inter"
    body.fontSize = 9.65
    body.leading = 14.25
    body.textColor = CHARCOAL
    for name in ("h1", "h2", "h3"):
        report.styles[name].fontName = "SourceSerif4"
        report.styles[name].textColor = ORANGE
    report.styles["h1"].fontSize = 25
    report.styles["h1"].leading = 29
    report.styles["h2"].fontSize = 16.5
    report.styles["h2"].leading = 20
    report.styles["caption"].fontName = "Inter"
    report.styles["caption"].textColor = colors.HexColor("#686765")
```

- [ ] **Step 4: Implement the original cover geometry, footer and cover story**

Use canvas primitives only: warm-grey upper field, 44 mm orange-red side rail, a charcoal/coral data-line illustration and no logo or photograph. Bind these functions into the imported report module as `cover_page`, `_footer`, `body_page`, `plain_page` and `build_cover`.

```python
def install_page_primitives(report: ModuleType) -> None:
    report.cover_page = consulting_cover_page
    report._footer = consulting_footer
    report.body_page = lambda canvas, doc: consulting_footer(canvas, doc, header=True)
    report.plain_page = lambda canvas, doc: consulting_footer(canvas, doc, header=False)
    report.build_cover = lambda: consulting_cover_story(report)
```

- [ ] **Step 5: Replace KPI and table helpers with editorial variants**

Implement `consulting_kpi_band` as a white typographic strip with a 1.5 pt orange top rule and no card background. Implement `consulting_data_table` with Inter text, warm-grey hairlines, numeric right alignment, no vertical rules and charcoal headers underlined in orange. Assign both helpers to the imported report module.

- [ ] **Step 6: Run all contract tests**

Run: `pytest tests/test_consulting_pdf_report.py -q`  
Expected: all tests pass.

- [ ] **Step 7: Commit the editorial system**

```bash
git add scripts/build_consulting_pdf_report.py tests/test_consulting_pdf_report.py
git commit -m "feat: add consulting report editorial system"
```

---

### Task 3: Temporary consulting chart pack

**Files:**
- Modify: `scripts/build_consulting_pdf_report.py`
- Modify: `tests/test_consulting_pdf_report.py`

**Interfaces:**
- Consumes: `src.visualization.build_executive_graphs`, `src.visualization.build_supplementary_graphs`, processed CSV files and `reports/main_business_analysis_metrics.json`.
- Produces: `EXPECTED_CHARTS: tuple[str, ...]`, `configure_chart_theme(core: ModuleType, supplementary: ModuleType) -> None`, `generate_consulting_charts(base_dir: Path, output_dir: Path, dpi: int = 190) -> set[str]`.

- [ ] **Step 1: Add an exact chart-contract test**

```python
def test_expected_chart_contract_matches_report_exhibits() -> None:
    module = load_builder()
    assert set(module.EXPECTED_CHARTS) == {
        "mrr_arr_growth_trend.png",
        "grr_nrr_retention_trend.png",
        "logo_churn_and_revenue_churn.png",
        "cohort_retention_heatmap.png",
        "logo_churn_by_segment_channel.png",
        "discount_dependency_trend.png",
        "discount_band_vs_forward_churn.png",
        "expansion_quality_mix.png",
        "at_risk_mrr_concentration.png",
        "governance_priority_accounts.png",
        "account_manager_discount_vs_churn.png",
        "churn_risk_score_distribution.png",
        "score_decile_calibration.png",
        "scenario_mrr_trajectories.png",
        "scenario_arr_variance_vs_base.png",
    }
```

- [ ] **Step 2: Run the chart-contract test and confirm it fails**

Run: `pytest tests/test_consulting_pdf_report.py::test_expected_chart_contract_matches_report_exhibits -q`  
Expected: FAIL because `EXPECTED_CHARTS` is not defined.

- [ ] **Step 3: Add the chart map and configure both chart modules**

Set both modules to white backgrounds, charcoal ink, orange-red primary data, coral comparison data, deep-red adverse data, warm-grey gridlines and cool-grey secondary labels. Rebuild palette and scenario dictionaries after replacing scalar constants so functions that resolve globals at runtime use the same approved colors.

- [ ] **Step 4: Generate the nine core and six supplementary charts**

Load the metrics JSON once. Call the exact chart functions required by the report, write them only to `output_dir`, then compare generated filenames to `EXPECTED_CHARTS`; raise `RuntimeError` listing missing charts.

```python
generated = {path.name for path in output_dir.glob("*.png")}
missing = set(EXPECTED_CHARTS) - generated
if missing:
    raise RuntimeError(f"Consulting chart generation missed: {sorted(missing)}")
return generated
```

- [ ] **Step 5: Run a focused chart integration test**

Add a test that calls `generate_consulting_charts(ROOT, tmp_path, dpi=72)` and asserts the exact chart set and a non-zero file size for every image.

Run: `pytest tests/test_consulting_pdf_report.py -k chart -q`  
Expected: two chart tests pass and 15 PNGs exist only under pytest's temporary directory.

- [ ] **Step 6: Commit the chart pack**

```bash
git add scripts/build_consulting_pdf_report.py tests/test_consulting_pdf_report.py
git commit -m "feat: generate consulting report chart pack"
```

---

### Task 4: Atomic PDF build and preservation test

**Files:**
- Modify: `scripts/build_consulting_pdf_report.py`
- Modify: `tests/test_consulting_pdf_report.py`

**Interfaces:**
- Consumes: configured report module and temporary chart pack.
- Produces: `build_consulting_report(base_dir: Path = BASE, output_path: Path = DEFAULT_OUTPUT) -> Path`, `main(argv: list[str] | None = None) -> int`.

- [ ] **Step 1: Add an integration test for a complete PDF and unchanged original**

```python
def test_build_creates_second_pdf_without_touching_existing(tmp_path: Path) -> None:
    module = load_builder()
    before = module.sha256(module.EXISTING_REPORT)
    output = tmp_path / "consulting.pdf"
    result = module.build_consulting_report(ROOT, output)
    assert result == output
    assert output.read_bytes().startswith(b"%PDF-")
    assert output.stat().st_size > 500_000
    assert module.sha256(module.EXISTING_REPORT) == before
```

- [ ] **Step 2: Run the integration test and confirm the missing build interface**

Run: `pytest tests/test_consulting_pdf_report.py::test_build_creates_second_pdf_without_touching_existing -q`  
Expected: FAIL because `build_consulting_report` is not defined.

- [ ] **Step 3: Implement the build pipeline**

Validate inputs and capture the existing report checksum. Use `TemporaryDirectory(dir=BASE / "tmp/pdfs")` for charts and the temporary PDF. Configure chart modules, generate the chart pack, configure the report module, set `report.GRAPHS` to the temporary chart directory, set `report.OUT` to the temporary PDF and clear `report.EXHIBITS`. Call `report.build()`, verify the temporary PDF signature and size, confirm the existing checksum is unchanged, create the final parent directory and use `os.replace(temporary_pdf, output_path)`.

- [ ] **Step 4: Implement the CLI entrypoint**

```python
def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output = build_consulting_report(BASE, args.output.resolve())
    print(f"Consulting report written -> {output.relative_to(BASE)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run the full test file**

Run: `pytest tests/test_consulting_pdf_report.py -q`  
Expected: all tests pass, including a full temporary PDF build.

- [ ] **Step 6: Commit the complete builder**

```bash
git add scripts/build_consulting_pdf_report.py tests/test_consulting_pdf_report.py
git commit -m "feat: build consulting-grade revenue report"
```

---

### Task 5: Generate and audit the final deliverable

**Files:**
- Create: `outputs/reports/revenue_quality_os_consulting_report.pdf`
- Preserve: `outputs/reports/revenue_quality_os_analytical_report.pdf`

**Interfaces:**
- Consumes: completed builder and repository data.
- Produces: final PDF plus command evidence for preservation, typography, completeness and rendering quality.

- [ ] **Step 1: Record the original checksum and build the final PDF**

Run: `shasum -a 256 outputs/reports/revenue_quality_os_analytical_report.pdf`  
Run: `python scripts/build_consulting_pdf_report.py`  
Expected: the final PDF is written to the agreed second filename.

- [ ] **Step 2: Verify PDF structure and embedded fonts**

Run: `pdfinfo outputs/reports/revenue_quality_os_consulting_report.pdf`  
Expected: A4 page size, no encryption, valid metadata and approximately 33 pages.

Run: `pdffonts outputs/reports/revenue_quality_os_consulting_report.pdf`  
Expected: embedded subsets of Inter and Source Serif 4; no missing-font warning.

- [ ] **Step 3: Verify narrative completeness and unfinished-marker absence**

Run: `pdftotext -layout outputs/reports/revenue_quality_os_consulting_report.pdf tmp/pdfs/consulting-report.txt`  
Confirm the title, Executive summary, Context and objectives, Data and methodology, Analytical framework, Findings, Risks, Recommendations and Appendix occur in the extracted text. Search case-insensitively for work-in-progress markers, dummy copy and tool tokens; expected result is no match.

- [ ] **Step 4: Render every page for visual inspection**

Run: `pdftoppm -png -r 150 outputs/reports/revenue_quality_os_consulting_report.pdf tmp/pdfs/consulting-page`  
Inspect the cover, contents, executive summary, every page containing a chart or table, each section transition and the final appendix page. Fix any clipping, collisions, weak contrast, stranded headings, blank pages, illegible chart labels or inconsistent footers, rebuild and repeat until no defect remains.

- [ ] **Step 5: Prove source preservation and clean the temporary QA area**

Re-run the original-report checksum and compare it with Step 1. List `outputs/reports/` and confirm it contains the original plus the new final report. Remove `tmp/pdfs/consulting-page-*.png` and `tmp/pdfs/consulting-report.txt` after inspection; do not remove unrelated user files.

- [ ] **Step 6: Commit the final artifact**

```bash
git add outputs/reports/revenue_quality_os_consulting_report.pdf
git commit -m "docs: add consulting-grade revenue quality report"
```

- [ ] **Step 7: Run final verification**

Run: `pytest tests/test_consulting_pdf_report.py -q`  
Run: `git diff --check`  
Expected: tests pass and no whitespace errors are reported for the new implementation files.

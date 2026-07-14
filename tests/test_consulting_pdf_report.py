from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from PIL import Image

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


def test_configure_report_theme_applies_approved_typography() -> None:
    module = load_builder()
    report = module.load_report_module()
    module.configure_report_theme(report)
    assert report.styles["body"].fontName == "Inter"
    assert report.styles["h1"].fontName == "SourceSerif4"
    assert report.styles["h1"].textColor.hexval().lower() == "0xf04424"
    assert report.styles["caption"].fontName == "Inter"


def test_install_report_primitives_replaces_cover_kpis_and_tables() -> None:
    module = load_builder()
    report = module.load_report_module()
    module.configure_report_theme(report)
    module.install_report_primitives(report)

    cover = report.build_cover()
    cover_text = " ".join(item.getPlainText() for item in cover if hasattr(item, "getPlainText"))
    assert "Revenue Quality" in cover_text
    assert report.cover_page is module.consulting_cover_page

    kpis = report.kpi_band([("$114.3M", "ARR run-rate"), ("99.82%", "NRR")])
    assert kpis._ncols == 2

    table = report.data_table(["Metric", "Value"], [["NRR", "99.82%"]], [200, 100])
    assert table._nrows == 2
    assert table._ncols == 2


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


def test_generate_consulting_charts_writes_exact_chart_pack(tmp_path: Path) -> None:
    module = load_builder()
    generated = module.generate_consulting_charts(ROOT, tmp_path, dpi=72)
    assert generated == set(module.EXPECTED_CHARTS)
    assert all((tmp_path / name).stat().st_size > 1_000 for name in generated)


def test_heatmap_does_not_reintroduce_green_brand_colors(tmp_path: Path) -> None:
    module = load_builder()
    module.generate_consulting_charts(ROOT, tmp_path, dpi=72)
    with Image.open(tmp_path / "cohort_retention_heatmap.png") as image:
        pixels = list(image.convert("RGB").get_flattened_data())
    green_dominant = sum(g > r * 1.12 and g > b * 1.06 and g > 90 for r, g, b in pixels)
    assert green_dominant / len(pixels) < 0.001

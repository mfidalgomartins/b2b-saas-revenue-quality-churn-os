from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

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


def test_consulting_toc_matches_final_editorial_pagination() -> None:
    module = load_builder()
    assert module.CONSULTING_TOC_ENTRIES == (
        ("1", "Executive summary", "3"),
        ("2", "Context and objectives", "4"),
        ("3", "Data and methodology", "5"),
        ("4", "Analytical framework", "7"),
        ("5", "Findings", "8"),
        ("5.1", "Revenue scale and the quality of growth", "8"),
        ("5.2", "Retention: gross, net and the cohort view", "10"),
        ("5.3", "Where churn concentrates", "12"),
        ("5.4", "Discount intensity and realized pricing", "13"),
        ("5.5", "Expansion quality", "16"),
        ("5.6", "Account-level concentration and at-risk MRR", "17"),
        ("5.7", "The churn-risk scoring system", "20"),
        ("5.8", "Forecast and scenario analysis", "22"),
        ("5.9", "Probabilistic forecast calibration", "25"),
        ("5.10", "Intervention effectiveness and ROI", "25"),
        ("6", "Risks, limitations and caveats", "26"),
        ("7", "Recommendations and action priorities", "27"),
        ("8", "Appendix", "29"),
    )


def test_install_copy_overrides_names_the_consulting_builder() -> None:
    module = load_builder()
    captured: list[str] = []
    report = SimpleNamespace(P=lambda text, _style: captured.append(text) or text)

    module.install_copy_overrides(report)
    report.P("Built by scripts/build_pdf_report.py.", "body")

    assert captured == ["Built by scripts/build_consulting_pdf_report.py."]


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
    assert report.styles["cover_sub"].rightIndent >= module.cm * 3
    assert all(report.styles[name].keepWithNext for name in ("h1", "h2", "h3"))
    assert all(not report.styles[name].allowWidows for name in ("body", "body_first", "lead"))


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


def test_install_report_primitives_adds_editorial_exhibit_rule(tmp_path: Path) -> None:
    module = load_builder()
    report = module.load_report_module()
    module.configure_report_theme(report)
    module.install_report_primitives(report)
    report.GRAPHS = tmp_path
    report.EXHIBITS.clear()
    Image.new("RGB", (120, 60), "white").save(tmp_path / "sample.png")

    exhibit = report.figure("sample.png", "Sample chart", "Sample caption")
    assert exhibit[0]._content[0].__class__.__name__ == "HRFlowable"
    assert report.EXHIBITS == [(1, "Sample chart")]


def test_install_document_metadata_replaces_legacy_report_identity() -> None:
    module = load_builder()
    captured: dict[str, str] = {}

    def fake_template(*_args, **kwargs):
        captured.update(kwargs)
        return object()

    report = SimpleNamespace(BaseDocTemplate=fake_template)
    module.install_document_metadata(report)
    report.BaseDocTemplate("report.pdf", title="Legacy title", subject="Legacy subject")

    assert captured["title"] == module.CONSULTING_TITLE
    assert captured["subject"] == module.CONSULTING_SUBJECT


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


def test_heatmap_caption_matches_consulting_palette() -> None:
    module = load_builder()
    caption = module.CHART_CAPTION_OVERRIDES["cohort_retention_heatmap.png"]
    assert "Green cells" not in caption
    assert "deep-red" in caption


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


def test_build_creates_second_pdf_without_touching_existing(tmp_path: Path) -> None:
    module = load_builder()
    before = module.sha256(module.EXISTING_REPORT)
    output = tmp_path / "consulting.pdf"
    result = module.build_consulting_report(ROOT, output)
    assert result == output
    assert output.read_bytes().startswith(b"%PDF-")
    assert output.stat().st_size > 500_000
    assert module.sha256(module.EXISTING_REPORT) == before


def test_main_writes_requested_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    module = load_builder()
    output = tmp_path / "requested.pdf"
    assert module.main(["--output", str(output)]) == 0
    assert output.is_file()
    assert "Consulting report written" in capsys.readouterr().out

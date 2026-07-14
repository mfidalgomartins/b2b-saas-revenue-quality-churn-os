"""Build the consulting-grade Revenue Quality Operating System report."""

from __future__ import annotations

import argparse
import hashlib
import sys
from importlib import import_module
from pathlib import Path
from types import ModuleType

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import Spacer, Table, TableStyle

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

CHARCOAL = colors.HexColor("#252323")
PAPER = colors.HexColor("#F5F4F1")
ORANGE = colors.HexColor("#F04424")
DEEP_RED = colors.HexColor("#AD2B1F")
CORAL = colors.HexColor("#F49A80")
WARM_GREY = colors.HexColor("#D9D7D2")
COOL_GREY = colors.HexColor("#9DA3A6")


def sha256(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_inputs(base_dir: Path) -> None:
    """Fail fast when a source document, dataset or font is unavailable."""
    for relative_path in REQUIRED_INPUTS:
        path = base_dir / relative_path
        if not path.is_file():
            raise FileNotFoundError(f"Required report input is missing: {path}")


def load_report_module() -> ModuleType:
    """Load the validated analytical report builder without modifying its source."""
    if str(BASE) not in sys.path:
        sys.path.insert(0, str(BASE))
    return import_module("scripts.build_pdf_report")


def configure_report_theme(report: ModuleType) -> None:
    """Inject the approved consulting-grade type and color system."""
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

    report.styles["body_first"].fontName = "Inter"
    report.styles["lead"].fontName = "Inter"
    report.styles["lead"].fontSize = 10.6
    report.styles["lead"].leading = 15.4
    report.styles["lead"].textColor = CHARCOAL

    for name in ("h1", "h2", "h3"):
        report.styles[name].fontName = "SourceSerif4"
        report.styles[name].textColor = ORANGE
    report.styles["h1"].fontSize = 25
    report.styles["h1"].leading = 29
    report.styles["h2"].fontSize = 16.5
    report.styles["h2"].leading = 20
    report.styles["h3"].fontSize = 12.5
    report.styles["h3"].leading = 15.5
    report.styles["h1num"].fontName = "Inter"
    report.styles["h1num"].textColor = ORANGE
    report.styles["caption"].fontName = "Inter"
    report.styles["caption"].fontSize = 7.7
    report.styles["caption"].leading = 10.1
    report.styles["caption"].textColor = colors.HexColor("#686765")
    report.styles["figtitle"].fontName = "Inter-SemiBold"
    report.styles["figtitle"].fontSize = 8.7
    report.styles["figtitle"].textColor = CHARCOAL
    report.styles["pull"].fontName = "SourceSerif4-Italic"
    report.styles["pull"].textColor = DEEP_RED
    report.styles["kpi_num"].fontName = "SourceSerif4"
    report.styles["kpi_num"].fontSize = 20
    report.styles["kpi_num"].leading = 22
    report.styles["kpi_num"].textColor = CHARCOAL
    report.styles["kpi_lab"].fontName = "Inter"
    report.styles["kpi_lab"].fontSize = 7.2
    report.styles["kpi_lab"].textColor = colors.HexColor("#686765")
    report.styles["tbl"].fontName = "Inter"
    report.styles["tbl"].fontSize = 7.8
    report.styles["tbl"].leading = 10.1
    report.styles["tbl"].textColor = CHARCOAL
    report.styles["tbl_b"].fontName = "Inter-SemiBold"
    report.styles["tbl_b"].fontSize = 7.8
    report.styles["tbl_b"].leading = 10.1
    report.styles["tbl_b"].textColor = CHARCOAL
    report.styles["tbl_r"].fontName = "Inter"
    report.styles["toc_l"].fontName = "Inter"
    report.styles["toc_n"].fontName = "Inter-SemiBold"
    report.styles["toc_n"].textColor = ORANGE
    report.styles["note"].fontName = "SourceSerif4-Italic"
    report.styles["cover_kicker"].fontName = "Inter-SemiBold"
    report.styles["cover_kicker"].fontSize = 9.2
    report.styles["cover_kicker"].tracking = 1.7
    report.styles["cover_kicker"].textColor = CHARCOAL
    report.styles["cover_title"].fontName = "SourceSerif4"
    report.styles["cover_title"].fontSize = 38
    report.styles["cover_title"].leading = 41
    report.styles["cover_title"].textColor = CHARCOAL
    report.styles["cover_sub"].fontName = "Inter-SemiBold"
    report.styles["cover_sub"].fontSize = 11.1
    report.styles["cover_sub"].leading = 15.2
    report.styles["cover_sub"].textColor = CHARCOAL
    report.styles["cover_meta"].fontName = "Inter"
    report.styles["cover_meta"].textColor = CHARCOAL
    report.styles["cover_meta_b"].fontName = "Inter-SemiBold"
    report.styles["cover_meta_b"].textColor = CHARCOAL
    report.RUNNING_TITLE = "Revenue Quality Operating System"
    report.DOC_REF = "Executive Report 2026"


def consulting_cover_page(canvas, _doc) -> None:
    """Draw an original asymmetric cover using only geometric data motifs."""
    page_w, page_h = A4
    canvas.saveState()
    canvas.setFillColor(colors.white)
    canvas.rect(0, 0, page_w, page_h, fill=1, stroke=0)

    field_bottom = 12.0 * cm
    canvas.setFillColor(PAPER)
    canvas.rect(0, field_bottom, page_w, page_h - field_bottom, fill=1, stroke=0)
    canvas.setFillColor(ORANGE)
    canvas.rect(page_w - 3.25 * cm, field_bottom, 3.25 * cm, page_h - field_bottom, fill=1, stroke=0)

    # Data-inspired linework: original geometry, not a copied brand device.
    plot_x = [1.25, 3.4, 5.2, 7.1, 9.0, 11.2, 13.6, 15.7]
    plot_y = [12.8, 13.2, 12.95, 13.85, 14.15, 13.75, 14.7, 15.1]
    canvas.setStrokeColor(CHARCOAL)
    canvas.setLineWidth(1.15)
    path = canvas.beginPath()
    path.moveTo(plot_x[0] * cm, plot_y[0] * cm)
    for x, y in zip(plot_x[1:], plot_y[1:], strict=True):
        path.lineTo(x * cm, y * cm)
    canvas.drawPath(path, stroke=1, fill=0)
    for index, (x, y) in enumerate(zip(plot_x, plot_y, strict=True)):
        canvas.setFillColor(ORANGE if index in {3, 6, 7} else colors.white)
        canvas.setStrokeColor(CHARCOAL)
        canvas.circle(x * cm, y * cm, 0.10 * cm, fill=1, stroke=1)

    canvas.setFillColor(CORAL)
    canvas.rect(1.25 * cm, 12.25 * cm, 0.5 * cm, 0.5 * cm, fill=1, stroke=0)
    canvas.setFillColor(DEEP_RED)
    canvas.rect(15.45 * cm, 15.35 * cm, 0.5 * cm, 0.5 * cm, fill=1, stroke=0)
    canvas.restoreState()


def consulting_footer(canvas, doc, header: bool = True) -> None:  # noqa: ARG001
    """Draw a quiet bottom navigation line matching the reference's restraint."""
    page_w, _page_h = A4
    canvas.saveState()
    canvas.setStrokeColor(WARM_GREY)
    canvas.setLineWidth(0.45)
    canvas.line(2.0 * cm, 1.55 * cm, page_w - 2.0 * cm, 1.55 * cm)
    canvas.setFont("Inter", 7.1)
    canvas.setFillColor(colors.HexColor("#686765"))
    canvas.drawString(2.0 * cm, 1.08 * cm, "Revenue Quality Operating System")
    canvas.setFont("Inter-SemiBold", 7.1)
    canvas.setFillColor(CHARCOAL)
    canvas.drawRightString(page_w - 2.0 * cm, 1.08 * cm, str(doc.page))
    canvas.restoreState()


def consulting_cover_story(report: ModuleType) -> list:
    """Return the cover's type hierarchy and report metadata."""
    story = [
        Spacer(1, 1.75 * cm),
        report.P("REVENUE OPERATIONS ANALYTICS", "cover_kicker"),
        Spacer(1, 0.45 * cm),
        report.P("Revenue Quality<br/>Operating System", "cover_title"),
        Spacer(1, 0.45 * cm),
        report.P(
            "Retention, discount discipline, account-level risk and a six-month forward view "
            "for a $114M ARR B2B SaaS portfolio.",
            "cover_sub",
        ),
        Spacer(1, 8.25 * cm),
    ]
    metadata = [
        [report.P("Prepared by", "cover_meta"), report.P("Revenue Operations Analytics", "cover_meta_b")],
        [
            report.P("Analysis window", "cover_meta"),
            report.P("March 2023 to February 2026 (36 months)", "cover_meta_b"),
        ],
        [
            report.P("Portfolio", "cover_meta"),
            report.P("4,500 accounts | 40 account managers | 8 plans", "cover_meta_b"),
        ],
    ]
    table = Table(metadata, colWidths=[3.6 * cm, report.CONTENT_W - 3.6 * cm])
    table.setStyle(
        TableStyle(
            [
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("LINEABOVE", (0, 0), (-1, 0), 1.2, ORANGE),
                ("LINEBELOW", (0, 0), (-1, -1), 0.35, WARM_GREY),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(table)
    return story


def consulting_kpi_band(report: ModuleType, items: list[tuple[str, str]]) -> Table:
    """Render headline metrics as editorial statements instead of dashboard cards."""
    column_width = report.CONTENT_W / len(items)
    cells = []
    for value, label in items:
        cell = Table(
            [[report.P(value, "kpi_num")], [report.P(label, "kpi_lab")]],
            colWidths=[column_width - 8],
        )
        cell.setStyle(
            TableStyle(
                [
                    ("LINEABOVE", (0, 0), (-1, 0), 1.5, ORANGE),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, 0), 7),
                    ("BOTTOMPADDING", (0, 1), (-1, 1), 6),
                ]
            )
        )
        cells.append(cell)
    table = Table([cells], colWidths=[column_width] * len(items))
    table.setStyle(
        TableStyle(
            [
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return table


def consulting_data_table(
    report: ModuleType,
    header: list[str],
    rows: list[list[str]],
    widths: list[float],
    align_right_from: int = 1,
) -> Table:
    """Render a publication table with hairlines and disciplined alignment."""
    head = [report.P(value, "tbl_b") for value in header]
    body = []
    for row in rows:
        body.append(
            [
                report.P(str(value), "tbl_r" if index >= align_right_from else "tbl")
                for index, value in enumerate(row)
            ]
        )
    table = Table([head] + body, colWidths=widths, repeatRows=1)
    commands = [
        ("LINEABOVE", (0, 0), (-1, 0), 1.25, ORANGE),
        ("LINEBELOW", (0, 0), (-1, 0), 0.65, CHARCOAL),
        ("LINEBELOW", (0, 1), (-1, -1), 0.35, WARM_GREY),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (align_right_from, 0), (-1, -1), "RIGHT"),
    ]
    for row_index in range(2, len(body) + 1, 2):
        commands.append(("BACKGROUND", (0, row_index), (-1, row_index), PAPER))
    table.setStyle(TableStyle(commands))
    return table


def install_report_primitives(report: ModuleType) -> None:
    """Bind editorial page, KPI and table helpers to the existing content builder."""
    report.cover_page = consulting_cover_page
    report._footer = consulting_footer
    report.body_page = lambda canvas, doc: consulting_footer(canvas, doc, header=True)
    report.plain_page = lambda canvas, doc: consulting_footer(canvas, doc, header=False)
    report.build_cover = lambda: consulting_cover_story(report)
    report.kpi_band = lambda items: consulting_kpi_band(report, items)
    report.data_table = lambda header, rows, widths, align_right_from=1: consulting_data_table(
        report,
        header,
        rows,
        widths,
        align_right_from,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the report output path."""
    parser = argparse.ArgumentParser(description="Build the consulting-grade revenue quality PDF.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)

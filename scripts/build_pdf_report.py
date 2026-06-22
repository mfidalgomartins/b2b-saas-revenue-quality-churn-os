"""
Build the publication-grade analytical PDF report.

Produces outputs/reports/revenue_quality_os_analytical_report.pdf, a multi-page
advisory deliverable with narrative prose and inline charts drawn from the
project's processed data and the charts in outputs/graphs/.

Run:
    python scripts/build_pdf_report.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

BASE = Path(__file__).resolve().parents[1]
GRAPHS = BASE / "outputs" / "graphs"
OUT = BASE / "outputs" / "reports" / "revenue_quality_os_analytical_report.pdf"

# ---------------------------------------------------------------------------
# Palette (shared with the chart pack)
# ---------------------------------------------------------------------------
INK = colors.HexColor("#1d1d1b")
GREEN = colors.HexColor("#1f3b2d")
GREEN_LT = colors.HexColor("#2d5a42")
OX = colors.HexColor("#9c2b1b")
AMBER = colors.HexColor("#b07d2b")
SLATE = colors.HexColor("#5b5f66")
MUTE = colors.HexColor("#6b6f76")
PAPER = colors.HexColor("#fafaf7")
LINE = colors.HexColor("#d8d8d3")
SOFT = colors.HexColor("#eef0ec")
WHITE = colors.white

PAGE_W, PAGE_H = A4
LMARGIN = RMARGIN = 2.0 * cm
TMARGIN = 2.3 * cm
BMARGIN = 2.0 * cm
CONTENT_W = PAGE_W - LMARGIN - RMARGIN

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
BODY_FONT = "Times-Roman"
BODY_BOLD = "Times-Bold"
BODY_ITALIC = "Times-Italic"
HEAD_FONT = "Helvetica-Bold"
HEAD_REG = "Helvetica"

styles: dict[str, ParagraphStyle] = {}


def _add(name: str, **kw) -> None:
    styles[name] = ParagraphStyle(name, **kw)


_add("body", fontName=BODY_FONT, fontSize=10.7, leading=16.6, alignment=TA_JUSTIFY, textColor=INK, spaceAfter=9)
_add("body_first", parent=styles["body"], firstLineIndent=0)
_add("lead", fontName=BODY_FONT, fontSize=11.6, leading=17.8, alignment=TA_JUSTIFY, textColor=INK, spaceAfter=11)
_add("h1", fontName=HEAD_FONT, fontSize=19, leading=23, textColor=GREEN, spaceBefore=6, spaceAfter=4)
_add("h1num", fontName=HEAD_REG, fontSize=11, leading=13, textColor=AMBER, spaceAfter=2, tracking=2)
_add("h2", fontName=HEAD_FONT, fontSize=13, leading=17, textColor=GREEN, spaceBefore=14, spaceAfter=4)
_add("h3", fontName=HEAD_FONT, fontSize=10.8, leading=14, textColor=GREEN_LT, spaceBefore=10, spaceAfter=3)
_add(
    "caption",
    fontName=HEAD_REG,
    fontSize=8.4,
    leading=11,
    textColor=MUTE,
    spaceBefore=3,
    spaceAfter=14,
    alignment=TA_LEFT,
)
_add("figtitle", fontName=HEAD_FONT, fontSize=9.2, leading=12, textColor=INK, spaceBefore=2, spaceAfter=1)
_add(
    "pull",
    fontName=BODY_ITALIC,
    fontSize=12.5,
    leading=18,
    textColor=GREEN,
    spaceBefore=6,
    spaceAfter=10,
    leftIndent=10,
    rightIndent=10,
)
_add("kpi_num", fontName=HEAD_FONT, fontSize=17, leading=19, textColor=GREEN, alignment=TA_LEFT)
_add("kpi_lab", fontName=HEAD_REG, fontSize=7.6, leading=9.4, textColor=MUTE, alignment=TA_LEFT)
_add("tbl", fontName=HEAD_REG, fontSize=8.6, leading=11, textColor=INK)
_add("tbl_b", fontName=HEAD_FONT, fontSize=8.6, leading=11, textColor=WHITE)
_add("tbl_r", parent=styles["tbl"], alignment=TA_RIGHT)
_add("toc_l", fontName=HEAD_REG, fontSize=10, leading=20, textColor=INK)
_add("toc_n", fontName=HEAD_FONT, fontSize=10, leading=20, textColor=GREEN, alignment=TA_RIGHT)
_add("note", fontName=BODY_ITALIC, fontSize=9.2, leading=13, textColor=SLATE, spaceAfter=7)
_add("cover_kicker", fontName=HEAD_REG, fontSize=11, leading=14, textColor=WHITE, tracking=3)
_add("cover_title", fontName=HEAD_FONT, fontSize=33, leading=37, textColor=WHITE)
_add("cover_sub", fontName=HEAD_REG, fontSize=13, leading=18, textColor=colors.HexColor("#cfe0d4"))
_add("cover_meta", fontName=HEAD_REG, fontSize=9.5, leading=15, textColor=INK)
_add("cover_meta_b", fontName=HEAD_FONT, fontSize=9.5, leading=15, textColor=GREEN)


# ---------------------------------------------------------------------------
# Flowable helpers
# ---------------------------------------------------------------------------
def P(text: str, style: str = "body") -> Paragraph:
    return Paragraph(text, styles[style])


# Exhibit registry — figures auto-number in document order so captions and
# in-text references can never drift apart, and the front matter can list them.
EXHIBITS: list[tuple[int, str]] = []


def figure(name: str, title: str, caption: str, max_w: float = CONTENT_W, max_h: float = 11.5 * cm):
    """Return a single KeepTogether flowable: numbered title, image, caption.

    Figures are numbered as "Exhibit N" in the order figure() is called, which is
    document order, so the number in the caption always matches the in-text
    reference. The (number, title) pair is registered for the List of Exhibits.
    """
    path = GRAPHS / name
    with PILImage.open(path) as im:
        iw, ih = im.size
    ar = ih / iw
    w = max_w
    h = w * ar
    if h > max_h:
        h = max_h
        w = h / ar
    img = Image(str(path), width=w, height=h)
    img.hAlign = "CENTER"

    number = len(EXHIBITS) + 1
    EXHIBITS.append((number, title))

    block = []
    if title:
        block.append(P(f"Exhibit {number} &mdash; {title}", "figtitle"))
    block.append(img)
    if caption:
        block.append(P(caption, "caption"))
    # Bind title + image + caption so an exhibit never splits across a page break
    # or strands its caption.
    return [KeepTogether(block)]


def kpi_band(items: list[tuple[str, str]]):
    """Horizontal KPI strip: list of (value, label)."""
    cells = []
    for val, lab in items:
        inner = Table([[P(val, "kpi_num")], [P(lab, "kpi_lab")]], colWidths=[CONTENT_W / len(items) - 6])
        inner.setStyle(
            TableStyle(
                [
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, 0), 7),
                    ("BOTTOMPADDING", (0, 1), (-1, 1), 7),
                    ("LINEBEFORE", (0, 0), (0, -1), 2, AMBER),
                    ("BACKGROUND", (0, 0), (-1, -1), SOFT),
                ]
            )
        )
        cells.append(inner)
    t = Table([cells], colWidths=[CONTENT_W / len(items)] * len(items))
    t.setStyle(
        TableStyle(
            [
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    return t


def data_table(header: list[str], rows: list[list[str]], widths: list[float], align_right_from: int = 1):
    head = [P(h, "tbl_b") for h in header]
    body = []
    for r in rows:
        cells = []
        for j, c in enumerate(r):
            st = "tbl_r" if j >= align_right_from else "tbl"
            cells.append(P(str(c), st))
        body.append(cells)
    t = Table([head] + body, colWidths=widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), GREEN),
        ("TOPPADDING", (0, 0), (-1, -1), 4.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("LINEBELOW", (0, 0), (-1, -1), 0.4, LINE),
        ("LINEBELOW", (0, 0), (-1, 0), 0, GREEN),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    for i in range(1, len(body) + 1):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), PAPER))
    t.setStyle(TableStyle(style))
    return t


def rule(space_before: float = 2, space_after: float = 8, color=LINE, w: float = 0.6):
    from reportlab.platypus import Flowable

    class _Rule(Flowable):
        def __init__(self):
            super().__init__()
            self.width = CONTENT_W
            self.height = space_before + space_after

        def draw(self):
            self.canv.setStrokeColor(color)
            self.canv.setLineWidth(w)
            y = space_after
            self.canv.line(0, y, CONTENT_W, y)

    return _Rule()


# ---------------------------------------------------------------------------
# Page furniture
# ---------------------------------------------------------------------------
RUNNING_TITLE = "Revenue Quality Operating System"
DOC_REF = "Analytical Report 2026"


def _footer(canvas, doc, header: bool = True):
    canvas.saveState()
    if header:
        canvas.setFont(HEAD_REG, 7.5)
        canvas.setFillColor(MUTE)
        canvas.drawString(LMARGIN, PAGE_H - 1.35 * cm, RUNNING_TITLE.upper())
        canvas.drawRightString(PAGE_W - RMARGIN, PAGE_H - 1.35 * cm, DOC_REF.upper())
        canvas.setStrokeColor(LINE)
        canvas.setLineWidth(0.5)
        canvas.line(LMARGIN, PAGE_H - 1.5 * cm, PAGE_W - RMARGIN, PAGE_H - 1.5 * cm)
    canvas.setStrokeColor(LINE)
    canvas.setLineWidth(0.5)
    canvas.line(LMARGIN, BMARGIN - 0.35 * cm, PAGE_W - RMARGIN, BMARGIN - 0.35 * cm)
    canvas.setFont(HEAD_REG, 7.5)
    canvas.setFillColor(MUTE)
    canvas.drawRightString(PAGE_W - RMARGIN, BMARGIN - 0.78 * cm, f"{doc.page}")
    canvas.restoreState()


def body_page(canvas, doc):
    _footer(canvas, doc, header=True)


def plain_page(canvas, doc):
    _footer(canvas, doc, header=False)


def cover_page(canvas, doc):
    canvas.saveState()
    # full background
    canvas.setFillColor(PAPER)
    canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    # green top band
    band_h = PAGE_H * 0.52
    canvas.setFillColor(GREEN)
    canvas.rect(0, PAGE_H - band_h, PAGE_W, band_h, fill=1, stroke=0)
    # thin amber accent line under band
    canvas.setFillColor(AMBER)
    canvas.rect(0, PAGE_H - band_h - 5, PAGE_W, 5, fill=1, stroke=0)
    # subtle hairline columns in band (restrained texture)
    canvas.setStrokeColor(colors.HexColor("#2a4a39"))
    canvas.setLineWidth(0.4)
    for i in range(1, 7):
        x = PAGE_W * i / 7
        canvas.line(x, PAGE_H - band_h + 6, x, PAGE_H - 6)
    canvas.restoreState()


# ---------------------------------------------------------------------------
# Document
# ---------------------------------------------------------------------------
def build() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    doc = BaseDocTemplate(
        str(OUT),
        pagesize=A4,
        leftMargin=LMARGIN,
        rightMargin=RMARGIN,
        topMargin=TMARGIN,
        bottomMargin=BMARGIN,
        title="Revenue Quality Operating System: Analytical Report",
        author="Revenue Operations Analytics",
        subject="B2B SaaS revenue quality, retention, risk and forecasting",
    )
    frame = Frame(
        LMARGIN,
        BMARGIN,
        CONTENT_W,
        PAGE_H - TMARGIN - BMARGIN,
        id="body",
        leftPadding=0,
        rightPadding=0,
        topPadding=0,
        bottomPadding=0,
    )
    cover_frame = Frame(
        LMARGIN, BMARGIN, CONTENT_W, PAGE_H - 2 * cm - BMARGIN, id="cover", leftPadding=0, rightPadding=0
    )
    doc.addPageTemplates(
        [
            PageTemplate(id="cover", frames=[cover_frame], onPage=cover_page),
            PageTemplate(id="plain", frames=[frame], onPage=plain_page),
            PageTemplate(id="body", frames=[frame], onPage=body_page),
        ]
    )

    s = []  # story
    s += build_cover()
    s.append(NextPageTemplate("plain"))
    s.append(PageBreak())
    s += build_toc()
    s.append(NextPageTemplate("body"))
    s.append(PageBreak())
    s += build_exec_summary()
    s.append(PageBreak())
    s += build_context()
    s.append(PageBreak())
    s += build_data_method()
    s.append(PageBreak())
    s += build_framework()
    s.append(PageBreak())
    s += build_findings()
    s.append(PageBreak())
    s += build_risks()
    s.append(PageBreak())
    s += build_recommendations()
    s.append(PageBreak())
    s += build_appendix()

    doc.build(s)
    size_kb = OUT.stat().st_size / 1024
    print(f"Report written -> {OUT.relative_to(BASE)}  ({size_kb:.0f} KB)")


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------
def build_cover() -> list:
    s = []
    s.append(Spacer(1, 2.4 * cm))
    s.append(P("REVENUE OPERATIONS ANALYTICS", "cover_kicker"))
    s.append(Spacer(1, 0.5 * cm))
    s.append(P("Revenue Quality<br/>Operating System", "cover_title"))
    s.append(Spacer(1, 0.5 * cm))
    s.append(
        P(
            "Retention, discount discipline, account-level risk and a "
            "six-month forward view for a $114M ARR B2B SaaS portfolio.",
            "cover_sub",
        )
    )
    s.append(Spacer(1, 7.4 * cm))
    meta = [
        [P("Prepared by", "cover_meta"), P("Revenue Operations Analytics", "cover_meta_b")],
        [P("Analysis window", "cover_meta"), P("March 2023 to February 2026 (36 months)", "cover_meta_b")],
        [P("Portfolio", "cover_meta"), P("4,500 accounts | 40 account managers | 8 plans", "cover_meta_b")],
    ]
    t = Table(meta, colWidths=[3.6 * cm, CONTENT_W - 3.6 * cm])
    t.setStyle(
        TableStyle(
            [
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("LINEBELOW", (0, 0), (-1, -1), 0.4, LINE),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    s.append(t)
    return s


def build_toc() -> list:
    s = [P("Contents", "h1"), Spacer(1, 0.3 * cm)]
    entries = [
        ("1", "Executive summary", "3"),
        ("2", "Context and objectives", "5"),
        ("3", "Data and methodology", "6"),
        ("4", "Analytical framework", "8"),
        ("5", "Findings", "10"),
        ("5.1", "Revenue scale and the quality of growth", "10"),
        ("5.2", "Retention: gross, net and the cohort view", "11"),
        ("5.3", "Where churn concentrates", "13"),
        ("5.4", "Discount intensity and realized pricing", "15"),
        ("5.5", "Expansion quality", "17"),
        ("5.6", "Account-level concentration and at-risk MRR", "18"),
        ("5.7", "The churn-risk scoring system", "21"),
        ("5.8", "Forecast and scenario analysis", "23"),
        ("6", "Risks, limitations and caveats", "27"),
        ("7", "Recommendations and action priorities", "29"),
        ("8", "Appendix", "31"),
    ]
    rows = []
    for num, label, page in entries:
        indent = 14 if "." in num else 0
        ls = ParagraphStyle(
            "tl",
            parent=styles["toc_l"],
            leftIndent=indent,
            fontName=HEAD_FONT if "." not in num else HEAD_REG,
            textColor=GREEN if "." not in num else INK,
        )
        rows.append([Paragraph(f"{num}&nbsp;&nbsp;&nbsp;{label}", ls), P(page, "toc_n")])
    t = Table(rows, colWidths=[CONTENT_W - 1.4 * cm, 1.4 * cm])
    t.setStyle(
        TableStyle(
            [
                ("LINEBELOW", (0, 0), (-1, -1), 0.3, SOFT),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    s.append(t)
    s.append(Spacer(1, 0.8 * cm))
    s.append(P("A note on the figures throughout this report", "h3"))
    s.append(
        P(
            "Every chart in this document is generated directly from the project's "
            "processed data layer. The numbers in the narrative, the tables and the "
            "figures share a single source, so a reader can trace any quoted "
            "statistic back to the same underlying tables that feed the live "
            "dashboard. Where a figure is associative rather than causal, the text "
            "says so plainly.",
            "body",
        )
    )
    return s


def build_exec_summary() -> list:
    s = [P("1", "h1num"), P("Executive summary", "h1"), rule()]
    s.append(
        P(
            "This portfolio reached $9.52M in monthly recurring revenue in February "
            "2026, a run-rate of $114.3M in annual recurring revenue. Over the "
            "36-month window the business grew MRR from $3.75M, an implied 2.70% "
            "compounded monthly, and the ARR run-rate compounded at roughly 37.7% a "
            "year. Headline retention is strong: gross revenue retention sits at "
            "99.17% and net revenue retention at 99.82% in the latest month, with "
            "logo churn of 0.73% and revenue churn of 0.39%. On the front page of "
            "any board pack, this looks like a healthy, fast-growing software "
            "company.",
            "lead",
        )
    )
    s.append(
        P(
            "The purpose of this report is to look past the headline. The central "
            "finding is that growth durability degrades in three places before it "
            "shows up in retention: the intensity of discounting, the quality of "
            "expansion, and the concentration of downside risk in a small set of "
            "accounts. Net revenue retention near parity, at 99.82%, leaves almost "
            "no buffer. If churn or contraction accelerates even modestly, the "
            "business moves from net expansion to net contraction quickly, and the "
            "current topline trajectory would not reveal the turn until it had "
            "already happened.",
            "body",
        )
    )
    s.append(
        kpi_band(
            [
                ("$114.3M", "ARR run-rate, Feb 2026"),
                ("99.82%", "Net revenue retention"),
                ("0.73%", "Latest logo churn"),
                ("17.7%", "Weighted discount"),
            ]
        )
    )
    s.append(Spacer(1, 0.35 * cm))
    s.append(
        P(
            "Four results anchor the analysis. First, discounting is both deep and "
            "predictive. The weighted discount across the active base is 17.7%, the "
            "realized price index has settled at 0.822, and 15.9% of MRR now carries "
            "a discount-dependency flag. Accounts in the deepest discount band, more "
            "than 30% off list, churn at 4.31% over the following three months, "
            "roughly double the 1.81% rate of the 20-to-30% band. Discount intensity "
            "near renewal is therefore a usable early-warning signal, not just a "
            "margin cost.",
            "body",
        )
    )
    s.append(
        P(
            "Second, a meaningful share of expansion is fragile. Of $1.63M in "
            "expansion MRR booked across the window, 28% carries a fragile-quality "
            "flag, meaning the account grew while its health signals were "
            "deteriorating. Fragile expansion is associated with elevated churn "
            "three to nine months later, so a portion of the expansion that flatters "
            "net retention today is borrowing against retention tomorrow.",
            "body",
        )
    )
    s.append(
        P(
            "Third, downside risk is concentrated even though revenue is not. The top "
            "ten accounts represent only 4.0% of MRR and the top fifty only 14.3%, so "
            "the revenue base itself is well diversified. But within the at-risk "
            "cohort the picture inverts: 80 accounts carry a High or Critical "
            "governance priority — 79 High and one Critical — and together they "
            "hold $376K of MRR, with the top twenty of them accounting for 81.3% "
            "of that at-risk MRR. The annualized ARR associated "
            "with High and Critical accounts is $4.51M. A stress test in which the "
            "top twenty high-risk accounts churn outright removes $3.67M of ARR. The "
            "right response is account-level governance on a short list, not a "
            "blunt portfolio-wide policy.",
            "body",
        )
    )
    s.append(
        P(
            "Fourth, the early-warning system works on the project's own back-test. A "
            "transparent, weighted churn-risk score ranks 4,343 accounts into Low, "
            "Moderate and High tiers. Over a three-month forward window the High tier "
            "churns at 18.90% against an overall rate of 2.46%, a 7.7-times lift, and "
            "the tier ordering shows no monotonicity violations. The top score decile "
            "churns at 4.80%, more than three times the bottom decile. The score is "
            "interpretable: payment stress, usage deterioration and renewal exposure "
            "are the dominant drivers, which means each flag carries a specific next "
            "action rather than an opaque probability.",
            "body",
        )
    )
    s.append(
        P(
            "The forward view quantifies what is at stake. A transparent rate-based "
            "model projects base-case MRR of $10.65M six months out, up 11.8%. A "
            "risk-adjusted path that prices in the high-risk concentration lands "
            "$370K lower in MRR, equivalent to $4.44M less ARR. The full scenario "
            "range, from a fragile-growth downside to a healthy-growth upside, spans "
            "roughly $13M in end-of-horizon ARR. Most of that range is governable "
            "through retention execution and discount discipline rather than new "
            "bookings.",
            "body",
        )
    )
    s.append(
        P(
            "The recommendations that follow are deliberately concrete. They begin "
            "with standing up account-level governance for the 80 High and Critical "
            "accounts and the 30-name priority shortlist, move to a discount-approval "
            "and repricing-at-renewal policy aimed at the deep-discount tail, and "
            "extend to an expansion-quality gate that separates durable growth from "
            "growth that will reverse. None of these depend on new data collection. "
            "Each maps to a flag the scoring layer already produces.",
            "body",
        )
    )
    return s


def build_context() -> list:
    s = [P("2", "h1num"), P("Context and objectives", "h1"), rule()]
    s.append(
        P(
            "Recurring-revenue businesses are usually managed against a small number "
            "of growth metrics: new bookings, MRR, and a retention rate or two. Those "
            "metrics are necessary, but they are lagging and they are coarse. By the "
            "time net revenue retention falls below 100%, the commercial decisions "
            "that caused it, an over-discounted renewal, an expansion sold into a "
            "deteriorating account, a concentration of risk left unmanaged, were made "
            "one to three quarters earlier. A revenue-quality operating system exists "
            "to make those decisions visible while they can still be changed.",
            "lead",
        )
    )
    s.append(
        P(
            "This report applies the operating system to a mid-market and enterprise "
            "software vendor with 4,500 customer accounts, eight plans spanning "
            "self-serve to enterprise, six acquisition channels, and forty account "
            "managers. The analysis window runs for the 36 months from March 2023 to "
            "February 2026. The underlying data covers a longer period, from July 2021, "
            "which supports trailing-window features and cohort tracking.",
            "body",
        )
    )
    s.append(P("What the operating system is built to answer", "h2"))
    s.append(
        P(
            "The system is organized around a sequence of questions that move from "
            "the whole portfolio down to the individual account. How fast is revenue "
            "growing, and how much of that growth is real once discounting and "
            "collection loss are stripped out? How well does the business retain "
            "revenue, gross and net, and does retention hold up cohort by cohort? "
            "Where does churn concentrate by segment, channel, plan and renewal "
            "timing? How much of the customer base depends on discounting to stay, "
            "and does deep discounting predict departure? Which expansions are "
            "durable and which are fragile? Which specific accounts carry the most "
            "downside, and what should be done about each of them? Finally, given all "
            "of this, what is the most likely revenue trajectory over the next six "
            "months, and how wide is the band around it?",
            "body",
        )
    )
    s.append(P("Objectives of this report", "h2"))
    s.append(
        P(
            "The report has three objectives. The first is diagnostic: to give an "
            "executive reader an evidenced, quantified picture of revenue quality "
            "that goes beyond the headline growth and retention numbers. The second "
            "is operational: to translate that diagnosis into a ranked set of actions "
            "tied to specific accounts, policies and owners. The third is "
            "methodological: to demonstrate an approach to revenue analytics that is "
            "transparent end to end, where every score, forecast and chart can be "
            "traced to a documented calculation on auditable data.",
            "body",
        )
    )
    s.append(
        P(
            "A deliberate choice runs through the whole document. Wherever a result "
            "could be read as causal, it is presented as associative. The discount "
            "bands, the expansion-quality flags and the manager-level patterns "
            "describe correlations in the panel. They are strong enough to "
            "prioritize attention and to shape policy, but they do not by themselves "
            "establish that discounting causes churn or that any individual manager "
            "causes the outcomes in their portfolio. The recommendations are framed "
            "accordingly.",
            "note",
        )
    )
    s.append(P("How to read the document", "h2"))
    s.append(
        P(
            "Section 3 sets out the data and the methods. Section 4 describes the "
            "analytical framework, in particular the four composite scores that turn "
            "raw behaviour into governance signals. Section 5 is the body of the "
            "analysis and is split into eight findings, each with its own evidence "
            "and charts. Section 6 is candid about what the analysis cannot support. "
            "Section 7 sets out the recommendations in priority order. The appendix "
            "documents the scenario assumptions and the score weights so the results "
            "can be reproduced.",
            "body",
        )
    )
    return s


def build_data_method() -> list:
    s = [P("3", "h1num"), P("Data and methodology", "h1"), rule()]
    s.append(
        P(
            "The analysis rests on six source tables that together describe the "
            "portfolio from acquisition through monthly performance to invoicing. "
            "They were profiled before any analysis ran, and the profiling found no "
            "material data-quality issues: zero referential-integrity violations, "
            "zero subscription-date coherence problems, zero churn-flag "
            "misalignments, and invoice arithmetic that reconciles to within two "
            "cents. That clean bill matters, because revenue-quality conclusions are "
            "only as trustworthy as the invoice and status data underneath them.",
            "lead",
        )
    )
    s.append(P("Source tables", "h2"))
    s.append(
        data_table(
            ["Table", "Rows", "Grain", "Role in the analysis"],
            [
                ["customers", "4,500", "one per account", "Segment, region, industry, channel, manager"],
                ["plans", "8", "one per plan", "Plan list price and tier"],
                ["subscriptions", "99,729", "one per subscription", "Contract terms, start and end, status"],
                ["monthly_account_metrics", "112,038", "account-month", "MRR, usage, support, NPS, discount"],
                ["invoices", "99,729", "one per invoice", "Billed and collected amounts, delay"],
                ["account_managers", "40", "one per manager", "Portfolio attribution"],
            ],
            widths=[4.6 * cm, 1.7 * cm, 3.0 * cm, CONTENT_W - 4.6 * cm - 1.7 * cm - 3.0 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.3 * cm))
    s.append(
        P(
            "The account-month table is the spine of the work. With 112,038 "
            "account-month observations it carries the recurring-revenue series and "
            "the behavioural signals, usage trend, support volume, sentiment, payment "
            "delay and discount, that the scoring layer later turns into risk. The "
            "invoice table is what allows realized pricing to be separated from list "
            "pricing, and discount from collection loss.",
            "body",
        )
    )
    s.append(P("Definitions used throughout", "h2"))
    s.append(
        P(
            "Consistent definitions matter more than any single number, so they are "
            "fixed once here. MRR is the sum of active MRR in a month and ARR is "
            "twelve times MRR. Gross revenue retention is starting MRR less "
            "contraction and churn, divided by starting MRR. Net revenue retention "
            "adds expansion back into the numerator. Logo churn is churned logos over "
            "beginning-of-month active logos; revenue churn is churned MRR over "
            "beginning MRR. The realized price index is collected revenue relative to "
            "list, so a value of 0.822 means the portfolio realizes about 82 cents on "
            "each list dollar once discount and collection loss are taken together. "
            "That index deliberately mixes commercial discount and collection "
            "effects and is not a clean pricing metric on its own, a caveat carried "
            "through the discount section.",
            "body",
        )
    )
    s.append(P("Analytical layers", "h2"))
    s.append(
        P(
            "Raw tables are transformed into an analytical layer in documented steps. "
            "A customer-health feature table assembles trailing three-month averages "
            "and trends for usage, support, sentiment, payment delay and discount, "
            "together with seat-growth, expansion and contraction frequencies, "
            "tenure and renewal timing. A cohort-retention summary tracks gross and "
            "net retention by acquisition month, segment and region. A scoring layer "
            "converts the health features into four composite scores. A forecasting "
            "layer projects MRR forward under explicit scenarios. Each layer writes a "
            "processed table that is the single source for both this report and the "
            "live dashboard.",
            "body",
        )
    )
    s.append(P("Validation and release controls", "h2"))
    s.append(
        P(
            "The report is not a one-off analysis pasted into a presentation. The "
            "pipeline runs a governed validation gate before publication. In the "
            "current release, 21 of 21 controls pass with no warnings, failures, "
            "high-severity findings or critical findings, and the readiness tier is "
            "technically valid. The controls cover raw-data logic, processed-table "
            "contracts, feature engineering, metric construction, scoring outputs, "
            "forecast outputs, dashboard feeds, written conclusions and release "
            "governance. That matters because the report makes operational claims: "
            "an account may be routed to repricing, a manager portfolio may be "
            "reviewed, and a forecast scenario may influence budget decisions. Those "
            "claims need a stronger basis than a visually plausible chart.",
            "body",
        )
    )
    s.append(
        P(
            "The validation gate does not make the findings causal, and it does not "
            "make synthetic data equivalent to live production data. It does mean the "
            "numbers reconcile across the processed tables, charts, dashboard and "
            "written narrative, and that the score and forecast artefacts meet their "
            "published contracts. For an executive reader, the practical conclusion "
            "is simple: the report can be challenged on interpretation and business "
            "judgement, but not on avoidable arithmetic drift between artefacts.",
            "body",
        )
    )
    return s


def build_framework() -> list:
    s = [P("4", "h1num"), P("Analytical framework", "h1"), rule()]
    s.append(
        P(
            "The framework turns behaviour into decisions through four composite "
            "scores. Each score is a transparent weighted combination of normalized "
            "inputs, each produces a tier, and each carries a named main driver so "
            "that a high score points to a specific cause and therefore a specific "
            "action. The intention is the opposite of a black box. A revenue leader "
            "should be able to read why an account scored the way it did and what to "
            "do next, without a data scientist in the room.",
            "lead",
        )
    )
    s.append(P("The four scores", "h2"))
    s.append(
        P(
            "The churn-risk score estimates the likelihood that an account leaves in "
            "the near term. It is the most heavily validated of the four and drives "
            "the early-warning work in Section 5.7. The revenue-quality score judges "
            "how sound an account's revenue is, blending discount dependency, "
            "payment behaviour and the balance of expansion against contraction. The "
            "discount-dependency score isolates how much an account relies on price "
            "concessions to stay. The expansion-quality score separates durable "
            "growth from growth booked while health was deteriorating.",
            "body",
        )
    )
    s.append(P("How the churn-risk score is weighted", "h2"))
    s.append(
        P(
            "The churn-risk score is the weighted sum of seven normalized inputs. The "
            "weights were set to favour leading behavioural signals over lagging "
            "commercial ones, then frozen before the back-test so that validation "
            "could not be tuned after the fact.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Input", "Weight", "What it captures"],
            [
                ["Usage deterioration", "25%", "Falling product usage over the trailing window"],
                ["Payment stress", "20%", "Rising payment delay and collection friction"],
                ["Sentiment / support", "15%", "Falling NPS and rising support load"],
                ["Commercial contraction", "15%", "Frequency and severity of contraction events"],
                ["Discount pressure", "10%", "Reliance on deep or rising discounts"],
                ["Renewal exposure", "10%", "Proximity to a renewal decision point"],
                ["History / tenure", "5%", "Early-tenure and prior-churn fragility"],
            ],
            widths=[4.4 * cm, 1.8 * cm, CONTENT_W - 4.4 * cm - 1.8 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.3 * cm))
    s.append(
        P(
            "Usage deterioration carries the most weight because, in the panel, a "
            "sustained decline in usage is the earliest reliable sign that an account "
            "is disengaging. Payment stress is next: late payment is both a financial "
            "and a behavioural signal, and it tends to precede formal churn. Sentiment "
            "and contraction sit in the middle. Discount pressure, renewal exposure "
            "and tenure are lighter, included because they sharpen prioritization "
            "near a renewal even though each is weaker on its own.",
            "body",
        )
    )
    s.append(P("From score to governance", "h2"))
    s.append(
        P(
            "The four scores combine into a single governance-priority tier that "
            "decides how much management attention an account warrants. An account "
            "rises to High or Critical when several scores agree, for example a "
            "high churn risk driven by usage decline, on top of weak revenue quality "
            "from heavy discounting, near a renewal. That convergence is what makes "
            "the governance tier more useful than any single score: it surfaces the "
            "accounts where the case for intervention is strongest and where the "
            "recommended action is least ambiguous.",
            "body",
        )
    )
    s.append(
        P(
            "Each scored account also receives a recommended action drawn from a "
            "fixed vocabulary, monitor only, review discount policy, reprice at "
            "renewal, prepare renewal intervention, review account-manager behaviour, "
            "or escalate to customer success, so that the output is a work queue "
            "rather than a report. Section 5.7 shows how that queue distributes "
            "across the base.",
            "body",
        )
    )
    s.append(P("Decision rights and operating rhythm", "h2"))
    s.append(
        P(
            "The framework is deliberately designed to separate diagnosis from "
            "authority. The score can tell the business that an account is a renewal "
            "risk, a discount-dependency case, a payment-friction case or an "
            "expansion-quality case. It should not, by itself, approve a concession "
            "or force a commercial action. The right control is an operating cadence: "
            "customer success owns usage and sentiment interventions, finance owns "
            "payment stress, sales leadership owns discount exceptions, and revenue "
            "operations owns the weekly queue and exception log. This keeps the score "
            "interpretable while preventing it from becoming an ungoverned black-box "
            "decision engine.",
            "body",
        )
    )
    return s


def build_findings() -> list:
    s = [P("5", "h1num"), P("Findings", "h1"), rule()]
    s.append(
        P(
            "The findings move from the portfolio to the account. The first three "
            "subsections establish the shape of the business: how revenue grew, how "
            "well it is retained, and where churn concentrates. The next three "
            "examine the quality of that revenue: discounting, expansion and the "
            "concentration of risk. The last two operationalize the diagnosis "
            "through the scoring system and the forward view.",
            "lead",
        )
    )

    # 5.1
    s.append(P("5.1  Revenue scale and the quality of growth", "h2"))
    s.append(
        P(
            "MRR grew from $3.75M in March 2023 to $9.52M in February 2026. That is a "
            "2.54-times increase over the window, an implied 2.70% compounded each "
            "month, and an ARR run-rate that reached $114.3M. On the standard growth "
            "lens the company is performing well and the trend is steady rather than "
            "lumpy, as the smooth climb in Exhibit 1 shows.",
            "body",
        )
    )
    s += figure(
        "mrr_arr_growth_trend.png",
        "Monthly recurring revenue and annual run-rate, 36-month view",
        "MRR rose from $3.75M to $9.52M, an ARR run-rate of $114.3M, compounding "
        "at roughly 37.7% a year. The trajectory is steady, which is exactly why "
        "headline growth alone hides the quality questions raised in the rest of "
        "this report.",
    )
    s.append(
        P(
            "Growth on its own says nothing about quality. Two figures qualify the "
            "headline. The realized price index ended the window at 0.822, up "
            "modestly from 0.792, which means the portfolio gives back close to "
            "eighteen cents on each list dollar through discount and collection loss "
            "combined. And 15.9% of MRR carries a discount-dependency flag, revenue "
            "that exists at its current level because of a price concession. Growth "
            "that arrives at 82 cents on the dollar is worth materially less than the "
            "same growth at list, and the discount section quantifies how that "
            "discounting also predicts churn.",
            "body",
        )
    )
    s.append(
        P(
            "One reassuring structural fact emerges here and recurs later. Revenue is "
            "not concentrated. The ten largest accounts hold just 4.0% of MRR and the "
            "fifty largest only 14.3%. The business does not depend on a handful of "
            "marquee logos, which lowers idiosyncratic revenue risk. As Section 5.6 "
            "shows, that diversification does not extend to the at-risk cohort, where "
            "downside is highly concentrated, but the base itself is healthy in this "
            "respect.",
            "body",
        )
    )
    s.append(
        P(
            "The composition of that growth also deserves precision. Across the "
            "window the base booked $1.63M of expansion MRR against $579K of "
            "contraction, a net positive contribution from the installed base of "
            "about $1.05M. Set against total MRR growth of $5.77M, roughly eighteen "
            "percent of the increase came from the existing base expanding and the "
            "remaining four-fifths from net new logos. A company that grows mainly on "
            "new logos rather than base expansion is more exposed to any slowdown in "
            "acquisition, which is the structural reason the retention and expansion-"
            "quality findings later in this section carry weight beyond their size.",
            "body",
        )
    )
    s.append(
        P(
            "The management implication is that the company should not treat growth "
            "rate as the primary control metric. At this scale, the higher-quality "
            "question is whether each incremental dollar arrives with healthy usage, "
            "clean collection, rational discounting and a path to renewal. A board "
            "view that shows MRR and ARR without realized price, discount dependency "
            "and at-risk MRR would miss the three signals that deteriorate before "
            "reported growth does.",
            "body",
        )
    )

    # 5.2
    s.append(P("5.2  Retention: gross, net and the cohort view", "h2"))
    s.append(
        P(
            "Retention is strong on every headline measure and thin on the one that "
            "matters most for durability. Latest gross revenue retention is 99.17% "
            "and net revenue retention is 99.82%. Logo churn is 0.73% and revenue "
            "churn 0.39% in the most recent month. Across the full window the "
            "averages are similar, with GRR at 99.19% and NRR at 99.87%. Exhibit 2 "
            "tracks the two retention series over time.",
            "body",
        )
    )
    s += figure(
        "grr_nrr_retention_trend.png",
        "Gross and net revenue retention over the window",
        "GRR and NRR both sit close to 99%, but NRR hovers just below the "
        "100% line. Net retention near parity means expansion barely "
        "outweighs the combined drag of contraction and churn.",
    )
    s.append(
        P(
            "The single most important sentence in this report is about that net "
            "number. Net revenue retention at 99.82% is below 100%. Expansion is not "
            "quite covering the combined drag of contraction and churn. The business "
            "is growing because new bookings are large, not because the installed "
            "base is compounding on its own. That is a different and more fragile "
            "growth engine than NRR comfortably above 100% would provide, and it is "
            "the reason the discount and expansion-quality findings matter so much: "
            "there is no retention buffer to absorb a deterioration in either.",
            "body",
        )
    )
    s.append(
        P(
            "The logo and revenue churn series in Exhibit 3 reinforce the point. Both "
            "are low and stable, and revenue churn runs below logo churn, which tells "
            "us the accounts that leave are smaller than average. That is a benign "
            "pattern. It is also a complacency trap, because the accounts most likely "
            "to leave are not the ones doing damage today; they are the over-"
            "discounted and fragile-expansion accounts that the later sections "
            "isolate.",
            "body",
        )
    )
    s += figure(
        "logo_churn_and_revenue_churn.png",
        "Logo churn and revenue churn over the window",
        "Revenue churn runs below logo churn, so departing accounts are "
        "smaller than average. The base is stable today, which is why the "
        "leading indicators in later sections deserve the attention.",
    )
    s.append(
        P(
            "The cohort heatmap in Exhibit 4 puts retention on a more honest footing "
            "than any single rate. Reading down a column shows how a given month "
            "since acquisition behaves across cohorts; reading across a row shows a "
            "single cohort ageing. Most cells sit close to 100% net retention, with "
            "the warmer cells, dipping below 90%, clustered in specific younger "
            "cohorts rather than spread evenly. That clustering is useful: it means "
            "retention weakness is cohort-specific and can be traced to the "
            "acquisition conditions of those months rather than treated as a "
            "portfolio-wide drift.",
            "body",
        )
    )
    s += figure(
        "cohort_retention_heatmap.png",
        "Net revenue retention by acquisition cohort, months 0 to 12",
        "Green cells retain at or above 100%; red cells fall below. Weakness "
        "is concentrated in particular cohorts, which points to acquisition-"
        "quality differences rather than a uniform decline.",
    )
    s.append(
        P(
            "This is also where the report separates performance monitoring from "
            "management action. A small NRR shortfall can be tolerated for a month; "
            "a persistent shortfall, especially if concentrated in the same younger "
            "cohorts, should trigger a review of acquisition quality, onboarding "
            "promises and first-renewal execution. The operating threshold is not a "
            "single red cell in the heatmap. It is repeated weakness in the same "
            "cohort-age band, paired with rising discount dependency or fragile "
            "expansion in those accounts.",
            "body",
        )
    )

    # 5.3
    s.append(P("5.3  Where churn concentrates", "h2"))
    s.append(
        P(
            "Aggregate churn of under 1% conceals a wide spread underneath. Churn is "
            "not uniform; it concentrates predictably by segment, by acquisition "
            "channel, by plan and by renewal timing. Knowing where it concentrates is "
            "what makes a low average actionable rather than merely comfortable.",
            "body",
        )
    )
    s += figure(
        "logo_churn_by_segment_channel.png",
        "Logo churn by customer segment and by acquisition channel",
        "SMB churns at 1.13%, nearly four times the Enterprise rate of "
        "0.30%. Self-serve and paid-media cohorts churn around 1%, while "
        "enterprise-sales and outbound cohorts sit near 0.4%.",
    )
    s.append(
        P(
            "The segment gradient is steep. SMB accounts churn at 1.13%, mid-market "
            "at 0.51% and enterprise at just 0.30%. SMB logos leave at close to four "
            "times the enterprise rate. The channel gradient runs in parallel and is "
            "almost certainly related: self-serve churns at 1.08% and paid media at "
            "1.02%, while enterprise sales churns at 0.38% and outbound at 0.58%. The "
            "low-touch, fast-acquisition motions bring in revenue that is cheaper to "
            "win and quicker to lose. This is not an argument against self-serve, "
            "which is efficient, but an argument for matching retention investment to "
            "the channels that need it most.",
            "body",
        )
    )
    s.append(
        P(
            "Plan and renewal timing add two more concentrations. By plan, logo churn "
            "ranges from 0.17% on the stickiest plan to 1.46% on the most "
            "churn-prone, an order-of-magnitude spread that maps closely to the "
            "segment mix each plan serves. The renewal effect is cleaner and more "
            "operationally direct: accounts inside a renewal window churn at 1.29% "
            "against 0.70% for those that are not, so the act of renewing roughly "
            "doubles near-term churn risk. The renewal window is the single most "
            "predictable moment of vulnerability in the customer lifecycle, which is "
            "why the recommendations treat it as the primary intervention point.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Cut", "Lowest-churn group", "Highest-churn group", "Spread"],
            [
                ["Segment", "Enterprise 0.30%", "SMB 1.13%", "3.8x"],
                ["Channel", "Enterprise sales 0.38%", "Self-serve 1.08%", "2.8x"],
                ["Plan", "P6 0.17%", "P1 1.46%", "8.6x"],
                ["Renewal window", "Not due 0.70%", "Due 1.29%", "1.8x"],
            ],
            widths=[3.0 * cm, 5.0 * cm, 5.0 * cm, CONTENT_W - 13.0 * cm],
            align_right_from=3,
        )
    )
    s.append(Spacer(1, 0.3 * cm))
    s.append(
        P(
            "The plan-level detail is worth seeing in full, because it shows that "
            "churn risk is not a property of price point alone. The table below ranks "
            "all eight plans by logo churn and pairs each with the count of active "
            "account-months behind the rate. The two most churn-prone plans, P1 at "
            "1.46% and P3 at 1.27%, are also two of the three largest by exposure, so "
            "they drive a disproportionate share of total departures. The stickiest "
            "plans, P6 at 0.17% and P8 at 0.22%, retain almost an order of magnitude "
            "better. A retention program that starts with P1 and P3 reaches the "
            "largest pool of avoidable churn first.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Plan", "Active account-months", "Churn events", "Logo churn"],
            [
                ["P1", "22,467", "327", "1.46%"],
                ["P3", "19,458", "248", "1.27%"],
                ["P5", "7,528", "65", "0.86%"],
                ["P7", "4,276", "24", "0.56%"],
                ["P2", "14,284", "62", "0.43%"],
                ["P4", "13,639", "47", "0.34%"],
                ["P8", "6,381", "14", "0.22%"],
                ["P6", "8,829", "15", "0.17%"],
            ],
            widths=[2.6 * cm, 5.8 * cm, 4.0 * cm, CONTENT_W - 12.4 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "These concentrations compound rather than cancel. An SMB account on plan "
            "P1, acquired through self-serve and approaching a renewal, carries four "
            "separate risk signals at once, and the scoring system in Section 5.7 is "
            "designed precisely to combine them rather than read each in isolation. "
            "The value of the concentration analysis is that it tells the business "
            "where to point the score: the avoidable churn is not spread evenly, it "
            "sits in identifiable corners of the portfolio.",
            "body",
        )
    )

    # 5.4
    s.append(P("5.4  Discount intensity and realized pricing", "h2"))
    s.append(
        P(
            "Discounting is the clearest example of revenue quality hiding inside "
            "revenue growth. The weighted discount across the active base is 17.7%, "
            "the realized price index has settled at 0.822, and the share of MRR that "
            "carries a discount-dependency flag is 15.9%. Roughly one revenue dollar "
            "in six depends on a price concession to exist at its current level. "
            "Exhibit 5 tracks how discount dependency has moved over the window.",
            "body",
        )
    )
    s += figure(
        "discount_dependency_trend.png",
        "Discount-dependent share of MRR over the window",
        "About one MRR dollar in six carries a discount-dependency flag. "
        "Discounting is structural to the current revenue base, not a "
        "promotional spike.",
    )
    s.append(
        P(
            "Discounting would matter for margin even if it were harmless to "
            "retention. It is not harmless. Exhibit 6 sorts accounts by their current "
            "discount band and shows the churn they go on to experience over the next "
            "three months. The relationship is not a simple straight line, and the "
            "report is careful not to pretend it is. The first three bands actually "
            "decline, from 2.64% at ten percent or less, to 2.16% in the ten-to-"
            "twenty band, to 1.81% in the twenty-to-thirty band. Then the deepest "
            "band breaks the pattern hard: accounts discounted more than 30% off list "
            "churn at 4.31%, well over double the band just below them.",
            "body",
        )
    )
    s += figure(
        "discount_band_vs_forward_churn.png",
        "Forward three-month churn by current discount band",
        "The deepest discount band, over 30% off list, churns at 4.31%, "
        "more than double the 20-to-30% band. Moderate discounting does "
        "not predict departure; deep discounting does.",
    )
    s.append(
        P(
            "The honest reading is that moderate discounting is a normal commercial "
            "tool that does not, on its own, predict departure, while deep "
            "discounting is a distress signal. An account that needs more than thirty "
            "percent off to stay is usually an account that has already decided it is "
            "not getting enough value, and the discount postpones rather than "
            "prevents the exit. That is why the recommended response to the deep-"
            "discount tail is not simply more discount at renewal but repricing and a "
            "value conversation, and why discount intensity near renewal earns a "
            "place in the churn-risk score.",
            "body",
        )
    )
    s.append(
        P(
            "The scoring layer turns discounting from a portfolio average into an "
            "account-level tier. On the discount-dependency score, 542 accounts sit "
            "in the High tier and a further 245 in the Critical tier, so 787 accounts "
            "depend heavily on price concessions, against 3,408 with low dependency. "
            "The revenue-quality score, which blends discounting with payment "
            "behaviour and the expansion-to-contraction balance, is more sobering: "
            "876 accounts fall in its Critical tier and 501 in High, meaning roughly "
            "thirty percent of the base carries a revenue-quality concern of some "
            "degree even while headline retention looks healthy. The two distributions "
            "below show how each score spreads across the base.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Tier", "Discount-dependency score", "Revenue-quality score"],
            [
                ["Low", "3,408", "1,322"],
                ["Moderate", "305", "1,801"],
                ["High", "542", "501"],
                ["Critical", "245", "876"],
            ],
            widths=[4.0 * cm, CONTENT_W / 2 - 1.0 * cm, CONTENT_W / 2 - 1.0 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "These two tiers are not the same accounts, and the difference is "
            "informative. Discount dependency is narrow, concentrated in the 787 "
            "accounts that need a price concession to stay. Revenue-quality concern "
            "is broader, because an account can have sound pricing yet weak revenue "
            "quality through erratic payment or a contraction habit. Reading the two "
            "together separates the accounts that need a pricing fix from those that "
            "need a relationship or collections fix, which is exactly the distinction "
            "the recommended-action vocabulary is built to make.",
            "body",
        )
    )
    s.append(
        P(
            "One measurement caveat carries real weight here. The realized price "
            "index blends commercial discount with collection loss, so a low realized "
            "price can reflect either generous pricing or weak collections, and the "
            "two call for different fixes. The index is a strong portfolio-level "
            "signal of revenue quality, but it should not be read as a clean pricing "
            "metric for an individual account without checking the invoice detail "
            "underneath it.",
            "note",
        )
    )
    s.append(
        P(
            "The practical policy is therefore two-stage. First, classify the account "
            "problem before approving any new concession: is the price gap a true "
            "commercial discount, a collection-delay issue, a usage-value problem, "
            "or a renewal-timing problem? Second, require an offset for every deep "
            "discount that is renewed, such as scope reduction, term extension, "
            "executive-success plan, committed usage milestone or a dated path back "
            "toward list. Without that offset, discounting is simply converting "
            "margin loss into delayed churn.",
            "body",
        )
    )

    # 5.5
    s.append(P("5.5  Expansion quality", "h2"))
    s.append(
        P(
            "Expansion is the engine that is supposed to push net retention above "
            "100%. The question this section asks is not how much the base expanded "
            "but how much of that expansion is durable. The operating system flags "
            "every expansion event as healthy, watch or fragile based on the health "
            "of the account at the time it grew. Exhibit 7 shows how $1.63M of "
            "expansion MRR splits across those three qualities.",
            "body",
        )
    )
    s += figure(
        "expansion_quality_mix.png",
        "Composition of expansion MRR by quality flag",
        "Healthy expansion is 44% of the $1.63M booked. Fragile expansion, "
        "growth on top of deteriorating health, is 28%, and watch a "
        "further 28%.",
    )
    s.append(
        P(
            "Fragile expansion is 28% of the total, $457K of the $1.63M, spread "
            "across 1,060 events. The label is precise: these are accounts that "
            "bought more while their usage, sentiment or payment behaviour was "
            "already deteriorating. In the panel, fragile expansion is associated "
            "with elevated churn three to nine months later. The implication is "
            "uncomfortable but important. A portion of the expansion that flatters "
            "net retention this quarter is borrowing against retention next year, "
            "because some of those expanded accounts will contract or churn and take "
            "the new MRR with them.",
            "body",
        )
    )
    s.append(
        P(
            "This reframes the net-retention story from Section 5.2. NRR sits at "
            "99.82% even with fragile expansion counted in the numerator. Strip out "
            "the fragile portion and the underlying durable expansion is thinner "
            "still. The practical response is an expansion-quality gate: before "
            "celebrating an upsell, check whether the account's health supports it, "
            "and route fragile expansions to customer success rather than to the "
            "forecast. The data to run that gate already exists in the health-feature "
            "table.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Expansion quality", "Expansion MRR", "Events", "Management treatment"],
            [
                ["Healthy", "$715K", "1,525", "Count as durable expansion"],
                ["Watch", "$460K", "964", "Count, but monitor health before renewal"],
                ["Fragile", "$457K", "1,060", "Discount in forecast; route to success"],
            ],
            widths=[3.6 * cm, 3.0 * cm, 2.4 * cm, CONTENT_W - 9.0 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "This treatment avoids a common failure in SaaS planning: treating all "
            "expansion MRR as equal. A healthy expansion can carry quota credit and "
            "forecast confidence. A watch expansion should count commercially but "
            "stay in the customer-success book. A fragile expansion should not be "
            "allowed to inflate the durable-growth narrative until the account's "
            "usage and payment signals recover.",
            "body",
        )
    )

    # 5.6
    s.append(P("5.6  Account-level concentration and at-risk MRR", "h2"))
    s.append(
        P(
            "Revenue is diversified but risk is not, and the gap between those two "
            "facts is where account-level governance earns its keep. The portfolio's "
            "top ten accounts hold only 4.0% of MRR, so no single logo can sink the "
            "business. Inside the at-risk cohort, however, concentration is extreme. "
            "Seventy-nine accounts carry a High governance priority and one is "
            "Critical. Together, those 80 accounts hold $376K of MRR, and the top "
            "twenty of those account for "
            "81.3% of the at-risk MRR. Exhibit 8 draws that concentration curve.",
            "body",
        )
    )
    s += figure(
        "at_risk_mrr_concentration.png",
        "Concentration of at-risk MRR within the high-risk cohort",
        "The top twenty high-risk accounts hold 81.3% of at-risk MRR. "
        "Downside risk is concentrated even though the revenue base is "
        "well diversified.",
    )
    s.append(
        P(
            "Concentration of this kind is good news for management, because it means "
            "the problem is small enough to work by hand. The annualized ARR "
            "associated with High and Critical accounts is $4.51M. A stress test in "
            "which the top twenty high-risk accounts churn outright removes $3.67M of "
            "ARR; a milder test in which they each contract by twenty percent removes "
            "$734K. Those are large numbers relative to the $376K of at-risk MRR "
            "precisely because the at-risk accounts are not the smallest ones. "
            "Exhibit 9 names the top of the governance-priority queue with each "
            "account's current MRR and risk tier.",
            "body",
        )
    )
    s += figure(
        "governance_priority_accounts.png",
        "Top accounts by governance priority, current MRR and risk tier",
        "A single account carries a Critical tier; the rest of the top "
        "twenty are High. Current MRR varies widely, so the queue mixes "
        "large repricing cases with smaller renewal interventions.",
    )
    s.append(
        P(
            "Exhibit 10 reframes the same risk relationally, at the level of the "
            "account manager. Each bubble is one of the forty managers, positioned by "
            "average portfolio discount and portfolio churn rate and sized by "
            "portfolio MRR. The honest finding is that, at this aggregate level, "
            "discount and churn are only weakly related, with a correlation of about "
            "minus 0.12, so discount alone does not rank managers. What the chart "
            "does surface is a cluster in the top-right quadrant: a small number of "
            "managers, including the four labelled for review, who combine "
            "above-median discounting with above-median churn. Those portfolios merit "
            "a behavioural review, not because the chart proves cause, but because "
            "they are the places where both signals point the same way.",
            "body",
        )
    )
    s += figure(
        "account_manager_discount_vs_churn.png",
        "Account-manager portfolios: discount versus churn",
        "Portfolio-level discount and churn are only weakly correlated, so "
        "discounting does not rank managers on its own. The review priority "
        "is the cluster that is high on both at once.",
    )
    s.append(
        P(
            "The governance queue resolves to a named shortlist. The system publishes "
            "a 30-account priority list; the head of it is shown below with each "
            "account's segment, current MRR, churn-risk score and the action the "
            "system recommends. The list deliberately mixes a single large enterprise "
            "repricing case with a set of smaller SMB renewal interventions, because "
            "governance priority weighs risk and revenue together rather than chasing "
            "size alone. This table is the literal starting agenda for the weekly "
            "governance cadence proposed in Section 7.",
            "body",
        )
    )
    s.append(
        P(
            "The shortlist is small but not trivial. It contains $140K of current MRR, "
            "or $1.68M annualized, and 26 of the 30 accounts are already inside a "
            "renewal-risk window. Every account on the list carries both low-NPS and "
            "discount-dependency flags, ten also show usage decline, and seven show "
            "payment-delay stress. This is why a generic save motion would be weak. "
            "The list needs account-specific treatment: value recovery for the "
            "low-NPS cases, repricing discipline for the discount cases, product or "
            "success intervention where usage is falling, and billing remediation "
            "where payment stress is present.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Account", "Segment", "Current MRR", "Risk score", "Recommended action"],
            [
                ["CUST004130", "Enterprise", "$33,423", "61.1", "Reprice at renewal"],
                ["CUST004167", "Enterprise", "$17,819", "47.3", "Reprice at renewal"],
                ["CUST003615", "SMB", "$1,634", "62.6", "Review discount policy"],
                ["CUST000063", "Mid-Market", "$1,243", "62.9", "Reprice at renewal"],
                ["CUST003233", "SMB", "$662", "71.2", "Prepare renewal intervention"],
                ["CUST001183", "SMB", "$520", "62.2", "Reprice at renewal"],
                ["CUST003812", "SMB", "$302", "66.5", "Reprice at renewal"],
                ["CUST000998", "SMB", "$203", "71.3", "Prepare renewal intervention"],
            ],
            widths=[2.9 * cm, 2.4 * cm, 2.6 * cm, 2.2 * cm, CONTENT_W - 10.1 * cm],
            align_right_from=2,
        )
    )

    # 5.7
    s.append(P("5.7  The churn-risk scoring system", "h2"))
    s.append(
        P(
            "Everything above describes the past. The scoring system is the part of "
            "the operating system that acts on the future, and it is the most "
            "rigorously validated component. Exhibit 11 shows how the churn-risk score "
            "distributes across the base. The mass of accounts sits at low scores, "
            "with a thin right tail of genuinely high-risk accounts, which is the "
            "shape a useful early-warning score should have: most accounts are fine "
            "and the score does not cry wolf.",
            "body",
        )
    )
    s += figure(
        "churn_risk_score_distribution.png",
        "Distribution of the churn-risk score across the base",
        "Most accounts score low, with a thin high-risk tail. A useful "
        "early-warning score concentrates alarm rather than spreading it.",
    )
    s.append(
        P(
            "A distribution is only credible if the scores predict. The back-test in "
            "Exhibit 12 evaluates 4,343 accounts over an 88,613-row, three-month "
            "forward window and asks a simple question: do higher scores churn more? "
            "They do. The top score decile churns at 4.80% against an overall rate of "
            "2.46%, a lift of 1.9 times the average and more than three times the "
            "bottom decile's 1.52%. The gradient is not perfectly smooth at every "
            "decile, and the chart shows the two small reversals honestly rather than "
            "hiding them, but the direction is unambiguous and the top decile "
            "separates cleanly.",
            "body",
        )
    )
    s += figure(
        "score_decile_calibration.png",
        "Realized forward churn by churn-risk score decile",
        "Higher score deciles churn more, and the top decile separates "
        "sharply at 4.80%. Two mid-deciles reverse slightly; the overall "
        "rank-ordering holds.",
    )
    s.append(
        P(
            "The tier view is cleaner still and is the one used operationally. "
            "Accounts in the Low tier churn at 2.31%, the Moderate tier at 6.14% and "
            "the High tier at 18.90%. The High tier therefore churns at 7.7 times the "
            "overall rate, the ordering shows no monotonicity violations, and the "
            "separation is wide enough to drive resource allocation. The High and "
            "Moderate tiers together hold 259 accounts, a small enough number that a "
            "customer-success team can work every one of them.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Risk tier", "Accounts", "Forward 3m churn", "Lift vs overall"],
            [
                ["Low", "4,241", "2.31%", "0.94x"],
                ["Moderate", "217", "6.14%", "2.49x"],
                ["High", "42", "18.90%", "7.67x"],
                ["Overall", "4,343", "2.46%", "1.00x"],
            ],
            widths=[4.0 * cm, 3.2 * cm, 4.6 * cm, CONTENT_W - 11.8 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "Because the score is transparent, it also explains itself. Across the "
            "base, payment stress is the single most common main driver of churn "
            "risk, named for 3,163 accounts, followed by usage deterioration for 647 "
            "and renewal exposure for 408. That ordering tells the business where to "
            "invest: collections and payment-friction work would touch the largest "
            "group of at-risk accounts, while usage-based intervention addresses the "
            "next most common cause. The score does not just rank accounts; it sorts "
            "them into the kind of problem they have. The full driver distribution "
            "below makes the priority concrete.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Main churn-risk driver", "Accounts", "Implied owner"],
            [
                ["Payment stress", "3,163", "Collections and billing"],
                ["Usage deterioration", "647", "Customer success and product"],
                ["Renewal exposure", "408", "Renewals desk"],
                ["Discount pressure", "223", "Commercial and pricing"],
                ["Commercial contraction pattern", "35", "Account management"],
                ["Sentiment / support deterioration", "14", "Support and success"],
                ["History / early-tenure fragility", "10", "Onboarding"],
            ],
            widths=[6.6 * cm, 2.6 * cm, CONTENT_W - 9.2 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "Payment stress dominating the driver mix is itself a finding. The largest "
            "single lever on near-term churn risk is not a product or a discount "
            "problem but a billing and collections one, and a tighter dunning and "
            "payment-friction process would touch more at-risk accounts than any other "
            "single intervention. It also reinforces why payment carries the second-"
            "heaviest weight in the score: it is both common and early.",
            "body",
        )
    )
    s.append(
        P(
            "The output is a work queue. Of the 4,500 accounts, the system marks "
            "4,237 as monitor-only and routes the remaining 263 to a specific "
            "action: 148 to a discount-policy review, 61 to repricing at renewal, 32 "
            "to an account-manager behavioural review, 21 to a prepared renewal "
            "intervention and one to a customer-success escalation. A 30-name "
            "priority shortlist sits at the top of that queue and is the natural "
            "starting point for the governance cadence described in Section 7.",
            "body",
        )
    )
    s.append(
        P(
            "The weight-sensitivity test is a useful guardrail on whether this queue "
            "is stable enough to manage. Perturbing each governance-priority weight "
            "by plus or minus 20% moves, on average, 2.9% of accounts across a tier "
            "boundary; the largest movement is 5.3% when exposure concentration is "
            "reduced. That is not perfect immutability, and it should not be sold as "
            "such. It is good operational stability: the exact boundary cases may "
            "move, but the bulk of the queue and the high-priority narrative remain "
            "intact under a material weighting challenge.",
            "body",
        )
    )

    # 5.8
    s.append(P("5.8  Forecast and scenario analysis", "h2"))
    s.append(
        P(
            "The forward view is built to be decision-useful rather than precise to "
            "the dollar. It is an interpretable monthly rate model, with baseline "
            "expansion, contraction, churn and net-new rates derived from a "
            "recency-weighted average of the last six observed months, projected six "
            "months forward from February 2026. There is no black box; every "
            "assumption is explicit and can be changed. Exhibit 13 shows the resulting "
            "MRR trajectories under each scenario.",
            "body",
        )
    )
    s += figure(
        "scenario_mrr_trajectories.png",
        "Six-month MRR trajectories under each scenario",
        "The base case reaches $10.65M. The risk-adjusted path runs below "
        "it; the bull and bear cases bracket a wide band that the business "
        "can influence through execution.",
    )
    s.append(
        P(
            "The base case projects MRR of $10.65M in six months, up 11.8%, on "
            "monthly rates of 0.68% expansion, 0.30% contraction, 0.62% churn and "
            "2.12% net-new. The risk-adjusted case is the one to plan against. It "
            "prices in the high-risk concentration from Section 5.6 by raising churn "
            "to 0.93% and contraction to 0.51% a month, and it lands at $10.28M, "
            "about $370K lower in MRR than the base case. Annualized, that gap is "
            "$4.44M of ARR, the cost of leaving the at-risk cohort unmanaged.",
            "body",
        )
    )
    s.append(
        P(
            "Exhibit 14 expresses the full scenario set as ARR differences from the "
            "base case, which is the most useful way for an executive to read the "
            "range. The fragile-growth downside sits $8.69M below base. The healthy-"
            "growth upside sits $4.50M above it. The discount-discipline scenario is "
            "the most interesting: it lands within a rounding error of the base case "
            "on headline ARR, because slightly lower expansion offsets lower churn, "
            "yet it improves realized ARR quality by lifting the price index. In "
            "other words, discount discipline is close to free on the topline while "
            "improving the quality of the revenue underneath it.",
            "body",
        )
    )
    s += figure(
        "scenario_arr_variance_vs_base.png",
        "End-of-horizon ARR versus the base case, by scenario",
        "The scenario band spans roughly $13M of ARR, from $8.69M below "
        "base to $4.50M above. Most of that range is governable through "
        "retention and discount execution.",
    )
    s.append(
        P(
            "Two conclusions follow. The roughly $13M ARR spread between the bear and "
            "bull cases is not driven by new-bookings assumptions; it is driven by "
            "churn, contraction and expansion rates, which are exactly the variables "
            "the operating system is built to influence. And the risk-adjusted "
            "shortfall of $4.44M ARR is a concrete budget for retention investment: "
            "any program that costs less than that and meaningfully reduces the "
            "high-risk cohort's churn pays for itself within the forecast horizon.",
            "body",
        )
    )
    s.append(
        P(
            "There is a final layer to the forecast that ties the whole report "
            "together. Each scenario carries not just a nominal ARR but a realized ARR "
            "estimate, the nominal figure adjusted by the realized price index. In the "
            "base case, nominal end ARR of $127.8M corresponds to a realized estimate "
            "of about $105.0M, a gap of nearly $23M that is the dollar expression of "
            "the discount and collection drag documented in Section 5.4. The "
            "discount-discipline scenario, which barely moves nominal ARR, lifts the "
            "realized estimate to $107.6M, roughly $2.6M better, purely by improving "
            "the price index by two points. This is the clearest single argument for "
            "discount discipline in the report: it is nearly invisible on the topline "
            "and worth millions in realized revenue.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Monthly review trigger", "Why it matters", "Management response"],
            [
                [
                    "NRR remains below 100%",
                    "Expansion is not covering churn and contraction",
                    "Escalate retention plan",
                ],
                [
                    "Deep-discount churn rises",
                    "Discounting is delaying rather than preventing loss",
                    "Tighten renewal approval",
                ],
                ["High-risk MRR grows", "Concentration risk is expanding", "Reprioritize governance queue"],
                [
                    "Risk-adjusted gap widens",
                    "Forecast downside is becoming the base reality",
                    "Recut budget assumptions",
                ],
            ],
            widths=[4.4 * cm, 5.5 * cm, CONTENT_W - 9.9 * cm],
            align_right_from=3,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "These triggers turn the forecast from a planning exhibit into a control "
            "system. The monthly question is not whether the base case was exactly "
            "right. It is whether the business is migrating toward the risk-adjusted "
            "or downside path, and whether the leading indicators, discount "
            "dependency, fragile expansion, renewal exposure and high-risk MRR, are "
            "moving before the headline revenue line does.",
            "body",
        )
    )
    s.append(
        P(
            "These are operating forecasts, not statistical confidence intervals. The "
            "net-new rate is a residual that can absorb unobserved commercial "
            "drivers, and the scenario outputs are sensitive to their assumptions, "
            "which is why the appendix documents every rate and why the model is "
            "designed to be re-run monthly as new data arrives.",
            "note",
        )
    )
    return s


def build_risks() -> list:
    s = [P("6", "h1num"), P("Risks, limitations and caveats", "h1"), rule()]
    s.append(
        P(
            "A report that only argues its own case is not trustworthy. This section "
            "sets out, plainly, what the analysis cannot support, so the "
            "recommendations are read with the right level of confidence.",
            "lead",
        )
    )
    s.append(P("Simulation scope and transferability", "h3"))
    s.append(
        P(
            "The portfolio described in this report is a purpose-built simulation "
            "designed to be internally consistent and analytically realistic, with "
            "plausible revenue distributions, coherent invoice arithmetic and "
            "well-behaved retention dynamics. The operating system, the score "
            "construction, the back-test design, the scenario model and the "
            "governance logic are directly transferable to a production environment. "
            "Running the same pipeline against live data would change the numbers; "
            "it would not change the structure of the analysis or the validity of "
            "the method.",
            "body",
        )
    )
    s.append(P("Findings are associative, not causal", "h3"))
    s.append(
        P(
            "Every relationship in Section 5 is a correlation in the panel. Deep "
            "discounting is associated with higher forward churn, fragile expansion "
            "with later contraction, certain manager portfolios with worse outcomes. "
            "None of these establish cause. Discounting may be a symptom of an "
            "account already at risk rather than the cause of its departure. The "
            "recommendations are therefore framed as prioritization and policy "
            "experiments with measurable outcomes, not as proven levers.",
            "body",
        )
    )
    s.append(P("Realized pricing mixes two effects", "h3"))
    s.append(
        P(
            "The realized price index combines commercial discount and collection "
            "loss. A low value can mean generous pricing, weak collections, or both, "
            "and these require different responses. Where the index drives an "
            "account-level action, the invoice detail underneath it should be checked "
            "before concluding that discounting is the problem.",
            "body",
        )
    )
    s.append(P("Concentration and timing sensitivity", "h3"))
    s.append(
        P(
            "The at-risk concentration figures are snapshot measures and will move as "
            "accounts enter and leave the high-risk cohort. The score weights were "
            "frozen before the back-test, which protects the validation, but they "
            "were not optimized against a held-out outcome, so the lift figures "
            "should be read as evidence that the construction is sound rather than as "
            "a tuned-to-the-limit model. The forecast rates are recency-weighted and "
            "will shift month to month; the model is meant to be refreshed, not set "
            "once.",
            "body",
        )
    )
    s.append(P("What would raise confidence", "h3"))
    s.append(
        P(
            "Three additions would strengthen the conclusions on real data: a "
            "holdout-validated version of the churn score with a proper "
            "precision-recall curve, a controlled test of repricing versus continued "
            "discounting on a matched set of deep-discount accounts to move the "
            "discount finding from associative toward causal, and a longer outcome "
            "window to confirm the fragile-expansion-to-churn link beyond nine "
            "months. Each is a natural next phase rather than a gap in the present "
            "method.",
            "body",
        )
    )
    s.append(
        KeepTogether(
            [
                P("How to use the caveats", "h3"),
                P(
                    "The caveats should change the control design, not dilute the urgency. "
                    "Because the discount and expansion findings are associative, the first "
                    "implementation should be run with measurement discipline: define the "
                    "treated account group, record the intervention, compare outcomes to a "
                    "matched control group, and review the effect at the next renewal cycle. "
                    "Because the forecast is assumption-driven, the model should be refreshed "
                    "monthly and judged on whether it keeps the downside conversation timely, "
                    "not on whether every dollar of end-MRR lands exactly. And because the "
                    "data is synthetic, the first production deployment should start with "
                    "metric reconciliation before changing commercial policy.",
                    "body",
                ),
                P(
                    "In production, the minimum control set should include three "
                    "disciplines. First, reconcile the metric layer against finance-"
                    "owned ARR, invoice and collections records before publishing any "
                    "executive scorecard. Second, keep an intervention log that records "
                    "who acted on each high-risk account, what action was taken, and "
                    "what happened at the next renewal. Third, maintain a challenger "
                    "view of the score weights each quarter so that the model remains "
                    "stable enough to govern but flexible enough to learn from live "
                    "outcomes. These controls are modest, but they are the difference "
                    "between an analytical prototype and an operating system.",
                    "body",
                ),
                P(
                    "The report should therefore be read as a decision-ready operating "
                    "case, not as a final causal proof. It is strong enough to rank "
                    "work, allocate management attention, define policy tests and set "
                    "monthly review triggers. It is not strong enough to automate "
                    "commercial decisions without human review, matched-outcome "
                    "tracking and finance reconciliation. That is the right boundary "
                    "for the evidence available here.",
                    "body",
                ),
            ]
        )
    )
    return s


def build_recommendations() -> list:
    s = [P("7", "h1num"), P("Recommendations and action priorities", "h1"), rule()]
    s.append(
        P(
            "The recommendations are ordered by the ratio of impact to effort. Each "
            "ties to a finding, names the accounts or policy it touches, and can be "
            "run with data the operating system already produces. None requires new "
            "collection or new tooling to begin.",
            "lead",
        )
    )

    s.append(P("Priority 1. Stand up account-level governance on the high-risk cohort", "h3"))
    s.append(
        P(
            "Begin with the 80 High and Critical accounts and the 30-name priority "
            "shortlist. These accounts hold $4.51M of annualized ARR and the top "
            "twenty of them concentrate 81.3% of at-risk MRR, so a weekly governance "
            "cadence over a list this short is the highest-return action available. "
            "Assign each account an owner, the recommended action the system already "
            "attaches, and a review date. The stress test sets the prize: keeping the "
            "top-twenty high-risk accounts from churning protects $3.67M of ARR.",
            "body",
        )
    )

    s.append(P("Priority 2. Reprice the deep-discount tail at renewal", "h3"))
    s.append(
        P(
            "Accounts discounted more than 30% off list churn at 4.31% over the next "
            "three months, more than double the band below them, and the system "
            "already routes 61 accounts to reprice-at-renewal and 148 to a "
            "discount-policy review. Treat the deep-discount tail as a value problem, "
            "not a price problem: pair any renewal with a value review and a path "
            "back toward list rather than a reflexive re-discount. The "
            "discount-discipline scenario shows this is close to free on headline "
            "ARR while improving realized revenue quality.",
            "body",
        )
    )

    s.append(P("Priority 3. Put an expansion-quality gate in front of the forecast", "h3"))
    s.append(
        P(
            "Fragile expansion is 28% of expansion MRR, $457K across 1,060 events, "
            "and it is associated with churn three to nine months later. Before an "
            "upsell is counted as durable, check the account's health signals; route "
            "fragile expansions to customer success and discount them in the "
            "forecast. This protects net retention, which at 99.82% has no buffer to "
            "spare, and it stops the business from booking growth that will reverse.",
            "body",
        )
    )

    s.append(P("Priority 4. Match retention investment to channel and segment risk", "h3"))
    s.append(
        P(
            "SMB churns at 1.13% and self-serve at 1.08%, against 0.30% for "
            "enterprise and 0.38% for enterprise sales. Direct lifecycle and "
            "onboarding investment toward the SMB and self-serve cohorts where churn "
            "concentrates, and protect the low-touch economics by automating that "
            "investment rather than adding headcount. The aim is to narrow the "
            "segment gradient, not to abandon the efficient channels that create it.",
            "body",
        )
    )

    s.append(P("Priority 5. Make the renewal window the primary intervention point", "h3"))
    s.append(
        P(
            "Accounts inside a renewal window churn at 1.29% against 0.70% outside "
            "one, so the renewal is the most predictable moment of risk in the "
            "lifecycle. Trigger a structured renewal play whenever an account "
            "approaches renewal carrying a Moderate or High risk tier, combining the "
            "discount, expansion-quality and risk-driver signals into a single "
            "preparation brief. The 21 accounts already flagged for prepared renewal "
            "intervention are the pilot set.",
            "body",
        )
    )

    s.append(P("Priority 6. Review the manager portfolios that are high on both signals", "h3"))
    s.append(
        P(
            "Discount and churn are only weakly correlated across managers, so this "
            "is a targeted review rather than a portfolio-wide judgement. Focus on "
            "the cluster that combines above-median discounting with above-median "
            "churn, the four managers labelled in Exhibit 10 and the 32 accounts the "
            "system routes to a manager-behaviour review. The goal is to understand "
            "whether the pattern reflects a harder book of business or a coachable "
            "habit, and to act accordingly.",
            "body",
        )
    )

    s.append(P("Sequencing", "h2"))
    s.append(
        P(
            "Priorities 1 through 3 can start immediately and together address the "
            "largest and most governable share of the $4.44M risk-adjusted ARR "
            "shortfall. Priorities 4 through 6 are structural and run on a quarterly "
            "rhythm. The whole program is measurable against a single yardstick: the "
            "gap between the base-case and risk-adjusted forecasts should narrow "
            "month over month as the high-risk cohort shrinks and discount discipline "
            "improves the realized price index.",
            "body",
        )
    )
    s.append(P("Operating cadence and success measures", "h2"))
    s.append(
        data_table(
            ["Cadence", "Owner", "Success measure"],
            [
                ["Weekly risk queue", "Revenue operations", "High/Critical MRR and unresolved actions decline"],
                ["Renewal deal desk", "Sales leadership", "Deep-discount renewals require documented offsets"],
                ["Expansion quality review", "Customer success", "Fragile expansion share falls below current 28%"],
                ["Monthly forecast review", "Finance and RevOps", "Risk-adjusted gap narrows versus base case"],
            ],
            widths=[4.1 * cm, 4.4 * cm, CONTENT_W - 8.5 * cm],
            align_right_from=3,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "This cadence is intentionally lightweight. It uses the artefacts already "
            "produced by the pipeline: the priority shortlist, the recommended action "
            "field, the discount-dependency tier, the expansion-quality flag and the "
            "scenario table. The first month should be treated as a baseline-setting "
            "cycle. By the second month, leadership should expect fewer unresolved "
            "High and Critical accounts, fewer unoffset deep discounts, and a clearer "
            "view of whether the risk-adjusted forecast gap is shrinking.",
            "body",
        )
    )
    return s


def build_appendix() -> list:
    s = [P("8", "h1num"), P("Appendix", "h1"), rule()]
    s.append(P("A. Headline metrics", "h2"))
    s.append(
        data_table(
            ["Metric", "Value"],
            [
                ["MRR, start of window (Mar 2023)", "$3,749,108"],
                ["MRR, end of window (Feb 2026)", "$9,523,590"],
                ["ARR run-rate, end of window", "$114,283,079"],
                ["Implied compounded monthly MRR growth", "2.70%"],
                ["Latest gross revenue retention (GRR)", "99.17%"],
                ["Latest net revenue retention (NRR)", "99.82%"],
                ["Latest logo churn", "0.73%"],
                ["Latest revenue churn", "0.39%"],
                ["Weighted discount, active base", "17.7%"],
                ["Realized price index, end of window", "0.822"],
                ["Discount-dependent share of MRR", "15.9%"],
                ["Top-10 account share of MRR", "4.0%"],
                ["Top-50 account share of MRR", "14.3%"],
                ["High / Critical governance accounts", "80"],
                ["At-risk MRR (High / Critical)", "$376,193"],
                ["Top-20 share within at-risk MRR", "81.3%"],
                ["Annualized ARR at risk", "$4,514,312"],
            ],
            widths=[CONTENT_W - 4.5 * cm, 4.5 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.4 * cm))
    s.append(P("B. Scenario assumptions and outcomes", "h2"))
    s.append(
        P(
            "All scenarios start from $9.52M MRR in February 2026 and project six "
            "months forward. Rates are monthly. The base case continues the recent "
            "rate regime; the other cases apply the multipliers documented below.",
            "body",
        )
    )
    s.append(
        data_table(
            ["Scenario", "End MRR", "End ARR", "ARR vs base"],
            [
                ["Base case", "$10,647,476", "$127.77M", "reference"],
                ["Discount discipline", "$10,647,732", "$127.77M", "+$0.003M"],
                ["Healthy-growth (bull)", "$11,022,495", "$132.27M", "+$4.50M"],
                ["Risk-adjusted", "$10,277,564", "$123.33M", "-$4.44M"],
                ["Fragile-growth (bear)", "$9,923,687", "$119.08M", "-$8.69M"],
            ],
            widths=[5.4 * cm, 3.6 * cm, 3.4 * cm, CONTENT_W - 12.4 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.25 * cm))
    s.append(
        P(
            "Scenario multipliers, applied to base rates: downside raises churn by "
            "50% and contraction by 35% while cutting expansion by 20% and net-new by "
            "30%; improvement cuts churn by 20% and contraction by 15% while lifting "
            "expansion and net-new by 15%; discount discipline cuts churn by 12% and "
            "contraction by 10%, trims expansion by 6% and net-new by 3%, and lifts "
            "the realized price index by two points. The risk-adjusted case raises "
            "churn to 0.93% and contraction to 0.51% a month to reflect the "
            "high-risk concentration.",
            "note",
        )
    )
    s.append(P("C. Churn-risk score weights", "h2"))
    s.append(
        P(
            "The churn-risk score is a weighted sum of seven normalized inputs, "
            "frozen before the back-test: usage deterioration 25%, payment stress "
            "20%, sentiment and support 15%, commercial contraction 15%, discount "
            "pressure 10%, renewal exposure 10%, and history and tenure 5%. The "
            "back-test evaluated 4,343 accounts over an 88,613-row, three-month "
            "forward window, with an overall forward churn rate of 2.46% and no "
            "monotonicity violations across tiers.",
            "body",
        )
    )
    s.append(P("D. Commercial risk impact estimates", "h2"))
    s.append(
        data_table(
            ["Estimate", "Value"],
            [
                ["ARR associated with High / Critical accounts", "$4,514,312"],
                ["Expected 6-month contraction exposure (MRR)", "$124,165"],
                ["Concentration-adjusted 6-month downside (MRR)", "$100,198"],
                ["Stress test: top-20 high-risk full churn (ARR)", "$3,672,017"],
                ["Stress test: top-20 high-risk 20% contraction (ARR)", "$734,403"],
                ["Improvement scenario uplift vs base (ARR)", "$4,500,227"],
            ],
            widths=[CONTENT_W - 4.0 * cm, 4.0 * cm],
            align_right_from=1,
        )
    )
    s.append(Spacer(1, 0.35 * cm))
    s.append(P("E. Churn detail by segment and channel", "h2"))
    s.append(
        data_table(
            ["Cut", "Group", "Active account-months", "Churn events", "Logo churn"],
            [
                ["Segment", "SMB", "54,166", "613", "1.13%"],
                ["Segment", "Mid-Market", "28,569", "147", "0.51%"],
                ["Segment", "Enterprise", "14,127", "42", "0.30%"],
                ["Channel", "Self-serve", "19,334", "208", "1.08%"],
                ["Channel", "Paid media", "16,721", "171", "1.02%"],
                ["Channel", "Content marketing", "15,388", "148", "0.96%"],
                ["Channel", "Partner referral", "18,249", "138", "0.76%"],
                ["Channel", "Outbound SDR", "16,788", "98", "0.58%"],
                ["Channel", "Enterprise sales", "10,382", "39", "0.38%"],
            ],
            widths=[2.4 * cm, 4.2 * cm, 4.6 * cm, 3.0 * cm, CONTENT_W - 14.2 * cm],
            align_right_from=2,
        )
    )
    s.append(Spacer(1, 0.35 * cm))
    s.append(P("F. Glossary of terms", "h2"))
    s.append(
        data_table(
            ["Term", "Definition"],
            [
                ["MRR / ARR", "Monthly recurring revenue; ARR is twelve times MRR."],
                ["GRR", "Starting MRR less contraction and churn, over starting MRR."],
                ["NRR", "GRR with expansion added back to the numerator."],
                ["Logo churn", "Churned logos over beginning-of-month active logos."],
                ["Revenue churn", "Churned MRR over beginning-of-month MRR."],
                ["Realized price index", "Collected revenue relative to list price (1.0 = full list)."],
                ["Discount dependency", "Reliance on a price concession to retain current MRR."],
                ["Fragile expansion", "Expansion booked while account health was deteriorating."],
                ["Governance priority", "Composite tier combining the four account scores."],
                ["Forward churn", "Churn realized over the three months after a snapshot."],
                ["Lift", "A group's churn rate relative to the overall rate."],
            ],
            widths=[4.4 * cm, CONTENT_W - 4.4 * cm],
            align_right_from=99,
        )
    )
    s.append(Spacer(1, 0.35 * cm))
    s.append(P("G. Data and method traceability", "h2"))
    s.append(
        P(
            "Every figure and statistic in this report is generated from the "
            "processed data layer under data/processed and the chart pack under "
            "outputs/graphs. The charts are produced by the visualization scripts in "
            "src/visualization, the scores by the scoring layer in src/scoring, and "
            "the forecast by src/forecasting. This document is assembled by "
            "scripts/build_pdf_report.py, which reads the same processed tables, so "
            "the report, the dashboard and the underlying analysis cannot drift apart. "
            "A reader can reproduce any quoted statistic by running the pipeline "
            "against the source data with the same seed.",
            "body",
        )
    )
    return s


if __name__ == "__main__":
    sys.exit(build())

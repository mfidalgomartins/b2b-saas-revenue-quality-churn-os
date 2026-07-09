"""
Supplementary executive charts for the B2B SaaS Revenue Quality project.

Adds six decision-relevant charts to outputs/graphs/ that round out the
analytical coverage of the core pack (correlation, composition, variance,
and model-validation views). Visual language is shared with
build_executive_graphs.py.

Run:
    python src/visualization/build_supplementary_graphs.py [--dpi 200]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "matplotlib-cache"))
os.environ.setdefault("MPLBACKEND", "Agg")

import matplotlib as mpl
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

mpl.use("Agg")
import matplotlib.font_manager as fm  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Register the same bundled Inter used by the PDF report chrome and the
# executive chart pack, so every chart in the project shares one typographic
# system regardless of what fonts happen to be installed on the machine.
_REPO_ROOT = Path(__file__).resolve().parents[2]
for _weight_file in ("Inter-Regular.ttf", "Inter-SemiBold.ttf"):
    fm.fontManager.addfont(str(_REPO_ROOT / "assets" / "fonts" / _weight_file))

# Shared style ---------------------------------------------------------------
BG = "#fafaf7"
INK = "#1d1d1b"  # matches the PDF report's body-text INK and build_executive_graphs.py exactly
ACCENT = "#1f3b2d"
ACCENT_LIGHT = "#2d5a42"
NEG = "#9c2b1b"
NEUTRAL = "#6b6f76"
GRID = "#e8e8e3"
BORDER = "#d8d8d3"
AMBER = "#b07d2b"
BLUE = "#3a7abf"

mpl.rcParams["font.family"] = "sans-serif"
mpl.rcParams["font.sans-serif"] = ["Inter"]


def apply_style(ax: plt.Axes) -> None:
    ax.set_facecolor(BG)
    ax.figure.set_facecolor(BG)
    ax.grid(axis="y", color=GRID, linewidth=0.6, zorder=0)
    ax.grid(axis="x", visible=False)
    ax.tick_params(colors=NEUTRAL, labelsize=10)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color(NEUTRAL)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.spines["bottom"].set_visible(True)
    ax.spines["bottom"].set_color(BORDER)
    ax.yaxis.set_tick_params(length=0)
    ax.xaxis.set_tick_params(length=3, color=BORDER)


def title_block(ax: plt.Axes, title: str, subtitle: str | None = None) -> None:
    ax.set_title(title, fontsize=14, color=INK, pad=22 if subtitle else 12, loc="left", fontweight="semibold")
    if subtitle:
        ax.text(0.0, 1.02, subtitle, transform=ax.transAxes, fontsize=10.5, color=NEUTRAL, ha="left", va="bottom")


def fmt_pct(x: float, pos: int = 0) -> str:  # noqa: ARG001
    return f"{x * 100:.1f}%"


def save(fig: plt.Figure, path: Path, dpi: int) -> None:
    fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    log.info("  saved -> %s", path.name)


# ---------------------------------------------------------------------------
# 11. Discount intensity vs forward 3-month churn (correlation / risk)
# ---------------------------------------------------------------------------
def chart_discount_band_churn(metrics: dict, out: Path, dpi: int) -> None:
    log.info("discount band vs forward churn")
    rows = metrics["section3"]["discount_future_churn_3m"]
    labels = [r["discount_band"] for r in rows]
    rates = [r["future_churn_3m_rate"] for r in rows]
    counts = [r["rows"] for r in rows]
    colors = [NEUTRAL if label != ">30%" else NEG for label in labels]

    fig, ax = plt.subplots(figsize=(9.4, 5.4))
    bars = ax.bar(labels, rates, color=colors, width=0.62, zorder=3)
    apply_style(ax)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.set_ylim(0, max(rates) * 1.25)
    for b, r, n in zip(bars, rates, counts, strict=True):
        ax.text(
            b.get_x() + b.get_width() / 2,
            r + max(rates) * 0.02,
            f"{r * 100:.2f}%",
            ha="center",
            va="bottom",
            fontsize=11,
            color=INK,
            fontweight="medium",
        )
        ax.text(b.get_x() + b.get_width() / 2, r * 0.5, f"n={n:,}", ha="center", va="center", fontsize=9, color="white")
    title_block(
        ax,
        "Deep discounting carries the highest forward churn",
        "Forward 3-month logo churn rate by current discount band, simulated panel",
    )
    ax.set_xlabel("Discount band (share off list)", fontsize=10.5, color=NEUTRAL, labelpad=8)
    save(fig, out / "discount_band_vs_forward_churn.png", dpi)


# ---------------------------------------------------------------------------
# 12. Logo churn by segment and acquisition channel (composition / ranking)
# ---------------------------------------------------------------------------
def chart_churn_segment_channel(metrics: dict, out: Path, dpi: int) -> None:
    log.info("churn by segment & channel")
    seg = metrics["section2"]["churn_segment"]
    ch = metrics["section2"]["churn_channel"]
    overall = metrics["section2"]["overall_logo_churn_rate"]

    fig, axes = plt.subplots(1, 2, figsize=(12.6, 5.6))

    # segment
    s_lab = [r["segment"] for r in seg]
    s_val = [r["logo_churn_rate"] for r in seg]
    ax = axes[0]
    bars = ax.barh(s_lab[::-1], s_val[::-1], color=ACCENT, height=0.55, zorder=3)
    bars[-1].set_color(NEG)  # SMB highest after reverse -> last is SMB
    apply_style(ax)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.axvline(overall, color=NEUTRAL, linestyle="--", linewidth=1, zorder=2)
    ax.text(overall, 2.5, f" overall {overall * 100:.2f}%", color=NEUTRAL, fontsize=8.5, va="top")
    for b, v in zip(bars, s_val[::-1], strict=True):
        ax.text(v + 0.0003, b.get_y() + b.get_height() / 2, f"{v * 100:.2f}%", va="center", fontsize=10, color=INK)
    ax.set_xlim(0, max(s_val) * 1.25)
    title_block(ax, "By segment", None)

    # channel
    ch_sorted = sorted(ch, key=lambda r: r["logo_churn_rate"])
    c_lab = [r["acquisition_channel"].replace("_", " ") for r in ch_sorted]
    c_val = [r["logo_churn_rate"] for r in ch_sorted]
    ax = axes[1]
    colors = [ACCENT] * len(c_val)
    colors[-1] = NEG
    bars = ax.barh(c_lab, c_val, color=colors, height=0.6, zorder=3)
    apply_style(ax)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.axvline(overall, color=NEUTRAL, linestyle="--", linewidth=1, zorder=2)
    for b, v in zip(bars, c_val, strict=True):
        ax.text(v + 0.0002, b.get_y() + b.get_height() / 2, f"{v * 100:.2f}%", va="center", fontsize=9.5, color=INK)
    ax.set_xlim(0, max(c_val) * 1.2)
    title_block(ax, "By acquisition channel", None)

    fig.suptitle(
        "Churn concentrates in SMB and self-serve cohorts",
        fontsize=14,
        x=0.09,
        ha="left",
        y=1.02,
        color=INK,
        fontweight="semibold",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    save(fig, out / "logo_churn_by_segment_channel.png", dpi)


# ---------------------------------------------------------------------------
# 13. Score decile calibration / lift (model validation, funnel-like)
# ---------------------------------------------------------------------------
def chart_decile_calibration(base: Path, out: Path, dpi: int) -> None:
    log.info("score decile calibration")
    df = pd.read_csv(base / "data/processed/scoring_backtest_calibration_by_decile.csv")
    df = df.sort_values("score_decile")
    overall = (df["forward_churn_rate"] * df["observations"]).sum() / df["observations"].sum()

    fig, ax = plt.subplots(figsize=(10.2, 5.6))
    x = df["score_decile"].astype(int)
    colors = [NEUTRAL] * len(df)
    colors[-1] = NEG
    ax.bar(x, df["forward_churn_rate"], color=colors, width=0.66, zorder=3)
    apply_style(ax)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.axhline(overall, color=ACCENT, linestyle="--", linewidth=1.2, zorder=2)
    ax.text(
        0.0,
        overall,
        f" overall {overall * 100:.2f}% ",
        color=ACCENT,
        fontsize=9,
        va="center",
        zorder=4,
        bbox={"boxstyle": "square,pad=0.25", "facecolor": BG, "edgecolor": "none"},
    )
    top = df["forward_churn_rate"].iloc[-1]
    ax.text(
        len(df) - 1,
        top + 0.0015,
        f"{top * 100:.1f}%\n{df['lift_vs_overall'].iloc[-1]:.1f}x lift",
        ha="center",
        va="bottom",
        fontsize=10,
        color=NEG,
        fontweight="medium",
    )
    ax.set_ylim(0, top * 1.28)
    bottom = df["forward_churn_rate"].iloc[0]
    ratio = top / bottom
    title_block(
        ax,
        f"The top risk decile churns {ratio:.1f}x the bottom decile",
        "Realized forward 3-month churn by model score decile (1 = lowest risk, 10 = highest)",
    )
    ax.set_xlabel("Churn-risk score decile", fontsize=10.5, color=NEUTRAL, labelpad=8)
    save(fig, out / "score_decile_calibration.png", dpi)


# ---------------------------------------------------------------------------
# 14. Scenario ARR variance vs base case (variance / tornado)
# ---------------------------------------------------------------------------
def chart_scenario_variance(base: Path, out: Path, dpi: int) -> None:
    log.info("scenario ARR variance vs base")
    df = pd.read_csv(base / "data/processed/mrr_scenario_table.csv")
    df = df[df["scenario"] != "base_case"].copy()
    label_map = {
        "improvement_case": "Healthy-growth (bull)",
        "discount_discipline_improvement_case": "Discount discipline",
        "risk_adjusted_case": "Risk-adjusted",
        "downside_case": "Fragile-growth (bear)",
    }
    df["lab"] = df["scenario"].map(label_map)
    df = df.sort_values("arr_vs_base")
    vals = df["arr_vs_base"].values / 1e6
    colors = [NEG if v < 0 else ACCENT for v in vals]

    fig, ax = plt.subplots(figsize=(10.2, 5.2))
    bars = ax.barh(df["lab"], vals, color=colors, height=0.6, zorder=3)
    apply_style(ax)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.axvline(0, color=NEUTRAL, linewidth=1)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, p: f"{v:+.1f}M"))
    for b, v in zip(bars, vals, strict=True):
        off = 0.15 if v >= 0 else -0.15
        ax.text(
            v + off,
            b.get_y() + b.get_height() / 2,
            f"${v:+.1f}M",
            va="center",
            ha="left" if v >= 0 else "right",
            fontsize=10.5,
            color=INK,
            fontweight="medium",
        )
    ax.set_xlim(min(vals) * 1.35, max(vals) * 1.45)
    title_block(
        ax,
        "Six-month ARR swing spans roughly $13M across scenarios",
        "End-of-horizon ARR difference versus base case (Feb 2026 start)",
    )
    ax.set_xlabel("ARR vs base case", fontsize=10.5, color=NEUTRAL, labelpad=8)
    save(fig, out / "scenario_arr_variance_vs_base.png", dpi)


# ---------------------------------------------------------------------------
# 15. Account-manager discount vs churn (correlation / scatter)
# ---------------------------------------------------------------------------
def chart_am_discount_churn(base: Path, out: Path, dpi: int) -> None:
    log.info("account-manager discount vs churn")
    df = pd.read_csv(base / "data/processed/account_manager_summary.csv")
    x = df["avg_discount"].values
    y = df["churn_rate"].values
    sizes = (df["portfolio_mrr"].values / df["portfolio_mrr"].max()) * 520 + 30

    fig, ax = plt.subplots(figsize=(10.2, 6.0))
    # quadrant reference lines at medians
    mx, my = np.median(x), np.median(y)
    ax.axvline(mx, color=GRID, linewidth=1, zorder=1)
    ax.axhline(my, color=GRID, linewidth=1, zorder=1)
    # highlight high-discount/high-churn managers (top-right quadrant)
    hot = (x >= mx) & (y >= my)
    ax.scatter(x[~hot], y[~hot], s=sizes[~hot], color=NEUTRAL, alpha=0.4, edgecolor="white", linewidth=0.6, zorder=3)
    ax.scatter(x[hot], y[hot], s=sizes[hot], color=NEG, alpha=0.78, edgecolor="white", linewidth=0.6, zorder=4)
    apply_style(ax)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    # Label the named review-priority managers from the analysis memo. AM0029 and
    # AM0040 sit almost on top of each other (adjacent discount/churn values, and
    # AM0040's bubble is the largest on the chart), so a single fixed offset makes
    # the two labels collide. Each flagged label gets a hand-placed offset instead,
    # chosen to clear both the neighbouring bubble and the neighbouring label.
    flagged_offsets = {
        "AM0040": ((16, -10), "left"),
        "AM0029": ((-10, 14), "right"),
        "AM0014": ((10, -4), "left"),
        "AM0008": ((-10, 8), "right"),
    }
    for _, row in df.iterrows():
        mgr_id = row["account_manager_id"]
        if mgr_id in flagged_offsets:
            offset, ha = flagged_offsets[mgr_id]
            ax.annotate(
                mgr_id,
                (row["avg_discount"], row["churn_rate"]),
                textcoords="offset points",
                xytext=offset,
                ha=ha,
                fontsize=8.5,
                color=NEG,
            )
    r = np.corrcoef(x, y)[0, 1]
    ax.text(
        0.98,
        0.04,
        f"Portfolio-level corr {r:+.2f} (weak): discount alone does not rank managers",
        transform=ax.transAxes,
        ha="right",
        fontsize=9,
        color=NEUTRAL,
    )
    ax.text(mx + 0.001, ax.get_ylim()[1] * 0.985, "High discount + high churn", va="top", fontsize=9.5, color=NEG)
    title_block(
        ax,
        "A cluster of manager portfolios pairs heavy discounts with high churn",
        "Each bubble is one of 40 account managers; size = portfolio MRR. Labelled = review priority",
    )
    ax.set_xlabel("Average portfolio discount", fontsize=10.5, color=NEUTRAL, labelpad=8)
    ax.set_ylabel("Portfolio churn rate", fontsize=10.5, color=NEUTRAL, labelpad=8)
    save(fig, out / "account_manager_discount_vs_churn.png", dpi)


# ---------------------------------------------------------------------------
# 16. Expansion quality mix (composition)
# ---------------------------------------------------------------------------
def chart_expansion_quality_mix(metrics: dict, out: Path, dpi: int) -> None:
    log.info("expansion quality mix")
    rows = metrics["section4"]["expansion_quality_summary"]
    order = {"healthy": 0, "watch": 1, "fragile": 2}
    rows = sorted(rows, key=lambda r: order[r["expansion_quality"]])
    labels = [r["expansion_quality"].capitalize() for r in rows]
    mrr = [r["expansion_mrr"] for r in rows]
    events = [r["events"] for r in rows]
    total = sum(mrr)
    colors = {"Healthy": ACCENT, "Watch": AMBER, "Fragile": NEG}

    fig, ax = plt.subplots(figsize=(10.4, 3.4))
    left = 0
    for lab, v, n in zip(labels, mrr, events, strict=True):
        ax.barh(0, v, left=left, color=colors[lab], height=0.5, zorder=3, edgecolor=BG, linewidth=2)
        share = v / total
        ax.text(
            left + v / 2,
            0,
            f"{lab}\n{share * 100:.0f}%   ${v / 1e3:.0f}K   {n:,} events",
            ha="center",
            va="center",
            color="white",
            fontsize=10.5,
            fontweight="medium",
        )
        left += v
    ax.set_xlim(0, total)
    ax.set_ylim(-0.45, 1.7)
    ax.axis("off")
    ax.figure.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.text(
        0,
        1.42,
        "Fragile growth is 28% of expansion MRR",
        fontsize=14,
        color=INK,
        ha="left",
        va="center",
        fontweight="semibold",
    )
    ax.text(
        0,
        1.05,
        f"Composition of ${total / 1e6:.2f}M expansion MRR over the 36-month window by quality flag",
        fontsize=10.5,
        color=NEUTRAL,
        ha="left",
        va="center",
    )
    save(fig, out / "expansion_quality_mix.png", dpi)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", default=".")
    ap.add_argument("--dpi", type=int, default=200)
    args = ap.parse_args()

    base = Path(args.base_dir).resolve()
    out = base / "outputs" / "graphs"
    out.mkdir(parents=True, exist_ok=True)

    with open(base / "reports" / "main_business_analysis_metrics.json") as fh:
        metrics = json.load(fh)

    log.info("Generating supplementary charts -> %s", out)
    chart_discount_band_churn(metrics, out, args.dpi)
    chart_churn_segment_channel(metrics, out, args.dpi)
    chart_decile_calibration(base, out, args.dpi)
    chart_scenario_variance(base, out, args.dpi)
    chart_am_discount_churn(base, out, args.dpi)
    chart_expansion_quality_mix(metrics, out, args.dpi)
    log.info("Supplementary charts complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

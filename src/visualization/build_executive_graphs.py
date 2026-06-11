"""
Executive-ready PNG charts for the B2B SaaS Revenue Quality project.

Generates 10 charts under outputs/graphs/ for use in the HTML dashboard,
PDF reports, and slide decks. All charts use a consistent visual style:
  - Background: #fafaf7 (warm off-white)
  - Accent:     #1f3b2d (forest green)
  - Negative:   #9c2b1b (oxblood)
  - Neutral:    #6b6f76 (slate)
  - Grid:       #e8e8e3 (barely visible)

Run:
    python src/visualization/build_executive_graphs.py [--base-dir .] [--dpi 150]
"""

from __future__ import annotations

import argparse
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
import seaborn as sns

mpl.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.metrics import build_monthly_retention  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Style constants
# ---------------------------------------------------------------------------
BG = "#fafaf7"
ACCENT = "#1f3b2d"
ACCENT_LIGHT = "#2d5a42"
NEG = "#9c2b1b"
NEUTRAL = "#6b6f76"
GRID = "#e8e8e3"
BORDER = "#d8d8d3"

PALETTE = {
    "Low": "#2d5a42",
    "Moderate": "#b07d2b",
    "High": "#9c2b1b",
    "Critical": "#5c1010",
    "healthy": "#2d5a42",
    "watch": "#b07d2b",
    "fragile": "#9c2b1b",
}

SCENARIO_COLORS = {
    "base_case": ACCENT,
    "downside_case": NEG,
    "improvement_case": "#2d5a42",
    "discount_discipline_improvement_case": "#3a7abf",
    "risk_adjusted_case": "#b07d2b",
}

SCENARIO_LABELS = {
    "base_case": "Base",
    "downside_case": "Bear",
    "improvement_case": "Bull",
    "discount_discipline_improvement_case": "Discount-discipline",
    "risk_adjusted_case": "Risk-adjusted",
}


def apply_style(ax: plt.Axes, spines: bool = True) -> None:
    """Apply the shared visual style to an axes object."""
    ax.set_facecolor(BG)
    ax.figure.set_facecolor(BG)
    ax.grid(axis="y", color=GRID, linewidth=0.6, zorder=0)
    ax.grid(axis="x", visible=False)
    ax.tick_params(colors=NEUTRAL, labelsize=10)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color(NEUTRAL)
    if spines:
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.spines["bottom"].set_visible(True)
        ax.spines["bottom"].set_color(BORDER)
    ax.yaxis.set_tick_params(length=0)
    ax.xaxis.set_tick_params(length=3, color=BORDER)


def fmt_currency(x: float, pos: int = 0) -> str:  # noqa: ARG001
    if abs(x) >= 1e6:
        return f"${x / 1e6:.1f}M"
    if abs(x) >= 1e3:
        return f"${x / 1e3:.0f}K"
    return f"${x:.0f}"


def fmt_pct(x: float, pos: int = 0) -> str:  # noqa: ARG001
    return f"{x * 100:.1f}%"


def save(fig: plt.Figure, path: Path, dpi: int = 150) -> None:
    fig.savefig(path, dpi=dpi, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    log.info("  saved → %s", path.name)


# ---------------------------------------------------------------------------
# 1. MRR / ARR growth trend
# ---------------------------------------------------------------------------
def chart_mrr_arr_growth(base: Path, out: Path, dpi: int) -> None:
    log.info("1/10  MRR / ARR growth trend")
    rq = pd.read_csv(base / "data/processed/account_monthly_revenue_quality.csv", parse_dates=["month"])
    monthly = rq.groupby("month")["active_mrr"].sum().reset_index()
    monthly["arr"] = monthly["active_mrr"] * 12

    fig, ax = plt.subplots(figsize=(12, 5.5))
    apply_style(ax)

    ax.fill_between(monthly["month"], monthly["active_mrr"], alpha=0.08, color=ACCENT, zorder=1)
    ax.plot(monthly["month"], monthly["active_mrr"], color=ACCENT, linewidth=2.2, zorder=2, label="MRR")

    ax2 = ax.twinx()
    ax2.set_facecolor(BG)
    for spine in ax2.spines.values():
        spine.set_visible(False)
    ax2.plot(
        monthly["month"],
        monthly["arr"],
        color=ACCENT_LIGHT,
        linewidth=1.4,
        linestyle="--",
        zorder=2,
        alpha=0.6,
        label="ARR (right axis)",
    )
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_currency))
    ax2.tick_params(colors=NEUTRAL, labelsize=10, length=0)
    for label in ax2.get_yticklabels():
        label.set_color(NEUTRAL)
    ax2.spines["right"].set_visible(True)
    ax2.spines["right"].set_color(BORDER)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_currency))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mpl.dates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

    # CAGR annotation
    n_months = len(monthly) - 1
    cagr = (monthly["arr"].iloc[-1] / monthly["arr"].iloc[0]) ** (12 / n_months) - 1
    ax.annotate(
        f"ARR CAGR  {cagr * 100:.1f}%",
        xy=(monthly["month"].iloc[-1], monthly["active_mrr"].iloc[-1]),
        xytext=(-110, 20),
        textcoords="offset points",
        fontsize=11,
        color=ACCENT,
        fontweight="semibold",
    )

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, frameon=False, fontsize=10, loc="upper left", labelcolor=NEUTRAL)

    ax.set_title(
        "Monthly Recurring Revenue — 36-Month View", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14
    )
    ax.set_xlabel("")
    ax.set_ylabel("MRR", fontsize=10, color=NEUTRAL)
    fig.tight_layout()
    save(fig, out / "mrr_arr_growth_trend.png", dpi)


# ---------------------------------------------------------------------------
# 2. GRR / NRR retention trend
# ---------------------------------------------------------------------------
def chart_grr_nrr_trend(base: Path, out: Path, dpi: int) -> None:
    log.info("2/10  GRR / NRR retention trend")
    rq = pd.read_csv(base / "data/processed/account_monthly_revenue_quality.csv", parse_dates=["month"])
    m = pd.read_csv(base / "data/raw/monthly_account_metrics.csv", parse_dates=["month"])

    df = build_monthly_retention(rq, m)

    # 3-month rolling to smooth noise
    df["grr_smooth"] = df["grr"].rolling(3, center=True, min_periods=1).mean()
    df["nrr_smooth"] = df["nrr"].rolling(3, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 5.5))
    apply_style(ax)

    ax.axhline(1.0, color=GRID, linewidth=1.0, linestyle="--", zorder=1)
    ax.annotate(
        "100% benchmark",
        xy=(df["month"].iloc[1], 1.0),
        xytext=(4, 6),
        textcoords="offset points",
        fontsize=9,
        color=NEUTRAL,
    )

    ax.plot(df["month"], df["grr_smooth"], color=NEG, linewidth=2.2, label="GRR (3m avg)", zorder=3)
    ax.plot(df["month"], df["nrr_smooth"], color=ACCENT, linewidth=2.2, label="NRR (3m avg)", zorder=3)

    ax.fill_between(df["month"], df["grr_smooth"], 1.0, where=df["grr_smooth"] < 1.0, alpha=0.06, color=NEG, zorder=2)
    ax.fill_between(
        df["month"], df["nrr_smooth"], 1.0, where=df["nrr_smooth"] > 1.0, alpha=0.06, color=ACCENT, zorder=2
    )

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x * 100:.1f}%"))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mpl.dates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL)
    ax.set_title("Gross & Net Revenue Retention — Monthly", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14)
    ax.set_xlabel("")
    ax.set_ylabel("Retention rate", fontsize=10, color=NEUTRAL)
    fig.tight_layout()
    save(fig, out / "grr_nrr_retention_trend.png", dpi)


# ---------------------------------------------------------------------------
# 3. Logo churn and revenue churn
# ---------------------------------------------------------------------------
def chart_logo_revenue_churn(base: Path, out: Path, dpi: int) -> None:
    log.info("3/10  Logo churn & revenue churn")
    rq = pd.read_csv(base / "data/processed/account_monthly_revenue_quality.csv", parse_dates=["month"])
    m = pd.read_csv(base / "data/raw/monthly_account_metrics.csv", parse_dates=["month"])

    df = build_monthly_retention(rq, m).rename(columns={"revenue_churn_rate": "rev_churn_rate"})
    df["logo_smooth"] = df["logo_churn_rate"].rolling(3, center=True, min_periods=1).mean()
    df["rev_smooth"] = df["rev_churn_rate"].rolling(3, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 5.5))
    apply_style(ax)

    ax.bar(
        df["month"], df["logo_churn_rate"], width=20, alpha=0.18, color=NEUTRAL, zorder=1, label="Logo churn (monthly)"
    )
    ax.bar(
        df["month"], df["rev_churn_rate"], width=20, alpha=0.22, color=NEG, zorder=1, label="Revenue churn (monthly)"
    )
    ax.plot(df["month"], df["logo_smooth"], color=NEUTRAL, linewidth=2.0, zorder=3, label="Logo churn (3M avg)")
    ax.plot(df["month"], df["rev_smooth"], color=NEG, linewidth=2.0, zorder=3, label="Revenue churn (3M avg)")

    avg_logo = df["logo_smooth"].mean()
    avg_rev = df["rev_smooth"].mean()
    ax.axhline(avg_logo, color=NEUTRAL, linewidth=0.8, linestyle=":", alpha=0.7)
    ax.axhline(avg_rev, color=NEG, linewidth=0.8, linestyle=":", alpha=0.7)

    # End-point labels
    last = df.iloc[-1]
    ax.annotate(
        f"Logo 3M avg {last.logo_smooth * 100:.2f}%",
        xy=(last["month"], last["logo_smooth"]),
        xytext=(6, 0),
        textcoords="offset points",
        fontsize=9.5,
        color=NEUTRAL,
        va="center",
    )
    ax.annotate(
        f"Revenue 3M avg {last.rev_smooth * 100:.2f}%",
        xy=(last["month"], last["rev_smooth"]),
        xytext=(6, -12),
        textcoords="offset points",
        fontsize=9.5,
        color=NEG,
        va="center",
    )

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mpl.dates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL, loc="upper left")
    ax.set_title(
        "Logo Churn vs Revenue Churn — Monthly Rate", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14
    )
    ax.set_xlabel("")
    ax.set_ylabel("Monthly churn rate", fontsize=10, color=NEUTRAL)
    fig.tight_layout()
    save(fig, out / "logo_churn_and_revenue_churn.png", dpi)


# ---------------------------------------------------------------------------
# 4. Revenue quality score distribution
# ---------------------------------------------------------------------------
def chart_revenue_quality_dist(base: Path, out: Path, dpi: int) -> None:
    log.info("4/10  Revenue quality score distribution")
    sc = pd.read_csv(base / "data/processed/account_scoring_model_output.csv")

    tier_order = ["Critical", "High", "Moderate", "Low"]
    tier_colors = [PALETTE[t] for t in tier_order]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    apply_style(ax)

    # Compute thresholds from scoring_utils logic: 0-25=healthy, 25-50=watch, 50+=fragile
    # For revenue quality (inverted — higher score = worse quality)
    bins = np.arange(0, 105, 5)
    for tier, color in zip(tier_order, tier_colors, strict=True):
        subset = sc[sc["revenue_quality_risk_tier"] == tier]["revenue_quality_score"]
        ax.hist(
            subset,
            bins=bins,
            color=color,
            alpha=0.75,
            edgecolor=BG,
            linewidth=0.4,
            label=f"{tier} ({len(subset):,})",
            zorder=2,
        )

    ax.axvline(25, color=NEUTRAL, linewidth=1.0, linestyle="--", alpha=0.6, zorder=3)
    ax.axvline(50, color=NEUTRAL, linewidth=1.0, linestyle="--", alpha=0.6, zorder=3)
    ax.text(12.5, ax.get_ylim()[1] * 0.92, "Healthy", ha="center", fontsize=9, color=PALETTE["Low"])
    ax.text(37.5, ax.get_ylim()[1] * 0.92, "Watch", ha="center", fontsize=9, color=PALETTE["Moderate"])
    ax.text(75, ax.get_ylim()[1] * 0.92, "Fragile", ha="center", fontsize=9, color=PALETTE["High"])

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL, title="Quality tier", title_fontsize=9)
    ax.set_title(
        "Revenue Quality Score — Account Distribution", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14
    )
    ax.set_xlabel("Revenue quality risk score (0 = healthy, 100 = fragile)", fontsize=10, color=NEUTRAL)
    ax.set_ylabel("Number of accounts", fontsize=10, color=NEUTRAL)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(10))
    fig.tight_layout()
    save(fig, out / "revenue_quality_score_distribution.png", dpi)


# ---------------------------------------------------------------------------
# 5. Churn risk score distribution
# ---------------------------------------------------------------------------
def chart_churn_risk_dist(base: Path, out: Path, dpi: int) -> None:
    log.info("5/10  Churn risk score distribution")
    sc = pd.read_csv(base / "data/processed/account_scoring_model_output.csv")

    tier_order = ["Critical", "High", "Moderate", "Low"]
    tier_colors = [PALETTE[t] for t in tier_order]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    apply_style(ax)

    bins = np.arange(0, 105, 5)
    for tier, color in zip(tier_order, tier_colors, strict=True):
        subset = sc[sc["churn_risk_tier"] == tier]["churn_risk_score"]
        ax.hist(
            subset,
            bins=bins,
            color=color,
            alpha=0.75,
            edgecolor=BG,
            linewidth=0.4,
            label=f"{tier}  ({len(subset):,})",
            zorder=2,
        )

    # Tier boundary lines: Low<30, Moderate 30-55, High 55-75, Critical 75+
    for cutoff, label in [(30, "30"), (55, "55"), (75, "75")]:
        ax.axvline(cutoff, color=NEUTRAL, linewidth=1.0, linestyle="--", alpha=0.5, zorder=3)
        ax.text(cutoff + 1, ax.get_ylim()[1] * 0.97, label, fontsize=8.5, color=NEUTRAL, ha="left")

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL, title="Churn risk tier", title_fontsize=9)
    ax.set_title("Churn Risk Score — Account Distribution", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14)
    ax.set_xlabel("Churn risk score (0 = low risk, 100 = critical)", fontsize=10, color=NEUTRAL)
    ax.set_ylabel("Number of accounts", fontsize=10, color=NEUTRAL)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(10))
    fig.tight_layout()
    save(fig, out / "churn_risk_score_distribution.png", dpi)


# ---------------------------------------------------------------------------
# 6. Discount dependency trend
# ---------------------------------------------------------------------------
def chart_discount_trend(base: Path, out: Path, dpi: int) -> None:
    log.info("6/10  Discount dependency trend")
    rq = pd.read_csv(base / "data/processed/account_monthly_revenue_quality.csv", parse_dates=["month"])

    monthly = (
        rq.groupby("month")
        .agg(
            total_mrr=("active_mrr", "sum"),
            discount_dep_mrr=("active_mrr", lambda x: x[rq.loc[x.index, "discount_dependency_flag"] == 1].sum()),
        )
        .reset_index()
    )
    monthly["discount_share"] = monthly["discount_dep_mrr"] / monthly["total_mrr"]

    fig, ax = plt.subplots(figsize=(12, 5.5))
    apply_style(ax)

    ax.fill_between(monthly["month"], monthly["discount_share"], alpha=0.12, color=NEG, zorder=1)
    ax.plot(monthly["month"], monthly["discount_share"], color=NEG, linewidth=2.2, zorder=2)

    # Trend line (simple linear fit)
    x_num = (monthly["month"] - monthly["month"].min()).dt.days.values
    z = np.polyfit(x_num, monthly["discount_share"].values, 1)
    p = np.poly1d(z)
    trend = p(x_num)
    ax.plot(
        monthly["month"], trend, color=NEUTRAL, linewidth=1.2, linestyle="--", alpha=0.6, label="Trend (linear fit)"
    )

    latest = monthly.iloc[-1]
    ax.annotate(
        f"{latest.discount_share * 100:.1f}% of MRR\nis discount-reliant",
        xy=(latest["month"], latest["discount_share"]),
        xytext=(-130, 20),
        textcoords="offset points",
        fontsize=11,
        color=NEG,
        fontweight="semibold",
        arrowprops={"arrowstyle": "-", "color": NEUTRAL, "alpha": 0.5},
    )

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_pct))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mpl.dates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL)
    ax.set_title("Discount-Reliant MRR Share — Monthly", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14)
    ax.set_xlabel("")
    ax.set_ylabel("Share of total MRR", fontsize=10, color=NEUTRAL)
    fig.tight_layout()
    save(fig, out / "discount_dependency_trend.png", dpi)


# ---------------------------------------------------------------------------
# 7. At-risk MRR concentration curve
# ---------------------------------------------------------------------------
def chart_at_risk_concentration(base: Path, out: Path, dpi: int) -> None:
    log.info("7/10  At-risk MRR concentration curve")
    sc = pd.read_csv(base / "data/processed/account_scoring_model_output.csv")

    at_risk = sc[sc["governance_priority_tier"].isin(["High", "Critical"])].copy()
    at_risk = at_risk.sort_values("governance_priority_score", ascending=False).reset_index(drop=True)
    at_risk["cumulative_mrr"] = at_risk["current_mrr"].cumsum()
    total_at_risk = at_risk["current_mrr"].sum()
    at_risk["cumulative_pct_mrr"] = at_risk["cumulative_mrr"] / total_at_risk
    at_risk["pct_accounts"] = (at_risk.index + 1) / len(at_risk)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    apply_style(ax)

    ax.plot(
        at_risk["pct_accounts"] * 100,
        at_risk["cumulative_pct_mrr"] * 100,
        color=ACCENT,
        linewidth=2.4,
        zorder=3,
        label="Concentration curve",
    )
    ax.fill_between(
        at_risk["pct_accounts"] * 100, at_risk["cumulative_pct_mrr"] * 100, alpha=0.07, color=ACCENT, zorder=2
    )

    # 45-degree equal distribution line
    ax.plot([0, 100], [0, 100], color=NEUTRAL, linewidth=1.0, linestyle="--", alpha=0.5, label="Equal distribution")

    # Annotate: top 20% accounts → X% of at-risk MRR
    idx_20 = int(len(at_risk) * 0.20) - 1
    share_at_20 = at_risk.iloc[idx_20]["cumulative_pct_mrr"] * 100
    ax.axvline(20, color=NEUTRAL, linewidth=0.8, linestyle=":", alpha=0.5, zorder=2)
    ax.axhline(share_at_20, color=NEUTRAL, linewidth=0.8, linestyle=":", alpha=0.5, zorder=2)
    ax.scatter([20], [share_at_20], color=NEG, s=55, zorder=4)
    ax.annotate(
        f"Top 20% accounts\n= {share_at_20:.0f}% of at-risk MRR",
        xy=(20, share_at_20),
        xytext=(10, -30),
        textcoords="offset points",
        fontsize=10,
        color=NEG,
        arrowprops={"arrowstyle": "-", "color": NEUTRAL, "alpha": 0.5},
    )

    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL)
    ax.set_title(
        f"At-Risk MRR Concentration  ·  {len(at_risk):,} accounts  ·  ${total_at_risk / 1e6:.1f}M at risk",
        fontsize=13,
        color="#0c0d0e",
        fontweight="semibold",
        pad=14,
    )
    ax.set_xlabel("Accounts ranked by governance priority (% of at-risk pool)", fontsize=10, color=NEUTRAL)
    ax.set_ylabel("Cumulative % of at-risk MRR", fontsize=10, color=NEUTRAL)
    fig.tight_layout()
    save(fig, out / "at_risk_mrr_concentration.png", dpi)


# ---------------------------------------------------------------------------
# 8. Scenario MRR trajectories
# ---------------------------------------------------------------------------
def chart_scenario_trajectories(base: Path, out: Path, dpi: int) -> None:
    log.info("8/10  Scenario MRR trajectories")
    rq = pd.read_csv(base / "data/processed/account_monthly_revenue_quality.csv", parse_dates=["month"])
    sc = pd.read_csv(base / "data/processed/scenario_mrr_trajectories.csv", parse_dates=["forecast_month"])

    # Historical MRR (actuals)
    historical = rq.groupby("month")["active_mrr"].sum().reset_index()
    historical.columns = ["month", "mrr"]

    fig, ax = plt.subplots(figsize=(13, 5.5))
    apply_style(ax)

    # Historical line
    ax.plot(historical["month"], historical["mrr"], color=NEUTRAL, linewidth=2.0, zorder=3, label="Actuals")
    ax.fill_between(historical["month"], historical["mrr"], alpha=0.06, color=NEUTRAL, zorder=1)

    # Connector dot between historical and forecast
    last_actual_date = historical["month"].iloc[-1]
    last_actual_mrr = historical["mrr"].iloc[-1]

    # Scenario forecasts
    displayed = ["base_case", "downside_case", "improvement_case", "discount_discipline_improvement_case"]
    end_labels: list[tuple[float, str, str]] = []

    for scenario in displayed:
        sub = sc[sc["scenario"] == scenario].sort_values("forecast_month")
        color = SCENARIO_COLORS[scenario]
        label = SCENARIO_LABELS[scenario]

        # Bridge from last actual to first forecast
        x = [last_actual_date] + list(sub["forecast_month"])
        y = [last_actual_mrr] + list(sub["forecast_mrr"])

        ax.plot(
            x,
            y,
            color=color,
            linewidth=1.8 if scenario != "base_case" else 2.4,
            linestyle="--" if scenario != "base_case" else "-",
            alpha=0.85,
            zorder=3,
            label=label,
        )

        end_labels.append((sub["forecast_mrr"].iloc[-1], label, color))

    # Collapse operationally indistinguishable endpoints before staggering labels.
    base_end = next(item for item in end_labels if item[1] == "Base")
    discount_end = next(item for item in end_labels if item[1] == "Discount-discipline")
    if abs(base_end[0] - discount_end[0]) < 25_000:
        end_labels = [item for item in end_labels if item[1] not in {"Base", "Discount-discipline"}]
        end_labels.append(
            (
                (base_end[0] + discount_end[0]) / 2,
                "Base / Discount-discipline",
                ACCENT,
            )
        )

    # Stagger remaining end labels when scenarios finish at similar values.
    end_labels.sort(key=lambda t: t[0])
    last_forecast_date = sc["forecast_month"].max()
    clusters: list[list[tuple[float, str, str]]] = []
    for end_label in end_labels:
        if not clusters or end_label[0] - clusters[-1][-1][0] >= 100_000:
            clusters.append([end_label])
        else:
            clusters[-1].append(end_label)

    label_offsets: dict[str, float] = {}
    for cluster in clusters:
        offsets = np.linspace(-12 * (len(cluster) - 1), 12 * (len(cluster) - 1), len(cluster))
        for (_, label, _), offset in zip(cluster, offsets, strict=True):
            label_offsets[label] = float(offset)

    for mrr_val, label, color in end_labels:
        ax.annotate(
            label,
            xy=(last_forecast_date, mrr_val),
            xytext=(8, label_offsets[label]),
            textcoords="offset points",
            fontsize=9.5,
            color=color,
            va="center",
        )

    ax.axvline(last_actual_date, color=NEUTRAL, linewidth=0.8, linestyle=":", alpha=0.5)
    ax.text(
        last_actual_date,
        ax.get_ylim()[0] * 1.02
        if ax.get_ylim()[0] > 0
        else (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02 + ax.get_ylim()[0],
        "  Forecast >>",
        fontsize=9,
        color=NEUTRAL,
    )

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_currency))
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mpl.dates.MonthLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

    ax.legend(frameon=False, fontsize=10, labelcolor=NEUTRAL, loc="upper left", ncol=2)
    ax.set_title(
        "MRR Scenario Trajectories — Actuals + 6-Month Forecast",
        fontsize=14,
        color="#0c0d0e",
        fontweight="semibold",
        pad=14,
    )
    ax.set_xlabel("")
    ax.set_ylabel("Monthly Recurring Revenue", fontsize=10, color=NEUTRAL)
    fig.tight_layout()
    save(fig, out / "scenario_mrr_trajectories.png", dpi)


# ---------------------------------------------------------------------------
# 9. Cohort retention heatmap
# ---------------------------------------------------------------------------
def chart_cohort_heatmap(base: Path, out: Path, dpi: int) -> None:
    log.info("9/10  Cohort retention heatmap")
    cr = pd.read_csv(base / "data/processed/cohort_retention_summary.csv", parse_dates=["cohort_month"])

    # Aggregate across segments / regions → overall cohort × month_number
    cohort_agg = (
        cr.groupby(["cohort_month", "month_number"])
        .agg(retained_revenue=("retained_revenue", "sum"), active_customers=("active_customers", "sum"))
        .reset_index()
    )

    # Cohort starting revenue (month_number == 0)
    cohort_start = cohort_agg[cohort_agg["month_number"] == 0][["cohort_month", "retained_revenue"]].rename(
        columns={"retained_revenue": "start_revenue"}
    )
    cohort_agg = cohort_agg.merge(cohort_start, on="cohort_month")
    cohort_agg["nrr"] = cohort_agg["retained_revenue"] / cohort_agg["start_revenue"]

    # Pivot: cohort months as rows, month_number as columns (up to 12 months for readability)
    max_months = 13
    pivot = cohort_agg[cohort_agg["month_number"] <= max_months].pivot(
        index="cohort_month", columns="month_number", values="nrr"
    )
    pivot.index = pivot.index.strftime("%b %Y")

    # Show most recent 18 cohorts (earlier ones don't have enough age)
    pivot = pivot.tail(18)

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.set_facecolor(BG)
    ax.set_facecolor(BG)

    # Custom diverging colormap: red → white → green
    cmap = sns.diverging_palette(10, 145, s=80, l=45, as_cmap=True)

    mask = pivot.isna()
    sns.heatmap(
        pivot,
        ax=ax,
        cmap=cmap,
        center=1.0,
        vmin=0.85,
        vmax=1.15,
        annot=True,
        fmt=".0%",
        annot_kws={"size": 8.5, "color": "#0c0d0e"},
        linewidths=0.5,
        linecolor=BG,
        cbar_kws={"shrink": 0.6, "label": "Net retention rate"},
        mask=mask,
    )

    ax.set_title(
        "Cohort Net Revenue Retention — Month 0 to 12", fontsize=14, color="#0c0d0e", fontweight="semibold", pad=14
    )
    ax.set_xlabel("Months since acquisition", fontsize=10, color=NEUTRAL)
    ax.set_ylabel("Cohort (acquisition month)", fontsize=10, color=NEUTRAL)
    ax.tick_params(colors=NEUTRAL, labelsize=9)

    # Style colorbar
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=9, colors=NEUTRAL)
    cbar.ax.yaxis.label.set_color(NEUTRAL)
    cbar.ax.yaxis.label.set_fontsize(9)

    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)
    plt.setp(ax.yaxis.get_majorticklabels(), rotation=0)

    fig.tight_layout()
    save(fig, out / "cohort_retention_heatmap.png", dpi)


# ---------------------------------------------------------------------------
# 10. Governance priority accounts
# ---------------------------------------------------------------------------
def chart_governance_priority_accounts(base: Path, out: Path, dpi: int) -> None:
    log.info("10/10  Governance priority accounts")
    sc = pd.read_csv(base / "data/processed/account_scoring_model_output.csv")

    # Top 20 highest governance priority accounts
    top = (
        sc[sc["governance_priority_tier"].isin(["High", "Critical", "Moderate"])]
        .sort_values("governance_priority_score", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )
    top["label"] = pd.Categorical("#" + top["customer_id"].str[-6:])  # prefix ensures string not parsed as number

    fig, ax = plt.subplots(figsize=(12, 6.5))
    apply_style(ax)
    ax.grid(axis="x", color=GRID, linewidth=0.6, zorder=0)
    ax.grid(axis="y", visible=False)

    bars = ax.barh(
        top["label"][::-1],
        top["current_mrr"][::-1],
        color=[PALETTE[t] for t in top["governance_priority_tier"][::-1]],
        alpha=0.80,
        edgecolor=BG,
        linewidth=0.4,
        zorder=2,
        height=0.65,
    )

    # Score annotations on bars
    for bar, score, tier in zip(
        bars,
        top["governance_priority_score"][::-1],
        top["governance_priority_tier"][::-1],
        strict=True,
    ):
        ax.text(
            bar.get_width() + top["current_mrr"].max() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{score:.0f}  {tier}",
            va="center",
            ha="left",
            fontsize=9,
            color=PALETTE[tier],
        )

    # Legend patches
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor=PALETTE[t], alpha=0.8, label=t)
        for t in ["Critical", "High", "Moderate"]
        if t in top["governance_priority_tier"].values
    ]
    ax.legend(
        handles=legend_elements,
        frameon=False,
        fontsize=10,
        labelcolor=NEUTRAL,
        title="Governance tier",
        title_fontsize=9,
    )

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_currency))
    ax.set_title(
        "Top 20 Accounts by Governance Priority — Current MRR & Risk Score",
        fontsize=13,
        color="#0c0d0e",
        fontweight="semibold",
        pad=14,
    )
    ax.set_xlabel("Current MRR", fontsize=10, color=NEUTRAL)
    ax.set_ylabel("")
    fig.tight_layout()
    save(fig, out / "governance_priority_accounts.png", dpi)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate executive PNG charts.")
    parser.add_argument("--base-dir", default=".", help="Project root directory")
    parser.add_argument("--dpi", type=int, default=150, help="Output DPI (default: 150)")
    args = parser.parse_args(argv)

    base = Path(args.base_dir).resolve()
    out = base / "outputs" / "graphs"
    out.mkdir(parents=True, exist_ok=True)

    mpl.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Inter", "Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.facecolor": BG,
            "axes.facecolor": BG,
            "text.color": "#0c0d0e",
            "font.size": 11,
            "axes.titlesize": 14,
            "axes.labelsize": 11,
        }
    )

    log.info("Generating executive charts → %s", out)

    chart_mrr_arr_growth(base, out, args.dpi)
    chart_grr_nrr_trend(base, out, args.dpi)
    chart_logo_revenue_churn(base, out, args.dpi)
    chart_revenue_quality_dist(base, out, args.dpi)
    chart_churn_risk_dist(base, out, args.dpi)
    chart_discount_trend(base, out, args.dpi)
    chart_at_risk_concentration(base, out, args.dpi)
    chart_scenario_trajectories(base, out, args.dpi)
    chart_cohort_heatmap(base, out, args.dpi)
    chart_governance_priority_accounts(base, out, args.dpi)

    generated = sorted(out.glob("*.png"))
    log.info("\nAll done — %d charts in %s", len(generated), out)
    for p in generated:
        size_kb = p.stat().st_size / 1024
        log.info("  %-52s  %5.0f KB", p.name, size_kb)

    return 0


if __name__ == "__main__":
    sys.exit(main())

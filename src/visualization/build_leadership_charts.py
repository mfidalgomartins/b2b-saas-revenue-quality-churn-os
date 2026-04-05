from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

# Prevent matplotlib/fontconfig cache permission warnings in restricted environments.
os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "matplotlib-cache"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter, PercentFormatter


# Colorblind-safe palette
COLORS = {
    "primary": "#1f77b4",
    "secondary": "#2ca02c",
    "accent": "#ff7f0e",
    "danger": "#d62728",
    "neutral": "#7f7f7f",
    "purple": "#9467bd",
    "teal": "#17becf",
}


def setup_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "figure.dpi": 130,
            "savefig.dpi": 160,
            "axes.titlesize": 15,
            "axes.labelsize": 11,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "legend.fontsize": 10,
            "font.family": "DejaVu Sans",
            "axes.titlepad": 12,
            "axes.edgecolor": "#E0E0E0",
            "grid.color": "#EAEAEA",
            "grid.linestyle": "-",
            "grid.linewidth": 0.8,
        }
    )


def fmt_millions(x: float, pos: int) -> str:
    return f"${x/1e6:.1f}M"


def fmt_pct(x: float, pos: int) -> str:
    return f"{100*x:.1f}%"


def save_fig(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def weighted_avg(df: pd.DataFrame, value_col: str, weight_col: str) -> float:
    weights = df[weight_col].astype(float)
    if len(df) == 0 or float(weights.sum()) <= 0:
        return 0.0
    return float(np.average(df[value_col].astype(float), weights=weights))


def load_data(base_dir: Path) -> Dict[str, pd.DataFrame]:
    raw = base_dir / "data/raw"
    processed = base_dir / "data/processed"

    data: Dict[str, pd.DataFrame] = {
        "monthly_quality": pd.read_csv(processed / "account_monthly_revenue_quality.csv", parse_dates=["month"]),
        "monthly_raw": pd.read_csv(raw / "monthly_account_metrics.csv", parse_dates=["month"]),
        "customers": pd.read_csv(raw / "customers.csv", parse_dates=["signup_date"]),
        "cohort_summary": pd.read_csv(processed / "cohort_retention_summary.csv", parse_dates=["cohort_month"]),
        "scoring": pd.read_csv(processed / "account_scoring_model_output.csv"),
        "health": pd.read_csv(processed / "customer_health_features.csv"),
        "manager_summary": pd.read_csv(processed / "account_manager_summary.csv"),
        "scenario_trajectories": pd.read_csv(processed / "scenario_mrr_trajectories.csv", parse_dates=["forecast_month"]),
    }
    return data


def build_core_panel(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    panel = data["monthly_quality"].merge(
        data["monthly_raw"][["customer_id", "month", "active_flag", "churn_flag", "product_usage_score", "payment_delay_days", "nps_score"]],
        on=["customer_id", "month"],
        how="left",
    )
    panel = panel.merge(
        data["customers"][["customer_id", "segment", "region", "industry", "acquisition_channel", "account_manager_id"]],
        on="customer_id",
        how="left",
    )
    return panel


def chart_mrr_arr_trend(panel: pd.DataFrame, out: Path) -> Dict[str, str]:
    monthly = panel.groupby("month", as_index=False)["active_mrr"].sum().rename(columns={"active_mrr": "mrr"})
    monthly["arr"] = monthly["mrr"] * 12

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(monthly["month"], monthly["mrr"], color=COLORS["primary"], linewidth=2.8, label="MRR")
    ax2 = ax.twinx()
    ax2.plot(monthly["month"], monthly["arr"], color=COLORS["secondary"], linewidth=2.2, linestyle="--", label="ARR")

    ax.set_title("Recurring revenue scaled strongly from 2023 to 2026, supporting sustained ARR expansion")
    ax.set_xlabel("Month")
    ax.set_ylabel("MRR")
    ax2.set_ylabel("ARR")
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_millions))
    ax2.yaxis.set_major_formatter(FuncFormatter(fmt_millions))

    start_mrr = monthly.iloc[0]["mrr"]
    end_mrr = monthly.iloc[-1]["mrr"]
    growth = (end_mrr / start_mrr - 1) if start_mrr > 0 else 0.0
    ax.annotate(
        f"MRR +{growth:.0%}",
        xy=(monthly.iloc[-1]["month"], end_mrr),
        xytext=(-80, 20),
        textcoords="offset points",
        arrowprops=dict(arrowstyle="->", color=COLORS["primary"]),
        fontsize=10,
        color=COLORS["primary"],
    )

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, frameon=False, loc="upper left")

    save_fig(fig, out / "01_mrr_arr_growth_trend.png")
    return {
        "file": "01_mrr_arr_growth_trend.png",
        "objective": "Show topline recurring revenue trajectory and scale-up quality over time.",
        "chart_type": "Dual-axis line chart (MRR and ARR) to show aligned growth levels and trend shape.",
        "takeaway": f"MRR grew from ${start_mrr/1e6:.2f}M to ${end_mrr/1e6:.2f}M (+{growth:.0%}), indicating strong recurring growth momentum.",
    }


def chart_grr_nrr_trend(panel: pd.DataFrame, out: Path) -> Dict[str, str]:
    active = panel[panel["active_flag"] == 1].copy()
    monthly = active.groupby("month", as_index=False).agg(
        starting_mrr=("active_mrr", "sum"),
        expansion_mrr=("expansion_mrr", "sum"),
        contraction_mrr=("contraction_mrr", "sum"),
    )
    churn_mrr = (
        active[active["churn_flag"] == 1]
        .groupby("month", as_index=False)["active_mrr"]
        .sum()
        .rename(columns={"active_mrr": "churn_mrr"})
    )
    monthly = monthly.merge(churn_mrr, on="month", how="left").fillna({"churn_mrr": 0.0})

    monthly["grr"] = np.where(
        monthly["starting_mrr"] > 0,
        (monthly["starting_mrr"] - monthly["contraction_mrr"] - monthly["churn_mrr"]) / monthly["starting_mrr"],
        np.nan,
    )
    monthly["nrr"] = np.where(
        monthly["starting_mrr"] > 0,
        (monthly["starting_mrr"] + monthly["expansion_mrr"] - monthly["contraction_mrr"] - monthly["churn_mrr"]) / monthly["starting_mrr"],
        np.nan,
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(monthly["month"], monthly["grr"], color=COLORS["danger"], linewidth=2.2, label="GRR")
    ax.plot(monthly["month"], monthly["nrr"], color=COLORS["primary"], linewidth=2.6, label="NRR")
    ax.axhline(1.0, color=COLORS["neutral"], linewidth=1.3, linestyle="--", alpha=0.8)

    ax.set_title("Net retention remains near parity while gross retention stays consistently high")
    ax.set_xlabel("Month")
    ax.set_ylabel("Retention Rate")
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.legend(frameon=False, loc="lower right")

    latest = monthly.iloc[-1]
    ax.annotate(
        f"Latest NRR: {latest['nrr']:.2%}",
        xy=(latest["month"], latest["nrr"]),
        xytext=(-95, 18),
        textcoords="offset points",
        fontsize=10,
        color=COLORS["primary"],
    )

    save_fig(fig, out / "02_grr_nrr_retention_trend.png")
    return {
        "file": "02_grr_nrr_retention_trend.png",
        "objective": "Track retention quality and expansion offset through GRR vs NRR over time.",
        "chart_type": "Two-line retention trend with 100% reference line for leadership benchmark context.",
        "takeaway": f"Latest GRR/NRR are {latest['grr']:.2%}/{latest['nrr']:.2%}, showing stable retention with limited net expansion surplus.",
    }


def chart_churn_by_segment(panel: pd.DataFrame, out: Path) -> Dict[str, str]:
    active = panel[panel["active_flag"] == 1].copy()
    churn = active.groupby("segment", as_index=False).agg(active_rows=("customer_id", "count"), churn_events=("churn_flag", "sum"))
    churn["logo_churn_rate"] = churn["churn_events"] / churn["active_rows"]
    churn = churn.sort_values("logo_churn_rate", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(churn["segment"], churn["logo_churn_rate"], color=[COLORS["danger"], COLORS["accent"], COLORS["secondary"]])
    ax.invert_yaxis()
    ax.set_title("SMB churn materially exceeds Enterprise, highlighting uneven retention quality")
    ax.set_xlabel("Logo Churn Rate")
    ax.xaxis.set_major_formatter(PercentFormatter(1.0))

    for bar, rate in zip(bars, churn["logo_churn_rate"]):
        ax.text(rate + 0.0003, bar.get_y() + bar.get_height() / 2, f"{rate:.2%}", va="center", fontsize=10)

    save_fig(fig, out / "03_logo_churn_by_segment.png")
    return {
        "file": "03_logo_churn_by_segment.png",
        "objective": "Expose which segment contributes disproportionate churn burden.",
        "chart_type": "Sorted horizontal bar chart for direct rank-order comparison.",
        "takeaway": f"Highest segment churn is {churn.iloc[0]['segment']} at {churn.iloc[0]['logo_churn_rate']:.2%}, well above {churn.iloc[-1]['segment']} ({churn.iloc[-1]['logo_churn_rate']:.2%}).",
    }


def chart_revenue_concentration(panel: pd.DataFrame, out: Path) -> Dict[str, str]:
    latest_month = panel["month"].max()
    latest = panel[(panel["month"] == latest_month) & (panel["active_mrr"] > 0)].copy()
    acc = latest.groupby("customer_id", as_index=False)["active_mrr"].sum().sort_values("active_mrr", ascending=False)
    acc["rank"] = np.arange(1, len(acc) + 1)
    acc["cum_share"] = acc["active_mrr"].cumsum() / acc["active_mrr"].sum()
    acc["rank_share"] = acc["rank"] / len(acc)

    top10_share = float(acc.head(10)["active_mrr"].sum() / acc["active_mrr"].sum())
    top50_share = float(acc.head(50)["active_mrr"].sum() / acc["active_mrr"].sum())

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.plot(acc["rank_share"], acc["cum_share"], color=COLORS["primary"], linewidth=2.8, label="Observed concentration")
    ax.plot([0, 1], [0, 1], color=COLORS["neutral"], linestyle="--", linewidth=1.2, label="Equal distribution")

    ax.set_title("Revenue concentration is controlled, with top-account exposure still strategically relevant")
    ax.set_xlabel("Share of Accounts")
    ax.set_ylabel("Cumulative Share of MRR")
    ax.xaxis.set_major_formatter(PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.legend(frameon=False, loc="lower right")

    ax.annotate(
        f"Top 10 = {top10_share:.1%} of MRR\nTop 50 = {top50_share:.1%}",
        xy=(0.22, 0.25),
        xycoords="axes fraction",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#DDDDDD"),
        fontsize=10,
    )

    save_fig(fig, out / "04_revenue_concentration_curve.png")
    return {
        "file": "04_revenue_concentration_curve.png",
        "objective": "Quantify concentration risk and cumulative dependence on top accounts.",
        "chart_type": "Lorenz-style cumulative concentration curve to compare observed vs equal distribution.",
        "takeaway": f"Top 10 accounts represent {top10_share:.1%} of MRR and top 50 represent {top50_share:.1%}; concentration is manageable but material for downside planning.",
    }


def chart_avg_discount_views(panel: pd.DataFrame, manager_summary: pd.DataFrame, out: Path) -> Dict[str, str]:
    latest_month = panel["month"].max()
    latest = panel[(panel["month"] == latest_month) & (panel["active_mrr"] > 0)].copy()

    seg = (
        latest.groupby("segment", as_index=False)
        .apply(lambda g: pd.Series({"avg_discount": weighted_avg(g, "avg_discount_pct", "active_mrr")}), include_groups=False)
        .reset_index(drop=True)
    )
    seg = seg.sort_values("avg_discount", ascending=False)

    channel = (
        latest.groupby("acquisition_channel", as_index=False)
        .apply(lambda g: pd.Series({"avg_discount": weighted_avg(g, "avg_discount_pct", "active_mrr")}), include_groups=False)
        .reset_index(drop=True)
    )
    channel = channel.sort_values("avg_discount", ascending=False)

    mgr = manager_summary.sort_values("avg_discount", ascending=False).head(12)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    axes[0].barh(seg["segment"], seg["avg_discount"], color=COLORS["accent"])
    axes[0].invert_yaxis()
    axes[0].set_title("Segment discount spread")
    axes[0].xaxis.set_major_formatter(PercentFormatter(1.0))

    axes[1].barh(channel["acquisition_channel"], channel["avg_discount"], color=COLORS["danger"])
    axes[1].invert_yaxis()
    axes[1].set_title("Channel discount dependence")
    axes[1].xaxis.set_major_formatter(PercentFormatter(1.0))

    axes[2].barh(mgr["account_manager_id"], mgr["avg_discount"], color=COLORS["purple"])
    axes[2].invert_yaxis()
    axes[2].set_title("Top manager discount outliers")
    axes[2].xaxis.set_major_formatter(PercentFormatter(1.0))

    fig.suptitle("Discount pressure is concentrated in specific channels and manager portfolios", y=1.02)

    save_fig(fig, out / "05_average_discount_segment_channel_manager.png")

    return {
        "file": "05_average_discount_segment_channel_manager.png",
        "objective": "Identify where discount pressure originates across segment, channel, and manager ownership.",
        "chart_type": "Three aligned sorted bar charts to show controllable discount concentration points.",
        "takeaway": f"Highest channel discounting appears in {channel.iloc[0]['acquisition_channel']} ({channel.iloc[0]['avg_discount']:.1%}); manager-level outliers are visible for policy review.",
    }


def chart_discounted_share_trend(panel: pd.DataFrame, out: Path) -> Dict[str, str]:
    active = panel[panel["active_mrr"] > 0].copy()
    monthly = active.groupby("month", as_index=False).agg(
        total_mrr=("active_mrr", "sum"),
        discounted_mrr=("active_mrr", lambda s: s[active.loc[s.index, "discount_dependency_flag"] == 1].sum()),
    )
    monthly["discounted_share"] = np.where(monthly["total_mrr"] > 0, monthly["discounted_mrr"] / monthly["total_mrr"], 0)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(monthly["month"], monthly["discounted_share"], color=COLORS["danger"], linewidth=2.6)
    ax.fill_between(monthly["month"], 0, monthly["discounted_share"], color=COLORS["danger"], alpha=0.12)

    ax.set_title("Discount-dependent revenue share remains meaningful, requiring ongoing discipline")
    ax.set_xlabel("Month")
    ax.set_ylabel("Discounted Revenue Share")
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))

    latest = monthly.iloc[-1]
    ax.annotate(
        f"Latest: {latest['discounted_share']:.1%}",
        xy=(latest["month"], latest["discounted_share"]),
        xytext=(-90, 18),
        textcoords="offset points",
        fontsize=10,
    )

    save_fig(fig, out / "06_discounted_revenue_share_trend.png")
    return {
        "file": "06_discounted_revenue_share_trend.png",
        "objective": "Monitor how much of recurring revenue relies on discount-dependent accounts.",
        "chart_type": "Annotated area-line trend to emphasize magnitude and trajectory.",
        "takeaway": f"Discount-dependent share is {latest['discounted_share']:.1%} in the latest month, remaining a non-trivial quality watchpoint.",
    }


def chart_churn_risk_distribution(scoring: pd.DataFrame, out: Path) -> Dict[str, str]:
    fig, ax = plt.subplots(figsize=(11, 6))
    sns.histplot(scoring["churn_risk_score"], bins=30, kde=True, color=COLORS["accent"], ax=ax)

    for x, label in [(30, "Moderate"), (55, "High"), (75, "Critical")]:
        ax.axvline(x, color=COLORS["neutral"], linestyle="--", linewidth=1)
        ax.text(x + 0.5, ax.get_ylim()[1] * 0.92, label, fontsize=9, color=COLORS["neutral"])

    ax.set_title("Most accounts are low-to-moderate churn risk, with a focused high-risk tail")
    ax.set_xlabel("Churn Risk Score (0-100)")
    ax.set_ylabel("Account Count")

    save_fig(fig, out / "07_churn_risk_score_distribution.png")

    high_share = (scoring["churn_risk_tier"].isin(["High", "Critical"]).mean())
    return {
        "file": "07_churn_risk_score_distribution.png",
        "objective": "Show concentration of churn risk across the account base and tail exposure.",
        "chart_type": "Histogram with risk-tier cut lines for operating-threshold alignment.",
        "takeaway": f"High/Critical churn-risk accounts represent {high_share:.1%} of accounts, supporting a focused intervention model.",
    }


def chart_revenue_quality_distribution(scoring: pd.DataFrame, out: Path) -> Dict[str, str]:
    fig, ax = plt.subplots(figsize=(11, 6))
    sns.histplot(scoring["revenue_quality_score"], bins=30, kde=True, color=COLORS["secondary"], ax=ax)

    for x, label in [(25, "Fragile"), (45, "Watch"), (70, "Healthy threshold")]:
        ax.axvline(x, color=COLORS["neutral"], linestyle="--", linewidth=1)
        ax.text(x + 0.5, ax.get_ylim()[1] * 0.90, label, fontsize=9, color=COLORS["neutral"])

    ax.set_title("Revenue quality skews mid-range, leaving clear room for commercial quality uplift")
    ax.set_xlabel("Revenue Quality Score (0-100)")
    ax.set_ylabel("Account Count")

    save_fig(fig, out / "08_revenue_quality_score_distribution.png")
    weak_share = (scoring["revenue_quality_score"] < 45).mean()
    return {
        "file": "08_revenue_quality_score_distribution.png",
        "objective": "Assess distribution of revenue quality and identify how much portfolio sits below healthy thresholds.",
        "chart_type": "Histogram with practical threshold markers for quality governance.",
        "takeaway": f"{weak_share:.1%} of accounts score below 45, indicating a significant quality-improvement opportunity.",
    }


def chart_expansion_quality_by_segment(panel: pd.DataFrame, out: Path) -> Dict[str, str]:
    active = panel[(panel["active_mrr"] > 0) & (panel["expansion_mrr"] > 0)].copy()
    active["expansion_quality"] = np.where(
        (active["product_usage_score"] >= 65)
        & (active["nps_score"] >= 20)
        & (active["payment_delay_days"] <= 15)
        & (active["avg_discount_pct"] <= 0.20),
        "Healthy",
        np.where(
            (active["avg_discount_pct"] >= 0.25)
            | (active["product_usage_score"] < 55)
            | (active["payment_delay_days"] > 20)
            | (active["nps_score"] < 10),
            "Fragile",
            "Watch",
        ),
    )

    grouped = active.groupby(["segment", "expansion_quality"], as_index=False)["expansion_mrr"].sum()
    pivot = grouped.pivot(index="segment", columns="expansion_quality", values="expansion_mrr").fillna(0)
    for col in ["Healthy", "Watch", "Fragile"]:
        if col not in pivot.columns:
            pivot[col] = 0.0
    pivot = pivot[["Healthy", "Watch", "Fragile"]]
    shares = pivot.div(pivot.sum(axis=1), axis=0).fillna(0)

    fig, ax = plt.subplots(figsize=(11, 6))
    bottom = np.zeros(len(shares))
    color_map = {"Healthy": COLORS["secondary"], "Watch": COLORS["accent"], "Fragile": COLORS["danger"]}
    for col in shares.columns:
        vals = shares[col].values
        ax.bar(shares.index, vals, bottom=bottom, label=col, color=color_map[col])
        bottom += vals

    ax.set_title("SMB expansion mix is more fragile than Mid-Market and Enterprise")
    ax.set_ylabel("Share of Expansion MRR")
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.legend(frameon=False, loc="upper right")

    smb_fragile = shares.loc["SMB", "Fragile"] if "SMB" in shares.index else 0.0
    ax.annotate(f"SMB fragile share: {smb_fragile:.1%}", xy=(0.05, 0.92), xycoords="axes fraction", fontsize=10)

    save_fig(fig, out / "09_expansion_quality_by_segment.png")
    return {
        "file": "09_expansion_quality_by_segment.png",
        "objective": "Compare quality composition of expansion revenue across segments.",
        "chart_type": "100% stacked bar chart to show healthy/watch/fragile mix by segment.",
        "takeaway": f"SMB fragile expansion share is {smb_fragile:.1%}, materially above healthier segment mixes.",
    }


def chart_top_accounts_governance(scoring: pd.DataFrame, out: Path) -> Dict[str, str]:
    top = scoring[scoring["current_mrr"] > 0].nlargest(15, "governance_priority_score").copy()
    top = top.sort_values("governance_priority_score", ascending=True)
    tier_colors = {
        "Low": "#9ecae1",
        "Moderate": "#6baed6",
        "High": "#3182bd",
        "Critical": "#08519c",
    }
    colors = top["governance_priority_tier"].map(tier_colors)

    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.barh(top["customer_id"], top["governance_priority_score"], color=colors)
    ax.set_title("A small set of accounts dominates immediate governance attention")
    ax.set_xlabel("Governance Priority Score")

    for bar, mrr in zip(bars, top["current_mrr"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2, f"MRR ${mrr/1000:.1f}k", va="center", fontsize=9)

    save_fig(fig, out / "10_top_accounts_governance_priority.png")
    return {
        "file": "10_top_accounts_governance_priority.png",
        "objective": "Prioritize account-level interventions by urgency and exposure.",
        "chart_type": "Ranked horizontal bar chart with tier coloring and MRR annotation.",
        "takeaway": f"Top priority account score is {top.iloc[-1]['governance_priority_score']:.1f} with MRR ${top.iloc[-1]['current_mrr']/1000:.1f}k, indicating concentrated intervention leverage.",
    }


def chart_cohort_heatmap(cohort_summary: pd.DataFrame, out: Path) -> Dict[str, str]:
    cohort_summary = cohort_summary.copy()
    agg = (
        cohort_summary.groupby(["cohort_month", "month_number"], as_index=False)
        .apply(
            lambda g: pd.Series(
                {
                    "net_retention_rate": weighted_avg(
                        g.assign(active_customers=np.maximum(g["active_customers"], 1)),
                        "net_retention_rate",
                        "active_customers",
                    ),
                }
            ),
            include_groups=False,
        )
        .reset_index(drop=True)
    )

    # limit to first 18 months for readability
    agg = agg[agg["month_number"] <= 18]
    heat = agg.pivot(index="cohort_month", columns="month_number", values="net_retention_rate").sort_index()

    fig, ax = plt.subplots(figsize=(13, 7))
    sns.heatmap(
        heat,
        cmap="YlGnBu",
        vmin=0.7,
        vmax=1.2,
        cbar_kws={"format": PercentFormatter(1.0), "label": "Net Retention"},
        ax=ax,
    )
    ax.set_title("Cohort retention heatmap highlights uneven durability across acquisition vintages")
    ax.set_xlabel("Months Since Cohort Start")
    ax.set_ylabel("Cohort Month")

    save_fig(fig, out / "11_cohort_retention_heatmap.png")
    return {
        "file": "11_cohort_retention_heatmap.png",
        "objective": "Visualize retention durability by cohort age to detect weak vintages.",
        "chart_type": "Heatmap for dense cohort-age retention pattern scanning.",
        "takeaway": "Cohort performance dispersion is visible by vintage, supporting cohort-specific renewal and CS interventions.",
    }


def chart_discount_vs_churn_risk(scoring: pd.DataFrame, health: pd.DataFrame, out: Path) -> Dict[str, str]:
    df = scoring.merge(health[["customer_id", "trailing_3m_discount_avg"]], on="customer_id", how="left")
    df = df[df["current_mrr"] > 0].copy()

    fig, ax = plt.subplots(figsize=(11, 6))
    sns.regplot(
        data=df,
        x="trailing_3m_discount_avg",
        y="churn_risk_score",
        scatter_kws={"alpha": 0.28, "s": 24, "color": COLORS["accent"]},
        line_kws={"color": COLORS["danger"], "linewidth": 2.2},
        ax=ax,
    )

    ax.set_title("Higher sustained discounting is associated with higher churn risk")
    ax.set_xlabel("Trailing 3M Average Discount")
    ax.set_ylabel("Churn Risk Score")
    ax.xaxis.set_major_formatter(PercentFormatter(1.0))

    corr = df[["trailing_3m_discount_avg", "churn_risk_score"]].corr().iloc[0, 1]
    ax.annotate(f"Correlation: {corr:.2f}", xy=(0.02, 0.92), xycoords="axes fraction", fontsize=10)

    save_fig(fig, out / "12_discount_vs_churn_risk.png")
    return {
        "file": "12_discount_vs_churn_risk.png",
        "objective": "Assess relationship between pricing concessions and forward churn risk level.",
        "chart_type": "Scatter + regression trendline for interpretable directional association.",
        "takeaway": f"Discount and churn-risk correlation is {corr:.2f}, indicating discount-heavy accounts warrant risk monitoring.",
    }


def chart_payment_delay_vs_churn_risk(scoring: pd.DataFrame, health: pd.DataFrame, out: Path) -> Dict[str, str]:
    df = scoring.merge(health[["customer_id", "trailing_3m_payment_delay_avg"]], on="customer_id", how="left")
    df = df[df["current_mrr"] > 0].copy()

    fig, ax = plt.subplots(figsize=(11, 6))
    sns.regplot(
        data=df,
        x="trailing_3m_payment_delay_avg",
        y="churn_risk_score",
        scatter_kws={"alpha": 0.28, "s": 24, "color": COLORS["purple"]},
        line_kws={"color": COLORS["danger"], "linewidth": 2.2},
        ax=ax,
    )

    ax.set_title("Payment delays align strongly with elevated churn risk")
    ax.set_xlabel("Trailing 3M Average Payment Delay (days)")
    ax.set_ylabel("Churn Risk Score")

    corr = df[["trailing_3m_payment_delay_avg", "churn_risk_score"]].corr().iloc[0, 1]
    ax.annotate(f"Correlation: {corr:.2f}", xy=(0.02, 0.92), xycoords="axes fraction", fontsize=10)

    save_fig(fig, out / "13_payment_delay_vs_churn_risk.png")
    return {
        "file": "13_payment_delay_vs_churn_risk.png",
        "objective": "Show how collections friction connects with account risk trajectory.",
        "chart_type": "Scatter + regression trendline to quantify directional risk relationship.",
        "takeaway": f"Payment delay and churn-risk correlation is {corr:.2f}, making collections behavior a leading warning signal.",
    }


def chart_usage_decline_vs_churn_risk(scoring: pd.DataFrame, health: pd.DataFrame, out: Path) -> Dict[str, str]:
    df = scoring.merge(health[["customer_id", "trailing_3m_usage_trend"]], on="customer_id", how="left")
    df = df[df["current_mrr"] > 0].copy()

    fig, ax = plt.subplots(figsize=(11, 6))
    sns.regplot(
        data=df,
        x="trailing_3m_usage_trend",
        y="churn_risk_score",
        scatter_kws={"alpha": 0.28, "s": 24, "color": COLORS["teal"]},
        line_kws={"color": COLORS["danger"], "linewidth": 2.2},
        ax=ax,
    )

    ax.axvline(0, color=COLORS["neutral"], linestyle="--", linewidth=1)
    ax.set_title("Accounts with declining usage trend carry disproportionate churn risk")
    ax.set_xlabel("Trailing 3M Usage Trend (slope)")
    ax.set_ylabel("Churn Risk Score")

    corr = df[["trailing_3m_usage_trend", "churn_risk_score"]].corr().iloc[0, 1]
    ax.annotate(f"Correlation: {corr:.2f}", xy=(0.02, 0.92), xycoords="axes fraction", fontsize=10)

    save_fig(fig, out / "14_usage_decline_vs_churn_risk.png")
    return {
        "file": "14_usage_decline_vs_churn_risk.png",
        "objective": "Assess whether usage momentum deterioration aligns with churn risk escalation.",
        "chart_type": "Scatter + regression line with zero-trend marker for clear directional interpretation.",
        "takeaway": f"Usage-trend correlation to churn risk is {corr:.2f}; declining-product-engagement accounts should be prioritized.",
    }


def chart_scenario_comparison(scenarios: pd.DataFrame, out: Path) -> Dict[str, str]:
    order = [
        "base_case",
        "risk_adjusted_case",
        "downside_case",
        "discount_discipline_improvement_case",
        "improvement_case",
    ]
    scenarios = scenarios.copy()
    scenarios["scenario"] = pd.Categorical(scenarios["scenario"], categories=order, ordered=True)

    color_map = {
        "base_case": COLORS["primary"],
        "risk_adjusted_case": COLORS["neutral"],
        "downside_case": COLORS["danger"],
        "discount_discipline_improvement_case": COLORS["purple"],
        "improvement_case": COLORS["secondary"],
    }

    fig, ax = plt.subplots(figsize=(12, 6))
    for scenario, g in scenarios.sort_values("forecast_month").groupby("scenario", observed=False):
        ax.plot(
            g["forecast_month"],
            g["forecast_mrr"],
            linewidth=2.5,
            label=scenario.replace("_", " "),
            color=color_map.get(str(scenario), COLORS["primary"]),
        )
        last = g.iloc[-1]
        ax.text(last["forecast_month"], last["forecast_mrr"], f"  {last['forecast_mrr']/1e6:.2f}M", fontsize=9)

    ax.set_title("Downside risk can erase most near-term growth, while healthy execution materially improves trajectory")
    ax.set_xlabel("Forecast Month")
    ax.set_ylabel("Forecast MRR")
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_millions))
    ax.legend(frameon=False, loc="upper left")

    save_fig(fig, out / "15_scenario_mrr_comparison.png")

    base_end = scenarios[scenarios["scenario"] == "base_case"].sort_values("forecast_month").iloc[-1]["forecast_mrr"]
    downside_end = scenarios[scenarios["scenario"] == "downside_case"].sort_values("forecast_month").iloc[-1]["forecast_mrr"]
    diff = downside_end - base_end

    return {
        "file": "15_scenario_mrr_comparison.png",
        "objective": "Compare expected MRR trajectories across base, risk, downside, and improvement scenarios.",
        "chart_type": "Multi-scenario line chart for direct leadership comparison of growth paths.",
        "takeaway": f"Downside path ends about ${diff/1e6:.2f}M below base-case MRR at horizon, quantifying fragility exposure.",
    }


def write_chart_brief(entries: List[Dict[str, str]], output_path: Path) -> None:
    lines = [
        "# Leadership Chart Brief",
        "",
        "| Chart | Objective | Chart Type (Why) | Business Takeaway |",
        "|---|---|---|---|",
    ]
    for e in entries:
        lines.append(f"| {e['file']} | {e['objective']} | {e['chart_type']} | {e['takeaway']} |")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build executive-grade visualization layer.")
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--charts-dir", type=str, default="outputs/charts")
    parser.add_argument("--brief-path", type=str, default="reports/visualization_chart_brief.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    charts_dir = (base_dir / args.charts_dir).resolve()

    setup_style()
    data = load_data(base_dir)
    panel = build_core_panel(data)

    entries: List[Dict[str, str]] = []
    entries.append(chart_mrr_arr_trend(panel, charts_dir))
    entries.append(chart_grr_nrr_trend(panel, charts_dir))
    entries.append(chart_churn_by_segment(panel, charts_dir))
    entries.append(chart_revenue_concentration(panel, charts_dir))
    entries.append(chart_avg_discount_views(panel, data["manager_summary"], charts_dir))
    entries.append(chart_discounted_share_trend(panel, charts_dir))
    entries.append(chart_churn_risk_distribution(data["scoring"], charts_dir))
    entries.append(chart_revenue_quality_distribution(data["scoring"], charts_dir))
    entries.append(chart_expansion_quality_by_segment(panel, charts_dir))
    entries.append(chart_top_accounts_governance(data["scoring"], charts_dir))
    entries.append(chart_cohort_heatmap(data["cohort_summary"], charts_dir))
    entries.append(chart_discount_vs_churn_risk(data["scoring"], data["health"], charts_dir))
    entries.append(chart_payment_delay_vs_churn_risk(data["scoring"], data["health"], charts_dir))
    entries.append(chart_usage_decline_vs_churn_risk(data["scoring"], data["health"], charts_dir))
    entries.append(chart_scenario_comparison(data["scenario_trajectories"], charts_dir))

    write_chart_brief(entries, (base_dir / args.brief_path).resolve())

    print("Visualization layer build complete.")
    print(f"charts_created: {len(entries)}")
    print(f"charts_dir: {charts_dir}")


if __name__ == "__main__":
    main()

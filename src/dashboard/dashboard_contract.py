from __future__ import annotations

CANONICAL_DASHBOARD_PATH = "outputs/dashboard/revenue-quality-command-center.html"

REDIRECT_ENTRYPOINTS = (
    "index.html",
    "docs/index.html",
)

DASHBOARD_CANDIDATE_PATTERNS = (
    "*.html",
    "*dashboard*",
    "*Dashboard*",
)

DASHBOARD_SECTIONS = (
    {
        "section_id": "executive_overview",
        "label": "Executive Overview",
        "purpose": "Portfolio-level KPIs, alerts, narrative, and source traceability.",
    },
    {
        "section_id": "revenue_quality",
        "label": "Revenue Quality",
        "purpose": "MRR/ARR growth, discount exposure, concentration, and expansion quality.",
    },
    {
        "section_id": "retention_churn",
        "label": "Retention & Churn",
        "purpose": "Logo churn, GRR/NRR, cohort durability, and segment-level retention signals.",
    },
    {
        "section_id": "account_risk",
        "label": "Account Risk",
        "purpose": "Risk tiers, score distributions, account diagnostics, and intervention queue.",
    },
    {
        "section_id": "portfolio_manager",
        "label": "Portfolio Manager",
        "purpose": "Manager-level governance lens and portfolio mix comparison.",
    },
    {
        "section_id": "scenario_forecast",
        "label": "Scenario Forecast",
        "purpose": "Base, downside, improvement, discount discipline, and risk-adjusted MRR trajectories.",
    },
)

OFFICIAL_KPI_SPECS = (
    {
        "key": "current_mrr",
        "label": "Current MRR",
        "unit": "currency",
        "source": "reports/main_business_analysis_metrics.json:section1.mrr_end",
        "definition": "Latest-month active MRR across the full portfolio.",
    },
    {
        "key": "arr",
        "label": "ARR",
        "unit": "currency",
        "source": "reports/main_business_analysis_metrics.json:section1.arr_end",
        "definition": "Annualized run-rate computed as Current MRR multiplied by 12.",
    },
    {
        "key": "net_retention",
        "label": "Net Retention",
        "unit": "percentage",
        "source": "reports/main_business_analysis_metrics.json:section2.latest_nrr",
        "definition": "Latest-month retention including expansion, contraction, and churn impact.",
    },
    {
        "key": "gross_retention",
        "label": "Gross Retention",
        "unit": "percentage",
        "source": "reports/main_business_analysis_metrics.json:section2.latest_grr",
        "definition": "Latest-month retention before expansion impact.",
    },
    {
        "key": "logo_churn",
        "label": "Logo Churn",
        "unit": "percentage",
        "source": "reports/main_business_analysis_metrics.json:section2.logo_churn_rate",
        "definition": "Churn events divided by active account rows.",
    },
    {
        "key": "avg_discount",
        "label": "Average Discount",
        "unit": "percentage",
        "source": "reports/main_business_analysis_metrics.json:section1.w_discount_end",
        "definition": "Latest weighted average discount across active MRR.",
    },
    {
        "key": "discounted_revenue_share",
        "label": "Discounted Revenue Share",
        "unit": "percentage",
        "source": "reports/main_business_analysis_metrics.json:section1.share_discounted_mrr_latest",
        "definition": "Share of latest MRR flagged as discount-dependent.",
    },
    {
        "key": "revenue_at_risk_mrr",
        "label": "Revenue At Risk",
        "unit": "currency",
        "source": "reports/main_business_analysis_metrics.json:section5.at_risk_mrr_total",
        "definition": "Current MRR from high/critical governance-priority accounts.",
    },
    {
        "key": "critical_risk_account_count",
        "label": "Critical Accounts",
        "unit": "count",
        "source": "data/processed/account_scoring_model_output.csv:governance_priority_tier",
        "definition": "Count of accounts with Critical governance-priority tier.",
    },
)


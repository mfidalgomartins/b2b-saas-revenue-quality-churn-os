# Outputs

Publication-ready artifacts for the Revenue Quality Operating System. Three
folders, each self-contained.

```
outputs/
├── graphs/      16 decision-relevant chart PNGs (one analytical question each)
├── dashboard/   self-contained interactive HTML command center
└── reports/     the analytical PDF report (31 pages, charts inline)
```

## reports/
`revenue_quality_os_analytical_report.pdf` is the primary deliverable: a
31-page analytical report with charts and tables inline, covering
revenue quality, retention, churn concentration, discounting, expansion quality,
account-level risk, the churn-risk scoring system and a six-month scenario
forecast. Regenerate with `python scripts/build_pdf_report.py`.

## graphs/
A consistent chart pack drawn directly from `data/processed`. Trend,
composition, ranking, distribution, variance, correlation, cohort,
concentration and before-vs-after views. Regenerate with
`python src/visualization/build_executive_graphs.py` and
`python src/visualization/build_supplementary_graphs.py`.

## dashboard/
`revenue-quality-command-center.html` opens standalone in any browser, with a
light and dark theme. `README.md` documents how to access it.

**Live dashboard (GitHub Pages):**
https://mfidalgomartins.github.io/b2b-saas-revenue-quality-churn-os/

All figures and statistics are generated from the processed data layer. The
dataset is synthetic and findings are associative. See Section 6 of the report.

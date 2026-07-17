# Outputs

Publication-ready artifacts for the Revenue Quality Operating System. Three
folders, each self-contained.

```
outputs/
├── graphs/      16 decision-relevant chart PNGs (one analytical question each)
├── dashboard/   self-contained interactive HTML command center
└── reports/     the consulting-grade PDF report (charts inline)
```

## reports/
`revenue_quality_os_consulting_report.pdf` is the primary deliverable: a
multi-page report with charts and tables inline, covering revenue quality,
retention, churn concentration, discounting, expansion quality, account-level
risk, the churn-risk scoring system and a six-month scenario forecast. The
root-level `reports/` directory adds machine-readable validation,
intervention-effectiveness and probabilistic-forecast evidence. Regenerate it
with `python scripts/build_pdf_report.py && python scripts/build_consulting_pdf_report.py` —
the first script writes the analytical base PDF that the second reads from
disk, verifies is byte-unchanged, and re-typesets into the published report.
The intermediate analytical PDF is a build input, not a tracked deliverable.

Typeset in Source Serif 4 (body) and Inter (headings, labels, table text) —
bundled as static font instances under `assets/fonts/` so the PDF renders
identically on any machine rather than falling back to a generic system font.

## graphs/
A consistent chart pack drawn directly from `data/processed`. Trend,
composition, ranking, distribution, variance, correlation, cohort,
concentration and before-vs-after views. Regenerate with
`python -m src.visualization.build_executive_graphs` and
`python -m src.visualization.build_supplementary_graphs`.

Chart text is set in the same bundled Inter as the PDF report, so the chart
pack and the report chrome share one typographic system.

## dashboard/
`revenue-quality-command-center.html` opens standalone in any browser, with a
light and dark theme. `README.md` documents how to access it.

**Live dashboard (GitHub Pages):**
https://mfidalgomartins.github.io/b2b-saas-revenue-quality-churn-os/

All figures and statistics are generated from the processed data layer. The
dataset is synthetic and findings are associative. See Section 6 of the report.

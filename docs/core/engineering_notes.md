# Engineering notes: report and chart-pack production

Working notes on a few non-obvious problems encountered building the analytical
PDF report (`scripts/build_pdf_report.py`) and chart pack
(`src/visualization/*.py`). Kept because the root causes aren't discoverable by
reading the final code — they were only visible during investigation.

## ReportLab has no OpenType shaping engine

Modern text fonts ship "tabular figures" (fixed-width digits, so numbers align
in a table column) as an opt-in OpenType `tnum` GSUB substitution. Declaring
that feature the way a browser or InDesign would does nothing in ReportLab,
because ReportLab's text layer only reads a font's default cmap-to-glyph
mapping — it does not run OpenType shaping at all.

Fix: read each bundled font's `tnum` substitution table directly with
`fontTools` and rewrite the digit glyphs into the font's *default* cmap, so
tabular figures become the only figures ReportLab ever sees. Verified by
measuring `pdfmetrics.stringWidth()` for every digit post-fix — all ten now
render at an identical advance width.

## Justification quality has to be measured, not eyeballed

Switching the report body from Times-Roman to a bundled serif was assumed to
improve justified-text word-spacing ("rivers"). It didn't — measurement proved
the opposite. The wrong first metric (variance of inter-word gaps *within* a
line) always returns ~0, because ReportLab distributes justification stretch
uniformly across a single line; it hides the defect instead of exposing it.

The metric that actually captures rivers: **stretch ratio** — a line's mean
inter-word gap divided by the font's natural (unstretched) space width at that
point size. Extracting glyph positions from the rendered PDF via PyMuPDF and
computing this per line across every page showed the new font's justified text
had *worse* rivers than the old baseline on most pages (e.g. one page measured
1.70x vs. 1.29x).

ReportLab's `Paragraph` engine also ignores soft hyphens (U+00AD) entirely —
confirmed with a narrow-frame render test where hyphenated and non-hyphenated
versions of the same text wrapped identically — so hyphenation-assisted
justification wasn't an available fix. The report now sets body text
ragged-right instead, which is itself standard professional practice when
hyphenation isn't available. Re-measured post-fix: stretch ratio ~1.0x and
max ≤1.29x across every content page.

## Paragraph-level widows/orphans are a measurement problem, not a proofreading one

"No stray line stranded at the top or bottom of a page" isn't reliably
catchable by reading a 32-page document once. It's checked programmatically:
extract every body-text line's font, bounding box, and page position via
PyMuPDF, then for each page check whether the top line is short and isolated
(a widow) or the bottom line is a lone paragraph-opener with no continuation
above it (an orphan). Borderline cases are verified individually against the
actual paragraph structure and the next page's continuation before being
accepted as clean, since a line that merely reaches near a page's edge is not
automatically a defect.

## Backtest parity as a pinned invariant, not a one-time check

`src/scoring/backtest_scoring_calibration.py` reconstructs the churn-risk
score at every historical month using the *same* weight dict and component
functions as production scoring (`src/scoring/scoring_utils.py`), rather than
a re-derived approximation. `tests/test_scoring_utils.py` pins the weights and
component families, so any future change to the production score is forced to
either update the backtest in lockstep or fail CI — the calibration result
(forward churn increasing monotonically by risk tier) can't silently drift
out of sync with what's actually deployed.

# Design audit & rationale — Revenue Quality Command Center

Written before and during the 2026-06 redesign pass. This is the design brief the
dashboard is held against; every change in `src/dashboard/_template.html` traces
back to a line here.

## 1. Design brief

**Purpose (one sentence).** Tell a revenue leader whether ARR growth is
high-quality — durable, low-concentration, not discount-financed — and where to
intervene this week.

**Target audience.** CFO, CRO, or senior revenue operator reviewing in under
two minutes; secondary audience is the analyst defending the numbers.

**Main decision supported.** Whether to trust the headline growth number, and
where to allocate retention/governance effort right now.

**Questions it must answer (5).**
1. How big is the book and which way is it moving?
2. Is growth durable, or discount-financed?
3. Where is the downside — risk tiers and revenue concentration?
4. Which accounts do we act on this week?
5. Where does ARR land under base / downside / upside scenarios?

These five questions are the five numbered sections of the dashboard, in that
order — the layout *is* the argument (overview → trend → risk → action →
forward view).

**KPIs above the fold (6).** MRR · ARR · Net retention · Gross retention ·
Discount-reliant MRR share · At-risk MRR. Each carries one qualifying delta
(QoQ, CAGR, pp-vs-benchmark, or Critical count) and nothing else.

*Why these six and not others.* The strip answers exactly two of the five
questions — size/direction (MRR, ARR) and quality of that growth (NRR, GRR,
discount share, at-risk MRR) — because those are the two an executive needs
before deciding whether to keep reading. Deliberately excluded: logo churn
(revenue-weighted churn is already inside GRR; logo counts mislead on a book
where 94% of accounts are SMB), ARPA (a ratio of two numbers already shown),
CAC/LTV (no acquisition-cost data in this system — showing it would fake
coverage), and account count (operational, not decisional — it lives in the
topbar as context, not as a KPI). Six, not seven: every additional tile
dilutes the red "-0.2pp vs 100%" signal that the strip exists to deliver.

## 2. End-to-end audit findings (state before this pass)

| Dimension | Finding | Action taken |
|---|---|---|
| Information hierarchy | Sound: brief → KPIs → sections as questions | Kept |
| Chart readability | **Axis text distorted up to 65% on mobile** — fixed-width `viewBox` + `preserveAspectRatio="none"` stretched glyphs | Replaced with a responsive redraw engine: every chart renders at its real pixel width, text always 1:1; debounced re-render on resize |
| Chart honesty | "Composition of monthly change · MRR added/lost" showed only *net* deltas, not gross flows — a CFO catches that | Retitled "Net MRR change by month", legend "Net increase / Net decline" |
| Chart encoding | Donut ring with a 4% arc read as broken and duplicated the stat beside it | Replaced with three headline shares + quiet bars, mirroring the risk-ladder rhythm |
| Series ambiguity | GRR line and the 100% reference were both faint-dashed — indistinguishable; legend contradicted the chart | GRR is now solid muted; only the reference is dashed |
| Number/date formatting | Raw ISO dates (`2023-03`) in topbar, axes, footer; last axis label clipped at the right edge | Human formats everywhere ("Mar ’23", "Mar 2023 – Feb 2026"); edge labels anchored inward |
| Narrative consistency | Hero said "one Critical account" while the churn-tier ladder showed no Critical tier (different taxonomies) | Hero now says "1 account at Critical action priority", matching section 04's tier language |
| False precision | Scenario card showed "+$255" against a $10.65M base (0.002%) | Deltas under 0.1% of base render as "≈ base" |
| Redundancy | Scenario chart had both direct end-labels and a legend | End-labels only on wide screens; HTML legend only when narrow |
| Filters | Tier chips, search, column sort, row drawer, "show more" — all purposeful; none removed | Verified all functional via Playwright; churn-risk column hidden on mobile (redundant with Priority; detail lives in the drawer) — this also fixed a 38px mobile overflow |
| Color/contrast | `--faint` carried informative panel notes at 2.52:1 in light | Darkened to 3.20:1 (light) / 3.49:1 (dark); every other informative pair audited ≥ 4.5:1 in both themes |
| Accessibility | Sortable headers styled for focus but unreachable by keyboard; clickable rows ignored Enter | `tabindex`, Enter/Space handlers, `aria-sort` on the active column |
| Theme system | Light/dark both first-class: resolved pre-paint, persisted, OS-following until user override; charts repaint via CSS variables alone | Kept; re-audited both palettes |

## 3. Standards alignment

**Typography — Fraunces + Inter Tight, JetBrains Mono for data.** The brief's
preferred pairings are Geist + Geist Mono, or DM Sans + Instrument Serif "for a
warmer editorial tone". This dashboard is the editorial archetype: a serif
display face for headline numbers and section titles over a quiet sans body.
Fraunces is chosen over Instrument Serif deliberately — Instrument Serif ships a
single 400 weight and thin hairlines that fall apart at KPI sizes in dark mode;
Fraunces' optical sizing and 500 weight hold a $114.28M figure at 40px in both
themes. The mono face plays exactly the role Geist Mono plays in the first
pairing: tabular numerals for axes, table figures, and metadata. Two reading
voices (serif display, sans text) plus a data voice — not three competing
typefaces.

**Color.** Warm off-white surface (#fafaf7) with near-black ink, one deep
forest accent used only for emphasis (hero figure, positive deltas, primary
series), a clay negative, muted grays for secondary data. Dark mode is a
warm-neutral charcoal (#0c0e0d) with a brighter sage accent so emphasis still
reads. This sits in the Stripe/Vercel family — neutral surface, single accent,
no rainbow — with a warmer cast that matches the editorial serif. Risk tiers
use four muted steps of one temperature ramp, not categorical brights.

**No template tricks.** No gradients, icon circles, colored card borders,
glassmorphism, or drop-shadow cards. Structure is carried by hairline rules and
whitespace; the only rounded chrome is functional (pills, chips, theme toggle).

**Spacing & hierarchy system.** One shell (max 1280px), one type scale:
48px hero → 40px KPI figures → 24px section titles → 22px panel figures →
13–14px body → 10–11.5px metadata, with weight never exceeding 600 — size and
color carry the hierarchy, not boldness. Vertical rhythm: 80px between
sections, 48px between a section's KPI strip and its charts, 28px from section
head to content; labels sit 12px above their figures so each KPI reads as one
unit. Numbers are tabular-lining everywhere (`font-variant-numeric:
tabular-nums`), money is compacted to 3–4 significant figures ($9.52M, $376.2K),
percentages carry one decimal only where the decimal is the message (99.8%),
and counts use thousands separators. Whitespace does the sectioning; hairlines
only confirm it.

## 3b. Against the generic AI-generated dashboard

The explicit bar was "clearly better than any generic AI-generated dashboard".
The signature traits of that genre, and where this artifact stands:

| Generic AI dashboard | This dashboard |
|---|---|
| Card grid with drop shadows, icon circles, 4-color KPI tiles | Open typographic layout on one surface; structure from rules and whitespace |
| Rainbow categorical palette, one gradient hero | Ink + one accent + clay negative; risk tiers on a single muted ramp |
| Generic titles ("Overview", "Analytics", "Performance") | Sections are the executive's actual questions ("Is growth durable, or discount-financed?") |
| KPIs without qualification, deltas without benchmarks | Every figure carries exactly one decision-relevant qualifier (vs 100%, vs 95%, CAGR, Critical count) |
| Donut charts regardless of data shape | Donut was removed *because* a 4% arc misleads; encodings chosen per distribution |
| Charts as images or canvas that blur and stretch | Inline SVG redrawn at real pixel width; text never scales |
| Labels copied from column names (`2023-03`, `risk_adjusted`) | Human language everywhere ("Mar ’23", "Risk-adjusted") |
| Decorative interactivity, broken filters | Every control exercised by an automated QA suite; nothing decorative shipped |
| Dark mode as an inverted afterthought | Two designed palettes, pre-paint resolution, charts restyle via CSS variables |
| No stance on data quality | Validation gate in the header, method and caveats in the footer, "≈ base" instead of false precision |

## 4b. Hard critique of the final build — remaining weaknesses, named

Run after the last change (footer count formatting, gate-badge explainer).
What still keeps this from a hypothetical 11/10, and why each is accepted:

1. **The "Primary driver" column is monotone at the top of the queue** — the
   top-ranked accounts all share "Churn risk pressure", so the column adds no
   discrimination until you scroll or filter. It stays because it is the data's
   truth and the column differentiates lower down; hiding it would hide signal.
2. **NRR at 99.8% renders red.** Arguably alarmist for a 0.2pp miss; kept
   deliberately — NRR below 100% is the one number this dashboard exists to
   flag, and softening it optimizes for comfort over decision.
3. **The High-tier ladder bar is nearly invisible (0.8% of MRR).** Honest
   encoding of a healthy book; the count column beside it carries the value.
4. **Three font families load (~90KB)** where the brief prefers two. Accepted:
   the mono is a data voice, not a reading voice (see §3); collapsing numerals
   into Inter Tight would cost tabular alignment in the queue and axes.
5. **No print stylesheet.** The artifact targets screen and screenshot;
   printing produces acceptable but not designed output.
6. **Scenario taxonomy ("Risk-adjusted" vs "Downside") assumes context** — a
   first-time reader may not know one is score-weighted and one is a stress
   path. The cards' deltas communicate the ordering; a glossary would add
   chrome the 2-minute reader doesn't want. Documented here instead.
7. **Tier chips don't show counts** (e.g., "Critical · 1"). Considered and
   rejected: counts change with search input and would imply the chips are
   scoped to the current search, which they are not.

## 4c. Formal scorecard against the stop criteria

Assessed on the final build, against fresh full-page screenshots (desktop
1440px and mobile 390px, light and dark) and the automated QA evidence.

| # | Criterion | Evidence | Verdict |
|---|---|---|---|
| 1 | 5-second readability | The largest element on screen (48px serif headline) *is* the finding: growth rate + discount share. KPI strip is the second-largest type on the page. | Pass |
| 2 | Purpose & narrative instantly clear | Five sections are the executive's five questions, in decision order; eyebrow says "Executive briefing". | Pass |
| 3 | KPI hierarchy obvious | 40px figures over 11px uppercase labels (3.6× ratio); one qualifier per figure; nothing else in the strip. | Pass |
| 4 | Typography premium & intentional | Single type scale, weight ≤600, serif display + sans text + mono data; rationale and trade-off documented (§3, §4b-4). | Pass |
| 5 | Charts sharp, relevant, readable | Axis text measured 1:1 at 1440/1100/390px (was 35% compressed); titles state what is actually encoded; donut removed on data-shape grounds. | Pass |
| 6 | Filters useful, clean, functional | Every control (chips, search, sort, drawer, show-more, toggle) exercised by the QA suite; none decorative; one redundant column demoted on mobile. | Pass |
| 7 | Light & dark both 10/10 | Both palettes designed (warm paper / warm charcoal + sage), not inverted; resolved pre-paint; charts restyle via variables; WCAG-audited pair-by-pair; full-page + crop review in both. | Pass |
| 8 | Cohesive, executive, publication-ready | One surface, one accent, consistent rhythm; self-contained HTML already published via GitHub Pages. | Pass |
| 9 | Zero obvious AI smell | Each of the ten genre traits (§3b) verified absent on the final build — by QA check (ISO dates, broken controls, stretched text) or by screenshot review (shadows, icon circles, rainbow, donut, inverted dark). | Pass |

## 4d. Measured deltas vs the session-start build (the concrete benchmark)

The starting artifact was itself an AI-generated dashboard. "Clearly better"
is claimed against it with measurements, not adjectives:

| Measure | Before | After |
|---|---|---|
| Mobile axis-glyph rendering (clientW/bboxW) | 35% (scenarios chart) | 100%, all charts/widths |
| Horizontal overflow at 390px | 428px page width | none |
| Min. contrast of informative text | 2.52:1 (light) / 3.11:1 (dark) | 3.20:1 / 3.49:1; data pairs ≥4.5:1 |
| Raw ISO dates visible | topbar, 5 charts, footer | zero (regex-checked, both themes, 3 widths) |
| Charts whose label contradicts the encoding | 1 ("MRR added/lost" on net deltas) | zero |
| Series visually identical to a reference line | 1 (GRR vs 100%) | zero |
| Encodings duplicating an adjacent number | 1 (4%-arc donut) | zero |
| False-precision deltas | "+$255" on a $10.65M base | "≈ base" under 0.1% |
| Keyboard-reachable table controls | none | all (sort + rows, `aria-sort`) |
| Axis labels clipped at container edge | yes (right edge) | zero (bounds-checked) |
| Hero vs risk-ladder taxonomy conflict | yes ("Critical" ambiguous) | resolved |
| Test suite | 46 green | 46 green (no regression) |

## 5. Process note

The design brief (§1) and the weaknesses list that drove round 1 were defined
at the start of the redesign session, before any edit; this file persists them
post-hoc alongside the verification results so the rationale travels with the
artifact. Iteration ran in three rounds, each closed by a critique that fed the
next (round 1: structural/honesty; round 2: overflow, contrast, alignment;
round 3: precision, keyboard access; final: formatting consistency and the
gate-badge explainer found by this critique).

## 4. Verification

- Playwright QA: zero failures — no horizontal overflow or SVG text clipping at
  1440/1100/390px in both themes; theme toggle, tier filters, search, sort,
  drawer, "show more", tooltips and resize-redraw all exercised.
- WCAG contrast audit computed for every informative pair in both palettes.
- Full project suite green (46 tests + 21 subtests), including the dashboard
  contract tests (single embedded payload, decision brief, working theme toggle).
- Visual review of full pages and real-scale crops at desktop and mobile, light
  and dark.

Regenerate with `make dashboard`; screenshot both themes at both widths with
`python scripts/shoot_dashboard.py`.

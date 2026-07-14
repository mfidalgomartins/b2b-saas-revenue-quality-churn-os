# Consulting-Grade Revenue Quality Report Redesign

**Status:** Approved direction  
**Date:** 2026-07-14  
**Primary audience:** CEO, CFO, CRO, revenue operations leadership and board-level readers

## Objective

Create a second, final-quality PDF report that retains the analytical content and evidence of the existing Revenue Quality Operating System report while adopting the consulting-grade editorial discipline demonstrated by `28th-ceo-survey.pdf`.

The reference PDF is used only for visual and editorial study: typography contrast, spacing, hierarchy, chart treatment, table treatment, page rhythm, executive formatting and palette logic. Its content, branding, logo, photography and proprietary decorative marks will not be reused.

## Deliverables and preservation constraints

- Preserve `outputs/reports/revenue_quality_os_analytical_report.pdf` byte-for-byte.
- Add `outputs/reports/revenue_quality_os_consulting_report.pdf` alongside it.
- Add a separate deterministic generator for the new report; do not overwrite or repurpose the current generator.
- Keep temporary renders and inspection files under `tmp/pdfs/` and remove them before final handoff.
- Leave no unfinished content, mock data, incomplete sections, stock template language or process notes in the final PDF.

## Source of truth

The existing 33-page analytical report and the repository's processed datasets remain the only content and numerical sources. The redesign preserves the current findings, caveats, recommendations, metric definitions and evidence order. Editorial changes are limited to headings, paragraph breaks, captions, callouts and page breaks needed to improve executive readability without changing meaning.

## Editorial system

### Format and grid

- A4 portrait, matching the existing report's operating context.
- Approximately 33 pages; page count may move modestly where the new hierarchy improves page balance.
- 18-20 mm outer margins with a six-column internal grid.
- One dominant reading column for narrative pages; chart pages may use a four-plus-two column split for the claim and methodological note.
- Generous vertical spacing, deliberate white space and no orphan headings or stranded captions.

### Typography

- `Source Serif 4` for cover display title, major section titles and selected insight headlines.
- `Inter` for body text, chart labels, tables, captions, footers and navigation.
- Display title: 34-42 pt depending on line length.
- Section title: 24-30 pt.
- Insight headline: 17-22 pt.
- Body: 9.5-10.5 pt with 13.5-15 pt leading.
- Captions and notes: 7.5-8.5 pt with high legibility and restrained contrast.
- Use weight and space before adding color; avoid oversized KPI-card typography.

### Color palette

- Charcoal text: `#252323`.
- Warm paper: `#F5F4F1`.
- Primary editorial orange-red: `#F04424`.
- Deep red for adverse outcomes: `#AD2B1F`.
- Soft coral for comparison series: `#F49A80`.
- Warm grey: `#D9D7D2`.
- Cool neutral for secondary data: `#9DA3A6`.
- White remains the dominant report surface.

The palette mirrors the reference's logic of one high-energy accent plus tonal comparisons, without reproducing PwC brand assets.

## Page architecture

### Cover

Use an asymmetric editorial composition: warm-grey upper field, a narrow orange-red side rail, a large serif title and an original data-inspired geometric illustration. The illustration will use simple lines, blocks and plotted paths derived from the report's analytical themes; it will not mimic the reference's photograph or proprietary graphic marks.

### Contents and executive summary

The contents page remains compact and functional. The executive summary opens the report body and leads with the central decision-useful conclusion, followed by four concise evidence points. Headline metrics appear as typographic statements integrated into the reading flow rather than generic dashboard cards.

### Section hierarchy

Each major section starts with a strong editorial title and a one-sentence argument. Subsections use conclusion-led headings. Section numbers remain for navigation but become secondary to the insight. Major transitions receive additional top space or a dedicated section-opening treatment when pagination allows.

### Narrative pages

Use shorter paragraphs, limited bold lead-ins, orange square bullets and occasional left-rule callouts for management implications. Dense methodology remains present but visually subordinate to findings.

### Charts

- Redraw charts in the new palette using the same processed data.
- Prefer direct labels, horizontal bars, restrained gridlines and end-of-series annotations.
- Use red only for risk, downside or the primary comparison; use coral and greys for context.
- Remove decorative chart frames and excessive legends.
- Every chart has a neutral exhibit label, an insight-led headline, a concise basis note and a source line.
- Preserve uncertainty bands, sample sizes, denominators and associative-versus-causal caveats where they affect interpretation.

### Tables

- Use compact serif or medium-weight section labels above tables.
- Use dark charcoal or orange-red headers only when the table needs strong navigation.
- Prefer thin horizontal rules, no vertical rules and warm-grey alternating rows only for long tables.
- Reserve red for adverse movement or priority status; composition and current values remain neutral.
- Keep column widths, wrapping and numeric alignment consistent across the report.

### Footers and navigation

Use a quiet running title on the left and page number on the right, with a fine warm-grey rule. The cover has no conventional footer. No PwC name, logo, survey URL or reference-report text appears in the new deliverable.

## Implementation architecture

### Components

1. A separate report builder owns document metadata, page templates, typography, layout primitives and output path.
2. A chart layer reads the existing processed CSV and JSON files and renders report-specific visuals in memory.
3. A content layer reuses the current report's validated narrative and metrics without importing reference-PDF content.
4. Shared helpers provide paragraphs, callouts, exhibit blocks, tables, page breaks and footers.

Each component has one responsibility and can be adjusted without changing the existing report generator.

### Data flow

`data/processed/` and `reports/*.json|csv` feed the new builder. The builder validates required inputs, calculates only display-safe values already represented in the existing report, generates charts into memory, lays out the full document and writes the new PDF to `outputs/reports/`.

The reference PDF never enters the data or content path.

### Error handling

- Fail early with a clear message when a required source file or font is missing.
- Validate expected columns before chart generation.
- Reject non-finite values used in charts or KPI text.
- Write to a temporary build path first and atomically move the completed PDF to the final filename.
- Never remove or rename the existing report.

## Verification and acceptance criteria

The redesign is complete only when all of the following are proven:

1. The existing report's checksum is identical before and after the build.
2. The new PDF exists under the agreed filename and opens successfully.
3. `pdfinfo` reports A4 pages, valid metadata and the intended page count range.
4. `pdffonts` confirms embedded Inter and Source Serif 4 fonts.
5. `pdftotext` confirms the title, executive summary, all major sections, recommendations and appendix are present.
6. An unfinished-marker scan finds no work-in-progress tokens, dummy copy, temporary labels or tool tokens.
7. Every page is rendered to PNG with Poppler and inspected for clipping, overlap, broken glyphs, weak contrast, blank pages, inconsistent footers and poor chart legibility.
8. Charts and tables are checked against their source datasets and the existing report's stated values.
9. Only the new final PDF appears in `outputs/reports/`; temporary renders are removed.
10. The final result is visually specific to this report and does not resemble a generic presentation or dashboard template.

## Explicit non-goals

- No reuse of PwC content, logo, photography, proprietary marks or branding.
- No condensation into a shorter executive brief.
- No changes to analytical methodology, underlying data or validated conclusions.
- No modification, deletion or replacement of the existing PDF.
- No HTML, slide deck or secondary report format in this scope.

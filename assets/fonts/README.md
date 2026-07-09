# Bundled fonts

Static-weight instances used by the analytical PDF report (`scripts/build_pdf_report.py`) and the
matplotlib chart pack (`src/visualization/`), so both render with the same typographic system instead
of falling back to the PDF base-14 fonts (Times-Roman/Helvetica) or whatever sans-serif happens to be
installed on the machine generating the charts.

| File | Family | Weight | Role |
|---|---|---|---|
| `SourceSerif4-Regular.ttf` | Source Serif 4 | Regular | Report body text |
| `SourceSerif4-Italic.ttf` | Source Serif 4 | Regular Italic | Report notes / pull quotes |
| `SourceSerif4-Bold.ttf` | Source Serif 4 | Bold | Report inline emphasis |
| `Inter-Regular.ttf` | Inter | Regular | Report labels/metadata, chart axes and legends |
| `Inter-SemiBold.ttf` | Inter | SemiBold | Report headings, KPI figures, table headers |

## Provenance

Both families are Google Fonts entries distributed as **variable fonts** (a single file spanning a
weight/optical-size range). Reportlab and older matplotlib font-matching expect static, single-weight
TTFs, so each file here is a static instance carved out of the upstream variable font with
`fontTools.varLib.instancer`, fixing:

- Inter: `opsz=14` (Inter's default optical size), `wght=400` or `600`.
- Source Serif 4: `opsz=12` (matched to the report's ~10.5-12pt body size), `wght=400` or `700`; the
  italic instance is derived from the upstream `Italic` variable font at the same axis values.

Source variable fonts: `google/fonts` GitHub repository, `ofl/inter/Inter[opsz,wght].ttf` and
`ofl/sourceserif4/SourceSerif4[opsz,wght].ttf` / `SourceSerif4-Italic[opsz,wght].ttf`.

## License

Both families are licensed under the **SIL Open Font License, Version 1.1** — free for embedding,
redistribution, and modification (including the static-instancing done here). Full license text is
in `Inter-OFL.txt` and `SourceSerif4-OFL.txt` in this directory.

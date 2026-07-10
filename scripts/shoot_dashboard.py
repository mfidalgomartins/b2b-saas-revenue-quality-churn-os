"""Dev-only: screenshot the dashboard in both themes at desktop + mobile.

Not part of the build, and playwright is deliberately not a declared
dependency — install it ad hoc when needed:
    pip install playwright && playwright install chromium

Run from repo root:
    python scripts/shoot_dashboard.py
Writes PNGs to a fresh temp directory (override with DASH_SHOT_DIR).
"""

import os
import pathlib
import tempfile

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parents[1]
HTML = ROOT / "outputs/dashboard/revenue-quality-command-center.html"
OUT = pathlib.Path(os.environ.get("DASH_SHOT_DIR") or tempfile.mkdtemp(prefix="dash_"))
OUT.mkdir(parents=True, exist_ok=True)

url = HTML.as_uri()
shots = []

with sync_playwright() as p:
    browser = p.chromium.launch()
    for theme in ("light", "dark"):
        for label, w, h, full in (("desktop", 1440, 900, True), ("mobile", 390, 844, True)):
            page = browser.new_page(viewport={"width": w, "height": h}, device_scale_factor=2)
            page.goto(url)
            page.evaluate(
                "t => { document.documentElement.setAttribute('data-theme', t);"
                "try{localStorage.setItem('rq-theme', t);}catch(e){} }",
                theme,
            )
            page.wait_for_timeout(500)
            out = OUT / f"{theme}_{label}.png"
            page.screenshot(path=str(out), full_page=full)
            shots.append(str(out))
            page.close()
    browser.close()

print("\n".join(shots))

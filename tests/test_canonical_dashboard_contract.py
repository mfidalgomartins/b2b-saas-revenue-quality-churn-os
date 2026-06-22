from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.dashboard.dashboard_contract import (  # noqa: E402
    CANONICAL_DASHBOARD_PATH,
    OFFICIAL_KPI_SPECS,
    REDIRECT_ENTRYPOINTS,
)


def _embedded_payload(html_path: Path) -> dict | None:
    html = html_path.read_text(encoding="utf-8")
    match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, flags=re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        # File contains the dashboard-data script tag but with the source-side
        # __PAYLOAD_JSON__ sentinel rather than rendered JSON.
        return None


class TestCanonicalDashboardContract(unittest.TestCase):
    def test_only_one_html_dashboard_embeds_payload(self) -> None:
        html_files = [path for path in ROOT.rglob("*.html") if ".git" not in path.parts and ".venv" not in path.parts]
        dashboard_payload_files = [path for path in html_files if _embedded_payload(path) is not None]

        expected = ROOT / CANONICAL_DASHBOARD_PATH
        self.assertEqual(dashboard_payload_files, [expected])

    def test_redirect_entrypoints_point_to_canonical_dashboard(self) -> None:
        canonical = CANONICAL_DASHBOARD_PATH.split("/", 1)[1]

        for rel_path in REDIRECT_ENTRYPOINTS:
            html = (ROOT / rel_path).read_text(encoding="utf-8")
            self.assertIn(canonical, html)
            self.assertNotIn('id="dashboard-data"', html)

    def test_dashboard_payload_includes_kpi_contract(self) -> None:
        payload = _embedded_payload(ROOT / CANONICAL_DASHBOARD_PATH)
        self.assertIsNotNone(payload)

        contract = payload.get("dashboard_contract", {})  # type: ignore[union-attr]
        self.assertEqual(contract.get("canonical_dashboard_path"), CANONICAL_DASHBOARD_PATH)

        expected_keys = {spec["key"] for spec in OFFICIAL_KPI_SPECS}
        payload_kpis = set(payload.get("official_kpis", {}).keys())  # type: ignore[union-attr]
        self.assertTrue(expected_keys.issubset(payload_kpis), f"Missing KPI keys: {expected_keys - payload_kpis}")

    def test_chart_catalog_carries_decision_metadata(self) -> None:
        """The chart catalog stays in the payload as decision metadata.

        Each entry must describe the question it answers and how a decision-maker
        should use it. The dashboard itself renders inline SVG from the underlying
        data, so the catalog no longer carries base64 image bytes — that bloat
        was 12MB+ of dead weight in the previous design.
        """
        payload = _embedded_payload(ROOT / CANONICAL_DASHBOARD_PATH)
        self.assertIsNotNone(payload)

        chart_catalog = payload.get("chart_catalog", [])  # type: ignore[union-attr]
        self.assertGreaterEqual(len(chart_catalog), 15)
        for chart in chart_catalog:
            self.assertIn("image_path", chart)
            self.assertNotEqual(chart.get("question", ""), "")
            self.assertNotEqual(chart.get("decision_use", ""), "")

    def test_dashboard_is_self_contained_inline_svg(self) -> None:
        """No external image hosts; charts are drawn inline as SVG from JSON."""
        html = (ROOT / CANONICAL_DASHBOARD_PATH).read_text(encoding="utf-8")
        # The page must not depend on remote image assets.
        self.assertNotIn("<img ", html)
        # And it must actually render charts inline.
        self.assertIn("<svg", html)
        # Embedded JSON payload must be present and parseable.
        self.assertIn('<script id="dashboard-data"', html)

    def test_dashboard_has_decision_brief_and_theme_toggle(self) -> None:
        """The canonical dashboard ships the decision brief, the priority queue,
        and a fully functional light/dark theme toggle.

        Both palettes must be defined, the toggle must be present, and the user's
        choice must persist across visits. The theme is also resolved before first
        paint so there is never a flash of the wrong palette.
        """
        html = (ROOT / CANONICAL_DASHBOARD_PATH).read_text(encoding="utf-8")

        self.assertIn('class="decision-brief"', html)
        self.assertIn('id="decisionBriefHeadline"', html)
        self.assertIn('id="priorityQueue"', html)

        # Theme control + both palettes.
        self.assertIn('id="themeToggle"', html)
        self.assertIn("data-theme", html)
        self.assertIn('[data-theme="dark"]', html)
        # Preference is persisted and the OS default is honoured.
        self.assertIn("rq-theme", html)
        self.assertIn("prefers-color-scheme", html)

    def test_dashboard_ships_a_designed_print_stylesheet(self) -> None:
        """Saving-to-PDF / printing must produce designed output, not a screen capture.

        The print layer forces the light palette onto paper, paginates without
        splitting decision units, drops interactive-only chrome, and renders the
        accent/pills via print-color-adjust.
        """
        html = (ROOT / CANONICAL_DASHBOARD_PATH).read_text(encoding="utf-8")

        self.assertIn("@media print", html)
        self.assertIn("@page", html)
        self.assertIn("print-color-adjust: exact", html)
        self.assertIn("break-inside: avoid", html)


if __name__ == "__main__":
    unittest.main()

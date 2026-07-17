"""Shared artifacts passed between analytical and release validation groups."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ValidationArtifacts:
    dashboard: dict[str, Any]
    dashboard_payload: dict[str, Any]
    analysis_payload: dict[str, Any]


def validate_dashboard_payload(base_dir: Path) -> dict[str, Any]:
    html_path = base_dir / "outputs" / "dashboard" / "revenue-quality-command-center.html"
    html_files = [path for path in base_dir.rglob("*.html") if ".git" not in path.parts and ".venv" not in path.parts]
    payload_files = []
    for path in html_files:
        html_text = path.read_text(encoding="utf-8")
        match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html_text, flags=re.S)
        if not match:
            continue
        try:
            json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        payload_files.append(str(path.relative_to(base_dir)))

    if not html_path.exists():
        return {"exists": False, "payload_files": payload_files}
    html = html_path.read_text(encoding="utf-8")
    match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, flags=re.S)
    if not match:
        return {"exists": True, "embedded_json_found": False, "payload_files": payload_files}
    payload = json.loads(match.group(1))
    return {
        "exists": True,
        "embedded_json_found": True,
        "payload": payload,
        "html_files": [str(path.relative_to(base_dir)) for path in html_files],
        "payload_files": payload_files,
    }

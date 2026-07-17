"""Shared validation findings and append-only result helper."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Finding:
    check_id: str
    check_name: str
    component: str
    status: str
    severity: str
    details: str
    recommended_fix: str
    fix_applied: str


def add_finding(
    findings: list[Finding],
    check_id: str,
    check_name: str,
    component: str,
    status: str,
    severity: str,
    details: str,
    recommended_fix: str = "None",
    fix_applied: str = "No",
) -> None:
    findings.append(
        Finding(
            check_id=check_id,
            check_name=check_name,
            component=component,
            status=status,
            severity=severity,
            details=details,
            recommended_fix=recommended_fix,
            fix_applied=fix_applied,
        )
    )

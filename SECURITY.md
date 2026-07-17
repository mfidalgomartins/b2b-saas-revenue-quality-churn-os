# Security

This repository is a self-contained analytics project that runs entirely on **synthetic, seeded data**.
It has no network services, no authentication, no secrets, and no user-supplied input at runtime. The
security surface is therefore small — but it is scanned, not assumed.

## Enforced in CI

Every push and pull request runs the controls below; the same workflow also runs weekly so newly disclosed
dependency issues surface even when the repository is idle (see
[`.github/workflows/qa.yml`](.github/workflows/qa.yml)):

- **Static analysis — Bandit** (`make security`): scans `src/` and `scripts/` for common insecure patterns.
- **Dependency audit — pip-audit** (`make audit`): checks declared dependencies against the PyPI advisory
  database for known CVEs.

Both must pass with no findings for the build to go green.

## Documented, reviewed exceptions

Bandit findings that are accepted rather than fixed are configured centrally in
[`pyproject.toml`](pyproject.toml) `[tool.bandit]`, each with a rationale:

| Check | Where | Why it is accepted |
|---|---|---|
| `B404`, `B603` | `src/pipeline/*`, `src/validation/*` | The pipeline orchestrator runs **fixed, in-repo Python module commands** as subprocesses for stage isolation. Arguments are hard-coded; `shell=True` is never used; no untrusted input reaches a shell. |
| `B607` | `src/pipeline/monthly_release_refresh.py` | Calls the `git` binary on `PATH` to tag a release. Developer-run tooling against a trusted environment. |
| `B105` | `src/validation/run_full_project_validation.py` | False positive — `"PASS"`/`"WARN"`/`"FAIL"` are check-status labels in an ordering map, not credentials. Suppressed inline with `# nosec`. |

There are **no blanket `# nosec` suppressions** and no disabled high-severity checks.

## Dependencies

Runtime dependencies are pinned to compatible ranges in [`pyproject.toml`](pyproject.toml); security-sensitive
lower bounds are raised when a patched release becomes available (currently `pillow>=12.3`). `pip-audit` fails
CI if a newly disclosed CVE affects the resolved environment, prompting a dependency-floor update.

## Reporting a vulnerability

This is a portfolio/showcase project. If you spot a security issue, please open a GitHub issue describing it,
or contact the maintainer directly. There is no production deployment to coordinate disclosure around.

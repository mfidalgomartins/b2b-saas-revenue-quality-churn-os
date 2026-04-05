# Governance Readiness Policy

## Purpose
Define release readiness states so validation output is decision-useful and cannot be interpreted as binary pass/fail only.

## Ordered Readiness States
1. `publish-blocked`
2. `not committee-grade`
3. `screening-grade only`
4. `decision-support only`
5. `analytically acceptable`
6. `technically valid`

## State Meaning
- `publish-blocked`: at least one High/Critical failed control. Distribution is blocked.
- `not committee-grade`: failed controls exist; outputs require remediation before formal leadership/committee use.
- `screening-grade only`: no hard failures, but warning/severity profile is too weak for decision authority.
- `decision-support only`: directional use allowed with explicit caveats and controlled context.
- `analytically acceptable`: minor caveats remain; acceptable for leadership review when caveats are disclosed.
- `technically valid`: all governed controls pass with no warnings or failures.

## Gate Enforcement
Use:

```bash
python3 src/validation/check_validation_gate.py \
  --summary-path reports/formal_validation_summary.json \
  --max-warn 0 \
  --max-fail 0 \
  --max-high-severity 0 \
  --max-critical-severity 0 \
  --min-readiness-tier "technically valid"
```

## Governance Rule
If governance tier is below target, release is blocked regardless of any visually appealing dashboard or narrative quality.

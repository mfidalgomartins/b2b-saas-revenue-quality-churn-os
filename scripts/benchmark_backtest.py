"""Micro-benchmark for the calibration backtest hotspot.

Times the two functions the performance work optimised — `build_trailing_panel`
(grouped trailing features) and `attach_forward_churn` (forward-outcome labels) —
on the real seeded panel, so the speedup is reproducible and regressions are
visible. Run from the repo root after the pipeline has produced the processed
layer:

    make benchmark        # or: python -m scripts.benchmark_backtest
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from pathlib import Path

from src.scoring.backtest_scoring_calibration import (
    attach_forward_churn,
    build_trailing_panel,
)


def _time[T](fn: Callable[[], T], repeats: int) -> tuple[float, T]:
    if repeats < 1:
        raise ValueError("repeats must be at least 1")

    start = time.perf_counter()
    result = fn()
    best = time.perf_counter() - start
    for _ in range(repeats - 1):
        start = time.perf_counter()
        result = fn()
        best = min(best, time.perf_counter() - start)
    return best, result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=str, default=".")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--horizon-months", type=int, default=3)
    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()

    panel_time, panel = _time(lambda: build_trailing_panel(base_dir), args.repeats)
    fwd_time, _ = _time(lambda: attach_forward_churn(panel, args.horizon_months), args.repeats)

    rows = len(panel)
    print(f"rows                     : {rows:,}")
    print(f"build_trailing_panel     : {panel_time * 1000:8.1f} ms  (best of {args.repeats})")
    print(f"attach_forward_churn     : {fwd_time * 1000:8.1f} ms  (best of {args.repeats})")
    print(f"combined                 : {(panel_time + fwd_time) * 1000:8.1f} ms")


if __name__ == "__main__":
    main()

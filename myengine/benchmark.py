from __future__ import annotations

import statistics
from pathlib import Path
from typing import List

from .q13_engine import run_q13


def benchmark_q13(
    data_dir: Path,
    runs: int = 6,
    out_path: str | None = None,
    log_timings: bool = False,
    log_every_batches: int = 25,
) -> None:
    """
    Run Q13 multiple times.
    Requirement-friendly usage:
    - runs >= 6
    - ignore the first run
    - average the rest
    """
    if runs < 2:
        raise ValueError("Benchmark runs must be at least 2.")

    totals: List[float] = []

    print(f"Benchmarking Q13 on {data_dir}")
    print(f"Runs: {runs} (first run treated as warm-up)\n")

    for i in range(runs):
        effective_out = out_path if i == runs - 1 and out_path else None
        _, timings = run_q13(
            data_dir=data_dir,
            out_path=effective_out,
            return_timings=True,
            log_timings=log_timings,
            log_every_batches=log_every_batches,
        )
        totals.append(timings["total_s"])

        print(
            f"Run {i + 1}: "
            f"total={timings['total_s']:.6f}s, "
            f"customer={timings['customer_scan_s']:.6f}s, "
            f"orders={timings['orders_scan_and_filter_s']:.6f}s, "
            f"hist={timings['histogram_build_s']:.6f}s, "
            f"sort={timings['final_sort_s']:.6f}s, "
            f"write={timings['output_write_s']:.6f}s"
        )

    warmup = totals[0]
    measured = totals[1:]
    avg = statistics.mean(measured)
    median = statistics.median(measured)
    stddev = statistics.pstdev(measured)

    print("\nSummary")
    print(f"Warm-up run: {warmup:.6f}s")
    print(f"Average of last {len(measured)} runs: {avg:.6f}s")
    print(f"Median of last {len(measured)} runs: {median:.6f}s")
    print(f"Std dev of last {len(measured)} runs: {stddev:.6f}s")
from __future__ import annotations

import csv
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pyarrow.compute as pc

from .parquet_utils import get_max_int_column, iter_record_batches


ResultRows = List[Tuple[int, int]]


def matches_special_requests(comment: str, memo: Dict[str, bool]) -> bool:
    """
    Return True if the string matches SQL LIKE '%special%requests%'.

    SQL pattern means:
    - "special" occurs somewhere
    - and later in the same string, "requests" occurs after that

    We memoize results by full comment string because TPC-H comments may repeat.
    """
    cached = memo.get(comment)
    if cached is not None:
        return cached

    first = comment.find("special")
    if first == -1:
        memo[comment] = False
        return False

    second = comment.find("requests", first + len("special"))
    matched = second != -1
    memo[comment] = matched
    return matched


def load_customers(customer_path: Path) -> Tuple[List[int], List[bool]]:
    """
    Build:
    - dense count array indexed by customer key
    - validity array telling us which customer keys exist

    This preserves LEFT OUTER JOIN semantics because every customer starts at count 0.
    """
    max_custkey = get_max_int_column(customer_path, "c_custkey")
    if max_custkey < 0:
        raise ValueError("No customer keys found in customer.parquet")

    counts = np.zeros(max_custkey + 1, dtype=np.int32)
    valid_customer = np.zeros(max_custkey + 1, dtype=bool)

    for batch in iter_record_batches(customer_path, ["c_custkey"]):
        valid_customer[batch.column(0).to_numpy()] = True

    return counts, valid_customer


def process_orders(
    orders_path: Path,
    counts: np.ndarray,
    valid_customer: np.ndarray,
    log_timings: bool = False,
    log_every_batches: int = 25,
) -> None:
    """
    Scan orders and apply:
        o_comment NOT LIKE '%special%requests%'

    For each qualifying order, increment counts[o_custkey].
    """
    processed_batches = 0
    processed_orders = 0
    stage_start = time.perf_counter()
    log_every_batches = max(1, log_every_batches)

    for batch in iter_record_batches(orders_path, ["o_custkey", "o_comment"]):
        custkeys = batch.column(0)
        comments = batch.column(1)

        # Vectorized SQL LIKE equivalent in Arrow: comments matching special.*requests.
        matched = pc.match_substring_regex(comments, "special.*requests")
        keep_mask = pc.fill_null(pc.invert(matched), False)
        qualifying_custkeys = pc.filter(custkeys, keep_mask)

        if len(qualifying_custkeys):
            batch_counts = pc.value_counts(qualifying_custkeys)
            values = pc.struct_field(batch_counts, "values").to_numpy()
            freqs = pc.struct_field(batch_counts, "counts").to_numpy()
            present = valid_customer[values]
            if present.all():
                counts[values] += freqs
            else:
                counts[values[present]] += freqs[present]

        processed_batches += 1
        processed_orders += batch.num_rows
        if log_timings and processed_batches % log_every_batches == 0:
            elapsed = time.perf_counter() - stage_start
            print(
                "[timing] orders progress: "
                f"batches={processed_batches}, "
                f"rows={processed_orders}, "
                f"elapsed={elapsed:.6f}s"
            )

    if log_timings and processed_batches % log_every_batches != 0:
        elapsed = time.perf_counter() - stage_start
        print(
            "[timing] orders progress: "
            f"batches={processed_batches}, rows={processed_orders}, elapsed={elapsed:.6f}s"
        )


def build_histogram(counts: np.ndarray, valid_customer: np.ndarray) -> Counter:
    """
    Build:
        c_count -> custdist
    """
    histogram_counts = np.bincount(counts[valid_customer])
    return Counter(
        {
            c_count: int(custdist)
            for c_count, custdist in enumerate(histogram_counts)
            if custdist
        }
    )


def sort_results(histogram: Counter) -> ResultRows:
    """
    Sort by:
    - custdist DESC
    - c_count DESC
    """
    rows = [(c_count, custdist) for c_count, custdist in histogram.items()]
    rows.sort(key=lambda row: (-row[1], -row[0]))
    return rows


def write_results_csv(results: ResultRows, out_path: Path) -> None:
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["c_count", "custdist"])
        writer.writerows(results)


def run_q13(
    data_dir: Path,
    out_path: str | None = None,
    return_timings: bool = False,
    log_timings: bool = False,
    log_every_batches: int = 25,
):
    """
    Execute Q13 specialized processor.

    Returns:
    - results only, or
    - (results, timings) if return_timings=True
    """
    customer_path = data_dir / "customer.parquet"
    orders_path = data_dir / "orders.parquet"

    t0 = time.perf_counter()
    if log_timings:
        print(f"[timing] starting q13 run on {data_dir}")

    counts, valid_customer = load_customers(customer_path)
    t1 = time.perf_counter()
    if log_timings:
        print(
            "[timing] customer stage complete: "
            f"elapsed={t1 - t0:.6f}s, cumulative={t1 - t0:.6f}s"
        )

    process_orders(
        orders_path,
        counts,
        valid_customer,
        log_timings=log_timings,
        log_every_batches=log_every_batches,
    )
    t2 = time.perf_counter()
    if log_timings:
        print(
            "[timing] orders stage complete: "
            f"elapsed={t2 - t1:.6f}s, cumulative={t2 - t0:.6f}s"
        )

    histogram = build_histogram(counts, valid_customer)
    t3 = time.perf_counter()
    if log_timings:
        print(
            "[timing] histogram stage complete: "
            f"elapsed={t3 - t2:.6f}s, cumulative={t3 - t0:.6f}s"
        )

    results = sort_results(histogram)
    t4 = time.perf_counter()
    if log_timings:
        print(
            "[timing] sort stage complete: "
            f"elapsed={t4 - t3:.6f}s, cumulative={t4 - t0:.6f}s"
        )

    if out_path:
        write_results_csv(results, Path(out_path))
    t5 = time.perf_counter()

    timings = {
        "customer_scan_s": t1 - t0,
        "orders_scan_and_filter_s": t2 - t1,
        "histogram_build_s": t3 - t2,
        "final_sort_s": t4 - t3,
        "output_write_s": t5 - t4,
        "processing_total_s": t4 - t0,
        "total_s": t5 - t0,
    }

    if log_timings:
        print(
            "[timing] final totals: "
            f"processing={timings['processing_total_s']:.6f}s, "
            f"write={timings['output_write_s']:.6f}s, "
            f"end_to_end={timings['total_s']:.6f}s"
        )

    if return_timings:
        return results, timings
    return results
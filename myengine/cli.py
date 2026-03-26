import argparse
from pathlib import Path

from .benchmark import benchmark_q13
from .q13_engine import run_q13
from .compare import compare_csv_outputs


SUPPORTED_SF_VALUES = {"0.5", "1", "2", "5"}


def resolve_data_dir(data: str | None, sf: str | None) -> Path:
    if data:
        return Path(data)

    if sf is None:
        raise ValueError("Either --data or --sf must be provided.")

    if sf not in SUPPORTED_SF_VALUES:
        allowed = ", ".join(sorted(SUPPORTED_SF_VALUES, key=lambda x: float(x)))
        raise ValueError(f"Unsupported scale factor '{sf}'. Allowed values: {allowed}")

    return Path("data") / f"sf{sf}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Specialized single-query processor for TPC-H Q13 over Parquet files."
    )
    data_group = parser.add_mutually_exclusive_group(required=True)
    data_group.add_argument(
        "--data",
        default=None,
        help="Path to scale-factor folder, e.g. data/sf1",
    )
    data_group.add_argument(
        "--sf",
        choices=["0.5", "1", "2", "5"],
        default=None,
        help="Scale factor shortcut. Resolves to data/sf<value>, e.g. --sf 2 => data/sf2",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional CSV output path.",
    )
    parser.add_argument(
        "--benchmark",
        type=int,
        default=0,
        help="Run benchmark mode with N runs. Recommended: 6",
    )
    parser.add_argument(
        "--compare-to",
        default=None,
        help="Optional path to DuckDB CSV output to compare against.",
    )
    parser.add_argument(
        "--log-timings",
        action="store_true",
        help="Print in-progress and final timing logs while processing.",
    )
    parser.add_argument(
        "--log-every-batches",
        type=int,
        default=25,
        help="When --log-timings is set, print order-scan progress every N batches.",
    )

    args = parser.parse_args()

    data_dir = resolve_data_dir(args.data, args.sf)

    if args.benchmark and args.benchmark > 0:
        benchmark_q13(
            data_dir=data_dir,
            runs=args.benchmark,
            out_path=args.out,
            log_timings=args.log_timings,
            log_every_batches=args.log_every_batches,
        )
    else:
        results = run_q13(
            data_dir=data_dir,
            out_path=args.out,
            log_timings=args.log_timings,
            log_every_batches=args.log_every_batches,
        )
        print("c_count,custdist")
        for c_count, custdist in results:
            print(f"{c_count},{custdist}")

    if args.compare_to and args.out:
        ok = compare_csv_outputs(Path(args.out), Path(args.compare_to))
        if ok:
            print("Output matches reference CSV.")
        else:
            print("Output does NOT match reference CSV.")
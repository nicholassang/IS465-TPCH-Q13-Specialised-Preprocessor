# Q13 Specialized Processor

Specialized single-query processor for TPC-H Q13 over Parquet files.

## Query
TPC-H Q13

## Strategy
Instead of executing Q13 as:
- left outer join
- group by customer
- group by count bucket

this engine executes it as:
- load all customers and initialize per-customer counts to 0
- scan orders and apply `o_comment NOT LIKE '%special%requests%'`
- increment qualifying customer counts directly
- build a histogram of counts
- sort final output

This preserves the exact semantics of Q13 while avoiding a large join intermediate and the first large hash aggregation used by DuckDB.

## Requirements
- Python 3.10+
- `pip install -r requirements.txt`

## Data Requirements
For this Q13 processor, only these Parquet files are required in the selected data directory:

- `customer.parquet`
- `orders.parquet`

Other TPC-H tables (`lineitem`, `nation`, `part`, `partsupp`, `region`, `supplier`) are not used by this implementation.

## Implementation Summary

### Python (`myengine`)
- Entry point: `python -m myengine`
- Reads customer metadata and customer keys to initialize dense per-customer count storage.
- Reads orders columns (`o_custkey`, `o_comment`) with PyArrow.
- Applies Q13 predicate (`o_comment NOT LIKE '%special%requests%'`) and aggregates qualifying orders by customer.
- Builds the final histogram (`c_count -> custdist`) and sorts by `custdist DESC, c_count DESC`.
- Supports stage-level timing logs and benchmark mode.

### C++ (`native/q13_native.cpp`)
- Entry point: `build-native\\q13_native.exe`
- Uses Arrow/Parquet C++ readers against the same Parquet files.
- Streams orders in record batches and processes comment filtering plus per-customer counting in parallel.
- Reuses the same Q13 semantics and final ordering as the Python path.
- Exposes comparable benchmark and timing logs to evaluate against DuckDB.

## Run
```bash
python -m myengine --data data/sf1 --out result.csv
```

Or using a scale-factor shortcut (supported: 0.5, 1, 2, 5):

```bash
python -m myengine --sf 1 --out result.csv
```

## Benchmark 

Python Benchmark (Use this to test average of 5 runs, change sf where needed)
```bash
python -m myengine --data data/sf1 --benchmark 6 --out result.csv
```

Or:

```bash
python -m myengine --sf 2 --benchmark 6 --out result.csv
```

## Timing Logs
Show in-progress timing during the orders scan plus final totals:

```bash
python -m myengine --sf 5 --out result.csv --log-timings --log-every-batches 25
```

## Native C++ Branch
This repository also has a native C++ implementation on branch `cpp-q13-native`.

Build from a Visual Studio developer shell:

```bash
cmake -S . -B build-native -G "NMake Makefiles"
cmake --build build-native --config Release
```

Run:

```bash
build-native\q13_native.exe --data data\sf1 --out result.csv
```

C++ Benchmark  (Use this to test average of 5 runs, change sf where needed):

```bash
build-native\q13_native.exe --data data\sf1 --benchmark 6 --log-timings --log-every-batches 5
```
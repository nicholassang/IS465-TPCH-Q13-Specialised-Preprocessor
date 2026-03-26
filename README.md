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

## Run
```bash
python -m myengine --data data/sf1 --out result.csv
```

Or using a scale-factor shortcut (supported: 0.5, 1, 2, 5):

```bash
python -m myengine --sf 1 --out result.csv
```

## Benchmark
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
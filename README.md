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

Benchmark with similar timing logs:

```bash
build-native\q13_native.exe --data data\sf1 --benchmark 6 --log-timings --log-every-batches 5
```

## DuckDB Reference Results

The repository includes DuckDB reference CSV files for each scale factor (`duckdb_result_sf=*.csv`). These are the ground truth outputs to validate engine implementations.

### Generate/Regenerate DuckDB Reference Results

To generate or regenerate the DuckDB reference CSVs for all scale factors, use Python:

```bash
python -c "
import duckdb, pathlib, csv

BASE = pathlib.Path('data')
SFS = [('05', 'sf0.5'), ('1', 'sf1'), ('2', 'sf2'), ('5', 'sf5')]

Q = '''
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c.c_custkey,
        COUNT(o.o_orderkey) AS c_count
    FROM
        customer c
        LEFT OUTER JOIN orders o
            ON c.c_custkey = o.o_custkey
            AND o.o_comment NOT LIKE '%special%requests%'
    GROUP BY c.c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
'''

for sf_label, sf_dir in SFS:
    sf_path = BASE / sf_dir
    cust = str(sf_path / 'customer.parquet').replace('\\\\', '/')
    ord_ = str(sf_path / 'orders.parquet').replace('\\\\', '/')

    con = duckdb.connect()
    con.execute(f'CREATE VIEW customer AS SELECT * FROM read_parquet(\"{cust}\")')
    con.execute(f'CREATE VIEW orders AS SELECT * FROM read_parquet(\"{ord_}\")')

    rows = con.execute(Q).fetchall()

    fname = f'duckdb_result_sf={sf_label}.csv'
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['c_count', 'custdist'])
        writer.writerows(rows)

    print(f'sf={sf_label}: {len(rows)} rows -> {fname}')
    con.close()
"
```

**Requirements:** DuckDB must be installed (`pip install duckdb`).

### Run Q13 Query in DuckDB CLI

To run the Q13 query directly in DuckDB for a specific scale factor:

```bash
duckdb -c "
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c.c_custkey,
        COUNT(o.o_orderkey) AS c_count
    FROM
        'data/sf1/customer.parquet' c
        LEFT OUTER JOIN 'data/sf1/orders.parquet' o
            ON c.c_custkey = o.o_custkey
            AND o.o_comment NOT LIKE '%special%requests%'
    GROUP BY c.c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"
```

**Change `sf1` to `sf0.5`, `sf2`, or `sf5` for other scale factors.**

Alternatively, save the query to a file and run it:

```bash
duckdb < query.sql
```

### Validate Engine Against DuckDB

Compare your Python or C++ engine outputs against the DuckDB reference:

**Python:**
```bash
python -m myengine --sf 1 --compare-to duckdb_result_sf=1.csv --out result.csv
```

**C++:**
```bash
build-native\q13_native.exe --data data\sf1 --compare-to duckdb_result_sf=1.csv --out result.csv
```

The `--compare-to` flag will display `Output matches reference CSV.` if results are identical.
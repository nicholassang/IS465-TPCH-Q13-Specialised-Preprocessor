import duckdb

# Connect to an in-memory DuckDB instance
con = duckdb.connect()

# Use forward slashes (or double backslashes) in Windows
result = con.execute(
    "SELECT COUNT(*) FROM 'customer_q3.parquet' WHERE c_mktsegment = 'BUILDING'"
).fetchall()

print(result)
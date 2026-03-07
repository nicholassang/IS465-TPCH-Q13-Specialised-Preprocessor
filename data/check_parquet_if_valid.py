import duckdb

# Connect to an in-memory database
con = duckdb.connect()

# Run the Q3 query directly on Parquet files
query = """
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM lineitem_q3.parquet l
JOIN orders_q3.parquet o ON l.l_orderkey = o.o_orderkey
JOIN customer_q3.parquet c ON o.o_custkey = c.c_custkey
WHERE
    c.c_mktsegment = 'BUILDING'
    AND o.o_orderdate < '1995-03-15'
    AND l.l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
"""

result = con.execute(query).fetchdf()
print(result)
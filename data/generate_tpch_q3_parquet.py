import duckdb

# Connect to an in-memory DuckDB database
con = duckdb.connect()

# Install and load TPC-H generator
con.execute("INSTALL tpch")
con.execute("LOAD tpch")

# Generate TPC-H dataset
# Change sf to 0.1, 1, or 10 depending on desired size
scale_factor = 0.1
print(f"Generating TPC-H data with scale factor {scale_factor}...")
con.execute(f"CALL dbgen(sf={scale_factor})")

print("Exporting Query 3 projection tables to Parquet...")

# Customer table 
con.execute("""
COPY (
    SELECT
        c_custkey,
        c_mktsegment
    FROM customer
)
TO 'customer_q3.parquet'
(FORMAT PARQUET);
""")

# Orders table
con.execute("""
COPY (
    SELECT
        o_orderkey,
        o_custkey,
        o_orderdate,
        o_shippriority
    FROM orders
)
TO 'orders_q3.parquet'
(FORMAT PARQUET);
""")

# Lineitem table
con.execute("""
COPY (
    SELECT
        l_orderkey,
        l_extendedprice,
        l_discount,
        l_shipdate
    FROM lineitem
)
TO 'lineitem_q3.parquet'
(FORMAT PARQUET);
""")

print("Done.")
print("Generated files:")
print(" - customer_q3.parquet")
print(" - orders_q3.parquet")
print(" - lineitem_q3.parquet")
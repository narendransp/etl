import os
import duckdb
import pandas as pd


# Paths

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEANED_DIR = os.path.join(BASE_DIR, "output")   # Folder with star schema CSVs
OUTPUT_DIR = os.path.join(BASE_DIR, "sql_outputs")   # Folder with star schema CSVs
SQL_DIR = os.path.join(BASE_DIR, "sql")         # Folder with SQL files

os.makedirs(OUTPUT_DIR, exist_ok=True)


# Helpers

def read_sql_file(file_path):
    """Read SQL file contents"""
    with open(file_path, "r", encoding="utf-8-sig") as f:
        return f.read().strip()

def safe_read_csv(file_path):
    """Load CSV safely, return empty DataFrame if missing"""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return pd.DataFrame()
    return pd.read_csv(file_path)


# Load CSVs

customers = safe_read_csv(os.path.join(CLEANED_DIR, "dim_customers.csv"))
products  = safe_read_csv(os.path.join(CLEANED_DIR, "dim_products.csv"))
dates     = safe_read_csv(os.path.join(CLEANED_DIR, "dim_date.csv"))
orders    = safe_read_csv(os.path.join(CLEANED_DIR, "fact_orders.csv"))

print(" Data loaded")
print("\nCustomers head:\n", customers.head())
print("\nOrders head:\n", orders.head())

# DuckDB Setup

conn = duckdb.connect()

# Register DataFrames
conn.register("customers_df", customers)
conn.register("products_df", products)
conn.register("orders_df", orders)
conn.register("dates_df", dates)

# Create physical tables
conn.execute("CREATE OR REPLACE TABLE customers AS SELECT * FROM customers_df")
conn.execute("CREATE OR REPLACE TABLE products AS SELECT * FROM products_df")
conn.execute("CREATE OR REPLACE TABLE orders AS SELECT * FROM orders_df")
conn.execute("CREATE OR REPLACE TABLE dates AS SELECT * FROM dates_df")

# Row counts
print("\n Row counts:")
for t in ["customers", "products", "orders", "dates"]:
    cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"{t}: {cnt}")

print("\nDuckDB tables:", conn.execute("SHOW TABLES").fetchall())

# Run SQL Queries

if os.path.exists(SQL_DIR):
    for file_name in sorted(os.listdir(SQL_DIR)):
        if not file_name.endswith(".sql"):
            continue

        sql_path = os.path.join(SQL_DIR, file_name)
        sql_text = read_sql_file(sql_path)

        # Support multiple queries in one .sql file
        queries = [q.strip() for q in sql_text.split(";") if q.strip()]

        print(f"\n--- Executing {file_name} ---")
        for i, query in enumerate(queries, start=1):
            print(f"\nQuery {i}:\n{query}\n")
            try:
                result_df = conn.execute(query).fetchdf()
                if result_df.empty:
                    print(" Query returned no rows.")
                else:
                    print(result_df.head(), "\n")
                    # Save results to CSV
                    output_file = os.path.join(
                        OUTPUT_DIR, f"{file_name.replace('.sql','')}_query{i}.csv"
                    )
                    result_df.to_csv(output_file, index=False)
                    print(f" Saved → {output_file}")
            except Exception as e:
                print(f" Error in {file_name}, Query {i}: {e}")
else:
    print(" No SQL folder found, skipping queries.")

conn.close()
print("\n All done!")
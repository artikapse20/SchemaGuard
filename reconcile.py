"""DuckDB-based reconciliation example."""
from pathlib import Path
import duckdb
from .config import OUTPUT_DIR

def main():
    target = OUTPUT_DIR / "orders.parquet"
    if not target.exists():
        print("No target dataset yet. Run the pipeline first.")
        return
    con = duckdb.connect()
    path = str(target).replace("\\", "/").replace("'", "''")
    print(con.execute(f"""
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT order_id) AS distinct_order_ids,
               ROUND(SUM(amount), 2) AS total_amount,
               MIN(order_ts) AS first_order_ts,
               MAX(order_ts) AS last_order_ts
        FROM read_parquet('{path}')
    """).df().to_string(index=False))

if __name__ == "__main__":
    main()

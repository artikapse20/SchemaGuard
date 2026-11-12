"""Create deterministic synthetic Parquet batches for demos and tests."""
from pathlib import Path
import argparse
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_COLUMNS = ["order_id", "customer_id", "order_ts", "amount", "status"]

def make_batches(output_dir: Path, rows: int = 30, seed: int = 42) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    customers = [f"C{i:04d}" for i in range(1, 21)]
    records = []
    for i in range(rows):
        records.append({
            "order_id": f"O{i+1:06d}",
            "customer_id": rng.choice(customers),
            "order_ts": pd.Timestamp("2026-01-01") + pd.Timedelta(minutes=i * 7),
            "amount": round(rng.uniform(10, 500), 2),
            "status": rng.choice(["PLACED", "PAID", "SHIPPED"]),
        })

    # Batch 1: original contract.
    df1 = pd.DataFrame(records[: max(1, rows // 3)])
    pq.write_table(pa.Table.from_pandas(df1, preserve_index=False), output_dir / "batch_001_v1.parquet")

    # Batch 2: approved nullable additions plus a duplicate of the first record.
    start = max(1, rows // 3)
    end = max(start + 1, 2 * rows // 3)
    df2 = pd.DataFrame(records[start:end])
    if len(df2):
        df2 = pd.concat([df2, df1.iloc[[0]]], ignore_index=True)
    df2["discount_amount"] = 0.0
    df2["coupon_code"] = None
    pq.write_table(pa.Table.from_pandas(df2, preserve_index=False), output_dir / "batch_002_v2.parquet")

    # Batch 3: record-level quality examples (missing customer, negative amount, unknown FK).
    tail = pd.DataFrame(records[end:])
    if len(tail):
        tail.loc[tail.index[0], "customer_id"] = "C9999"  # invalid reference
        if len(tail) > 1:
            tail.loc[tail.index[1], "amount"] = -5.0
        tail["discount_amount"] = 0.0
        tail["coupon_code"] = None
        pq.write_table(pa.Table.from_pandas(tail, preserve_index=False), output_dir / "batch_003_quality.parquet")

    # Batch 4: deliberately breaking schema: amount becomes string. Whole file is quarantined.
    breaking = pd.DataFrame([{
        "order_id": "O_BREAK_001", "customer_id": "C0001",
        "order_ts": pd.Timestamp("2026-02-01"), "amount": "not-a-number",
        "status": "PAID"
    }])
    pq.write_table(pa.Table.from_pandas(breaking, preserve_index=False), output_dir / "batch_004_breaking.parquet")

    # Stable reference table used for foreign-key validation.
    pq.write_table(pa.Table.from_pandas(
        pd.DataFrame({"customer_id": customers}), preserve_index=False
    ), output_dir.parent / "reference" / "customers.parquet")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/input"))
    args = parser.parse_args()
    make_batches(args.output, args.rows, args.seed)
    print(f"Generated synthetic batches in {args.output.resolve()}")

if __name__ == "__main__":
    main()

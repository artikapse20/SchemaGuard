"""Schema-aware, idempotent local Parquet ingestion pipeline."""
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import argparse
import json
import logging
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import (INPUT_DIR, REFERENCE_DIR, OUTPUT_DIR, QUARANTINE_DIR,
                     REPORT_DIR, REQUIRED_SCHEMA, APPROVED_OPTIONAL_SCHEMA, ALLOWED_STATUSES)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("schemaguard")

def _arrow_type_name(field: pa.Field) -> str:
    t = field.type
    if pa.types.is_string(t) or pa.types.is_large_string(t): return "string"
    if pa.types.is_floating(t): return "double"
    if pa.types.is_integer(t): return "integer"
    if pa.types.is_timestamp(t): return "timestamp"
    if pa.types.is_boolean(t): return "boolean"
    return str(t)

def inspect_schema(schema: pa.Schema) -> tuple[bool, list[str]]:
    """Return compatibility and explanatory drift messages."""
    errors = []
    actual = {f.name: _arrow_type_name(f) for f in schema}
    for name, expected in REQUIRED_SCHEMA.items():
        if name not in actual:
            errors.append(f"Missing required column: {name}")
        elif actual[name] != expected:
            errors.append(f"Breaking type change for {name}: expected {expected}, got {actual[name]}")
    for name, actual_type in actual.items():
        if name not in REQUIRED_SCHEMA:
            approved = APPROVED_OPTIONAL_SCHEMA.get(name)
            if approved is None:
                errors.append(f"Unapproved new column: {name}")
            elif actual_type != approved:
                errors.append(f"Breaking type change for optional column {name}: expected {approved}, got {actual_type}")
    return not errors, errors

def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)

def _append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, default=str) + "\n")

def run_pipeline(input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR,
                 quarantine_dir: Path = QUARANTINE_DIR, report_dir: Path = REPORT_DIR,
                 reference_file: Path = REFERENCE_DIR / "customers.parquet",
                 run_id: str | None = None) -> dict:
    run_id = run_id or str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    output_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    target_file = output_dir / "orders.parquet"
    metrics_file = report_dir / "run_metrics.jsonl"
    accepted_frames, rejected_rows = [], []
    input_rows = accepted_rows = rejected_count = duplicate_count = schema_quarantined = 0
    errors = []
    customer_ids = set()
    if reference_file.exists():
        customer_ids = set(pd.read_parquet(reference_file)["customer_id"].dropna().astype(str))
    seen_in_run = set()
    existing = pd.read_parquet(target_file) if target_file.exists() else pd.DataFrame()

    files = sorted(input_dir.glob("*.parquet")) if input_dir.exists() else []
    for source in files:
        try:
            table = pq.read_table(source)
            compatible, drift_errors = inspect_schema(table.schema)
            if not compatible:
                schema_quarantined += 1
                errors.extend([f"{source.name}: {e}" for e in drift_errors])
                _append_jsonl(quarantine_dir / "schema_rejects.jsonl", {
                    "run_id": run_id, "source_file": source.name, "reason": drift_errors,
                    "quarantined_at": datetime.now(timezone.utc).isoformat()
                })
                continue
            df = table.to_pandas()
            input_rows += len(df)
            for col in APPROVED_OPTIONAL_SCHEMA:
                if col not in df.columns:
                    df[col] = None
            for col in REQUIRED_SCHEMA:
                if col not in df.columns:
                    df[col] = None
            df["source_file"] = source.name
            df["run_id"] = run_id
            valid_records = []
            for idx, row in df.iterrows():
                reasons = []
                for col in ("order_id", "customer_id", "order_ts", "amount", "status"):
                    if pd.isna(row[col]):
                        reasons.append(f"missing_required:{col}")
                if not pd.isna(row["amount"]):
                    try:
                        if float(row["amount"]) < 0: reasons.append("amount_must_be_non_negative")
                    except (TypeError, ValueError):
                        reasons.append("amount_must_be_numeric")
                if not pd.isna(row["status"]) and row["status"] not in ALLOWED_STATUSES:
                    reasons.append("invalid_status")
                if not pd.isna(row["customer_id"]) and str(row["customer_id"]) not in customer_ids:
                    reasons.append("invalid_customer_reference")
                key = None if pd.isna(row["order_id"]) else str(row["order_id"])
                if key and (key in seen_in_run or (not existing.empty and key in set(existing.get("order_id", [])))):
                    duplicate_count += 1
                    reasons.append("duplicate_order_id")
                if key: seen_in_run.add(key)
                if reasons:
                    rejected_count += 1
                    rejected_rows.append({
                        **row.to_dict(), "reject_reason": "|".join(reasons),
                        "source_row_number": int(idx) + 1
                    })
                else:
                    valid_records.append(row.to_dict())
            if valid_records:
                accepted_frames.append(pd.DataFrame(valid_records))
                accepted_rows += len(valid_records)
        except Exception as exc:
            errors.append(f"{source.name}: {type(exc).__name__}: {exc}")
            _append_jsonl(quarantine_dir / "file_errors.jsonl", {
                "run_id": run_id, "source_file": source.name,
                "reason": f"{type(exc).__name__}: {exc}",
                "quarantined_at": datetime.now(timezone.utc).isoformat()
            })

    if rejected_rows:
        reject_path = quarantine_dir / f"rejected_records_{run_id}.parquet"
        _write_parquet(pd.DataFrame(rejected_rows), reject_path)

    new_data = pd.concat(accepted_frames, ignore_index=True) if accepted_frames else pd.DataFrame()
    if not new_data.empty:
        combined = pd.concat([existing, new_data], ignore_index=True) if not existing.empty else new_data
        combined = combined.drop_duplicates(subset=["order_id"], keep="first")
        _write_parquet(combined, target_file)

    ended = datetime.now(timezone.utc)
    metrics = {
        "run_id": run_id, "started_at": started.isoformat(), "ended_at": ended.isoformat(),
        "input_files": len(files), "input_rows_in_compatible_files": input_rows,
        "accepted_rows": accepted_rows, "rejected_rows": rejected_count,
        "duplicate_rows": duplicate_count, "schema_quarantined_files": schema_quarantined,
        "target_total_rows": len(pd.read_parquet(target_file)) if target_file.exists() else 0,
        "status": "completed_with_errors" if errors or rejected_count or schema_quarantined else "success",
        "errors": errors
    }
    _append_jsonl(metrics_file, metrics)
    (report_dir / f"reconciliation_{run_id}.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8"
    )
    LOG.info("run_id=%s input=%s accepted=%s rejected=%s schema_quarantined=%s",
             run_id, input_rows, accepted_rows, rejected_count, schema_quarantined)
    return metrics

def main():
    parser = argparse.ArgumentParser(description="Run SchemaGuard Parquet pipeline")
    parser.add_argument("--input", type=Path, default=INPUT_DIR)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    result = run_pipeline(input_dir=args.input, run_id=args.run_id)
    print(json.dumps(result, indent=2))
    if result["status"] == "success":
        return
    # Data-quality rejects are an expected, observable outcome; do not treat as process crash.

if __name__ == "__main__":
    main()

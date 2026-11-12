from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from schemaguard.pipeline import run_pipeline, inspect_schema

def write_df(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

def test_schema_accepts_approved_optional_column():
    schema = pa.schema([
        ("order_id", pa.string()), ("customer_id", pa.string()),
        ("order_ts", pa.timestamp("ns")), ("amount", pa.float64()),
        ("status", pa.string()), ("discount_amount", pa.float64())
    ])
    assert inspect_schema(schema)[0] is True

def test_schema_rejects_breaking_type():
    schema = pa.schema([
        ("order_id", pa.string()), ("customer_id", pa.string()),
        ("order_ts", pa.timestamp("ns")), ("amount", pa.string()),
        ("status", pa.string())
    ])
    ok, errors = inspect_schema(schema)
    assert not ok
    assert any("Breaking type change for amount" in e for e in errors)

def test_pipeline_accepts_valid_and_quarantines_invalid(tmp_path):
    inp, out, qua, rep, ref = [tmp_path / x for x in ("input","output","quarantine","reports","customers.parquet")]
    ref.parent.mkdir(parents=True)
    write_df(pd.DataFrame({"customer_id":["C1"]}), ref)
    write_df(pd.DataFrame({
        "order_id":["O1","O2","O3"], "customer_id":["C1","C404","C1"],
        "order_ts":pd.to_datetime(["2026-01-01"]*3), "amount":[10.0,20.0,-1.0],
        "status":["PAID","PAID","PAID"]
    }), inp/"batch.parquet")
    result = run_pipeline(inp,out,qua,rep,ref,run_id="test-run")
    assert result["accepted_rows"] == 1
    assert result["rejected_rows"] == 2
    target = pd.read_parquet(out/"orders.parquet")
    assert target["order_id"].tolist() == ["O1"]

def test_rerun_is_idempotent(tmp_path):
    inp, out, qua, rep, ref = [tmp_path / x for x in ("input","output","quarantine","reports","customers.parquet")]
    ref.parent.mkdir(parents=True)
    write_df(pd.DataFrame({"customer_id":["C1"]}), ref)
    write_df(pd.DataFrame({
        "order_id":["O1"], "customer_id":["C1"], "order_ts":pd.to_datetime(["2026-01-01"]),
        "amount":[10.0], "status":["PAID"]
    }), inp/"batch.parquet")
    run_pipeline(inp,out,qua,rep,ref,run_id="first")
    second = run_pipeline(inp,out,qua,rep,ref,run_id="second")
    assert second["target_total_rows"] == 1
    assert second["duplicate_rows"] == 1

def test_empty_input_is_safe(tmp_path):
    inp, out, qua, rep, ref = [tmp_path / x for x in ("input","output","quarantine","reports","customers.parquet")]
    result = run_pipeline(inp,out,qua,rep,ref,run_id="empty")
    assert result["input_files"] == 0
    assert result["target_total_rows"] == 0


def test_null_required_field_and_duplicate_key_are_rejected(tmp_path):
    inp, out, qua, rep, ref = [tmp_path / x for x in ("input","output","quarantine","reports","customers.parquet")]
    ref.parent.mkdir(parents=True)
    write_df(pd.DataFrame({"customer_id":["C1"]}), ref)
    write_df(pd.DataFrame({
        "order_id":["O1","O1",None], "customer_id":["C1","C1","C1"],
        "order_ts":pd.to_datetime(["2026-01-01"]*3), "amount":[10.0,10.0,5.0],
        "status":["PAID","PAID","PAID"]
    }), inp/"batch.parquet")
    result = run_pipeline(inp,out,qua,rep,ref,run_id="null-duplicate")
    assert result["accepted_rows"] == 1
    assert result["rejected_rows"] == 2
    assert result["duplicate_rows"] == 1

def test_missing_required_column_quarantines_file(tmp_path):
    inp, out, qua, rep, ref = [tmp_path / x for x in ("input","output","quarantine","reports","customers.parquet")]
    ref.parent.mkdir(parents=True)
    write_df(pd.DataFrame({"customer_id":["C1"]}), ref)
    write_df(pd.DataFrame({
        "order_id":["O1"], "customer_id":["C1"],
        "order_ts":pd.to_datetime(["2026-01-01"]), "amount":[10.0]
    }), inp/"missing_status.parquet")
    result = run_pipeline(inp,out,qua,rep,ref,run_id="missing-column")
    assert result["schema_quarantined_files"] == 1
    assert result["target_total_rows"] == 0

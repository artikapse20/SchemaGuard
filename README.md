# SchemaGuard – Intelligent Parquet Schema Evolution Pipeline

A local, reproducible Data Engineering demonstration that reads multiple Parquet batches, detects schema drift, accepts explicitly approved nullable schema additions, quarantines breaking schemas, validates records, prevents duplicate primary keys, and records operational metrics.

## 1. Architecture

```text
Synthetic Parquet batches + customer reference
                  |
                  v
        Schema inspection / drift detection
             /             \
     compatible           breaking
         |                    |
         v                    v
  Add approved nulls     Schema quarantine log
         |
         v
  Row-level validation (required fields, amount, status, FK, duplicate key)
         /                         \
      valid                       invalid
        |                            |
        v                            v
  Target orders.parquet      Quarantine rejected Parquet
        |
        v
  Run metrics + reconciliation JSON + DuckDB report
```

## 2. Requirements

- Python 3.12 recommended
- Windows, macOS, or Linux
- Dependencies in `requirements.txt`
- No cloud account or paid service required

## 3. Setup (Windows PowerShell)

From the repository root:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
$env:PYTHONPATH = "src"
```

If Python 3.12 is not installed, install it from https://www.python.org/downloads/ first. The project has not been validated against Python 3.15; use a supported stable interpreter if dependency installation fails.

## 4. Generate reproducible sample data

```powershell
python -m schemaguard.data_generator --rows 100000 --seed 42
```

The generator creates:
- Original schema batch
- Compatible schema batch with approved optional columns
- Data-quality examples (invalid customer reference and negative amount)
- Deliberately breaking schema batch (amount changes from double to string)
- Synthetic customer reference data

For a quick demo, use `--rows 30`.

## 5. Run the pipeline

```powershell
python -m schemaguard.pipeline
```

Outputs:
- `data/output/orders.parquet`: accepted target records
- `data/quarantine/schema_rejects.jsonl`: breaking file-level schema drift
- `data/quarantine/rejected_records_<run_id>.parquet`: invalid and duplicate records
- `data/reports/run_metrics.jsonl`: append-only operational metrics
- `data/reports/reconciliation_<run_id>.json`: per-run summary

Run it again to demonstrate duplicate prevention. Existing `order_id` values are not inserted again.

## 6. Tests

```powershell
pytest -q
```

Tests cover approved optional schema evolution, breaking type change, invalid foreign key, negative amount, empty input, and rerun idempotency. Add more tests before production use (see limitations).

## 7. Reconciliation

```powershell
python -m schemaguard.reconcile
```

The DuckDB report calculates total rows, distinct order IDs, total amount, and event-time range from the target Parquet file.

## 8. Data dictionary

| Field | Type | Required | Rule |
|---|---|---:|---|
| order_id | string | yes | Stable primary key; unique in target |
| customer_id | string | yes | Must exist in customer reference |
| order_ts | timestamp | yes | Event time; late events retained |
| amount | double | yes | Must be >= 0 |
| status | string | yes | PLACED, PAID, SHIPPED, CANCELLED |
| discount_amount | double | no | Approved nullable addition |
| coupon_code | string | no | Approved nullable addition |
| source_file | string | generated | Original input filename |
| run_id | string | generated | Pipeline execution identifier |

Grain: one row per order. Relationship: many orders may reference one customer. The demo uses synthetic customer IDs. `order_id` is the idempotency key.

## 9. Schema evolution policy

- Required columns and types are declared in `src/schemaguard/config.py`.
- `discount_amount` and `coupon_code` are explicitly approved nullable additions.
- Missing approved optional columns are filled with null.
- Missing required columns, changed required types, unapproved columns, or wrong optional types cause the entire input file to be schema-quarantined.
- No automatic destructive cast is performed.

## 10. Idempotency, retry, and backfill

The target is deduplicated by `order_id` when new records are merged. Re-running the same input therefore does not increase target row count. The run metrics are append-only, so every attempt remains observable. To backfill, place the desired Parquet files in a separate input directory and pass `--input <path>`.

This is a small local demonstration, not a transactional multi-writer system. For concurrent production runs, use a transactional table format or database, atomic writes, and a durable run ledger.

## 11. Performance measurement

Use the 100,000-row generator and record wall-clock runtime from the terminal. The pipeline currently processes files sequentially and performs row-level validation for clarity. A reasonable next optimization is vectorized validation and partitioned/parallel file processing after correctness is established.

## 12. Security and privacy

All sample data is synthetic. Do not commit credentials, tokens, personal data, or private company data. Generated output, quarantine and reports are excluded from Git by `.gitignore`; include only small reviewed sample artifacts if needed.

## 13. Known limitations and next steps

- This is a local single-process prototype; it does not provide distributed processing or transactional concurrency.
- File-level malformed Parquet errors are logged, but recovery depends on rerunning after the source file is corrected.
- Late-arriving records are retained, but no event-time watermark or slowly-changing dimension model is implemented.
- The demo checks customer foreign keys and order uniqueness; production systems should define retention, PII policy, schema version registry, and alert thresholds.
- Expand tests for malformed physical Parquet bytes, missing required columns, null required fields, duplicate IDs within a file, partial failure injection, and late-event ordering.

## 14. AI assistance disclosure

AI tools were used for planning, code scaffolding, explanations, and test ideas. The candidate must review, understand, execute, modify, and validate every submitted line. The full prompt history must be maintained in `docs/prompt_history.md`; add every prompt sent to every AI tool used during the challenge. AI responses are optional under the challenge instructions.

## 15. GitHub submission checklist

1. Run setup from a clean environment.
2. Run the generator, pipeline, reconciliation, and tests.
3. Inspect all outputs and update this README with actual observed results.
4. Review `docs/prompt_history.md` for completeness.
5. Check `git status` and ensure no secrets or unintended generated files are committed.
6. Create a **public** dedicated GitHub repository and push this project.
7. Open the repository in a private/incognito browser window.
8. Submit the repository URL in the YottaFlex portal before the stated deadline.

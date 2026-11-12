# Design decisions and trade-offs

- **PyArrow** reads Parquet metadata and enforces explicit schema checks before conversion.
- **Pandas** keeps row-level validation readable for a small candidate project.
- **DuckDB** provides a lightweight SQL reconciliation report without a server.
- Schema policy is allow-list based: only declared optional fields are accepted.
- Breaking schema changes quarantine the whole file; record-level data quality issues quarantine individual rows.
- `order_id` is the deterministic idempotency key.
- Sequential processing favors clarity and deterministic results over throughput.
- Limitations: no multi-process transaction isolation, distributed execution, schema registry, or automated alerting.

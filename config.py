from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
REFERENCE_DIR = DATA_DIR / "reference"
OUTPUT_DIR = DATA_DIR / "output"
QUARANTINE_DIR = DATA_DIR / "quarantine"
REPORT_DIR = DATA_DIR / "reports"

# The contract: these columns must exist with these logical Arrow types.
REQUIRED_SCHEMA = {
    "order_id": "string",
    "customer_id": "string",
    "order_ts": "timestamp",
    "amount": "double",
    "status": "string",
}
# Only these additional columns are approved. Missing optional columns are added as nulls.
APPROVED_OPTIONAL_SCHEMA = {
    "discount_amount": "double",
    "coupon_code": "string",
}
ALLOWED_STATUSES = {"PLACED", "PAID", "SHIPPED", "CANCELLED"}

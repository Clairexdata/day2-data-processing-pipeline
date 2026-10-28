"""
Day 2 - Multi-File Data Processing Pipeline
Squad Python Specialization - Data Engineering

Usage (suggested):  python pipeline.py --input data/ --output output/
"""
import argparse
import csv
import logging
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS: set[str] = {
    "transaction_id", "date", "branch", "customer_id",
    "product", "quantity", "unit_price", "payment_method",
}

# TODO: decide whether a known alias (e.g. a renamed column) should be mapped or rejected,
# and record the decision in your README.
COLUMN_ALIASES: dict[str, str] = {
    "transaction id": "transaction_id",
    "customer id": "customer_id",
    "unit price": "unit_price",
    "payment method": "payment_method",
     
}


def discover_files(folder: Path, pattern: str = "transactions_*.csv") -> list[Path]:
    """Return the matching CSV files in a stable (date) order using pathlib."""
    return sorted(folder.glob(pattern), key=lambda p: (p.name))



def read_transactions(path: Path) -> tuple[list[str], list[dict]]:
    """Read one file and return (header, rows). Every row should remember its source file."""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = [dict(row, _source_file=path.name) for row in reader]
    return header, rows


def validate_columns(header: list[str], required: set[str] = REQUIRED_COLUMNS) -> set[str]:
    """Return the set of required columns missing from the header (empty set means valid)."""
    normalized_header = {COLUMN_ALIASES.get(col.lower(), col) for col in header}
    missing = required - normalized_header
    return missing
    


def find_problems(row: dict, seen_ids: set[str]) -> list[str]:
    """Return a list of human-readable reasons this row is invalid (empty list means valid).

    Must cover: blank required values, non-numeric or non-positive quantity/price,
    and transaction IDs already seen in this run.
    """
    problems = []
    # Check for blank required values
    for col in REQUIRED_COLUMNS:
        if not row.get(col):
            problems.append(f"Missing required value for '{col}'")
    
    # Check for non-numeric or non-positive quantity
    try:
        quantity = float(row.get("quantity", 0))
        if quantity <= 0:
            problems.append("Quantity must be positive")
    except ValueError:
        problems.append("Quantity must be numeric")
    
    # Check for non-numeric or non-positive unit price
    try:
        unit_price = float(row.get("unit_price", 0))
        if unit_price <= 0:
            problems.append("Unit price must be positive")
    except ValueError:
        problems.append("Unit price must be numeric")
    
    # Check for duplicate transaction IDs
    transaction_id = row.get("transaction_id")
    if transaction_id in seen_ids:
        problems.append(f"Duplicate transaction ID '{transaction_id}'")
    
    return problems


def clean_row(row: dict) -> dict:
    """Return a normalised copy of a valid row (types converted, whitespace stripped)."""
    cleaned_row = {}
    for col in REQUIRED_COLUMNS:
        value = row.get(col, "").strip() if isinstance(row.get(col), str) else row.get(col)
        if col in ["quantity", "unit_price"]:
            cleaned_row[col] = float(value) if value else 0.0
        else:
            cleaned_row[col] = value
    return cleaned_row


def write_csv(rows: list[dict], path: Path, fieldnames: list[str]) -> None:
    """Write rows to CSV, creating parent folders as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_summary(stats: dict) -> str:
    """Format the processing summary shown in the expected-output resource."""
    summary_lines = [
        f"Files processed: {stats.get('files_processed', 0)}",
        f"Rows read: {stats.get('rows_read', 0)}",
        f"Rows written: {stats.get('rows_written', 0)}",
        f"Rows skipped: {stats.get('rows_skipped', 0)}",
    ]
    return "\n".join(summary_lines)


def run_pipeline(input_dir: Path, output_dir: Path) -> dict:
    """Run every step and return the statistics used by build_summary()."""
    # TODO
    pass 
    files = discover_files(input_dir)
    stats = {
        "files_processed": 0,
        "rows_read": 0,
        "rows_written": 0,
        "rows_skipped": 0
    }
    valid_rows = []
    seen_ids = set()
    invalid_rows = []
    output_fields = list(REQUIRED_COLUMNS) + ["_source_file"]

    for path in files:
        header, rows = read_transactions(path)
        stats["files_processed"] += 1
        stats["rows_read"] += len(rows)

        missing_columns = validate_columns(header)
        if missing_columns:
            logging.warning(f"File {path.name} is missing required columns: {missing_columns}")
            stats["rows_skipped"] += len(rows)
            continue

        for row in rows:
            problems = find_problems(row, seen_ids)
            if problems:
                logging.warning(f"Row in file {path.name} has problems: {problems}")
                invalid_rows.append(row)
                stats["rows_skipped"] += 1
                continue

            cleaned_row = clean_row(row)
            valid_rows.append(cleaned_row)
            seen_ids.add(cleaned_row["transaction_id"])
            stats["rows_written"] += 1

            write_csv(valid_rows, output_dir / "cleaned_transactions.csv", output_fields)
            write_csv(invalid_rows, output_dir / "invalid_transactions.csv", list(invalid_rows[0].keys()) if invalid_rows else [])
            pd.DataFrame(valid_rows).to_parquet(output_dir / "cleaned_transactions.parquet", index=False)
    return stats
        

if __name__ == "__main__":
    # TODO: parse arguments, call run_pipeline, print the summary
    
    parser = argparse.ArgumentParser(description="Process transaction CSV files.")
    parser.add_argument("--input", type=Path, required=True, help="Input directory containing transaction CSV files.")
    parser.add_argument("--output", type=Path, required=True, help="Output directory for cleaned CSV files.")
    args = parser.parse_args()
    stats = run_pipeline(args.input, args.output)
    print(build_summary(stats))

"""
Filter companies from companies_raw.csv:
  1. Deduplicate
  2. employees >= 5

Output: data/companies_filtered.csv
"""

import csv
import re
from pathlib import Path

RAW_FILE      = Path(__file__).parent.parent / "data" / "companies_raw.csv"
FILTERED_FILE = Path(__file__).parent.parent / "data" / "companies_filtered.csv"

FIELDS = ["name", "builtin_url", "sectors", "location", "employees", "description"]


def parse_employee_count(emp_str: str) -> int | None:
    """Parse '350 Employees', '1,200 Employees', etc. Returns number or None."""
    if not emp_str:
        return None
    match = re.search(r"[\d,]+", emp_str)
    if not match:
        return None
    return int(match.group().replace(",", ""))


def deduplicate(rows: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for row in rows:
        key = row["builtin_url"] or row["name"]
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def main():
    with open(RAW_FILE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} rows")

    rows = deduplicate(rows)
    print(f"After dedup: {len(rows)}")

    filtered = []
    for row in rows:
        count = parse_employee_count(row.get("employees", ""))
        if count is None or count >= 5:
            filtered.append(row)
        else:
            print(f"  Removed (employees={count}): {row['name']}")

    print(f"After employee filter (>=5): {len(filtered)}")

    FILTERED_FILE.parent.mkdir(exist_ok=True)
    with open(FILTERED_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(filtered)

    print(f"Saved -> {FILTERED_FILE}")


if __name__ == "__main__":
    main()

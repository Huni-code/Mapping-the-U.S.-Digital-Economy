"""
Load companies_raw.csv into SQLite database.
Output: data/companies.db (table: companies_raw)
"""

import csv
import sqlite3
from pathlib import Path

CSV_FILE = Path(__file__).parent.parent / "data" / "companies_raw.csv"
DB_FILE  = Path(__file__).parent.parent / "data" / "companies.db"


def main():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS companies_raw;
        CREATE TABLE companies_raw (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT,
            builtin_url TEXT,
            sectors     TEXT,
            location    TEXT,
            employees   TEXT,
            description TEXT,
            hub         TEXT
        );
    """)

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (r["name"], r["builtin_url"], r["sectors"],
             r["location"], r["employees"], r["description"], r["hub"])
            for r in reader
        ]

    cur.executemany("""
        INSERT INTO companies_raw (name, builtin_url, sectors, location, employees, description, hub)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM companies_raw").fetchone()[0]
    print(f"Loaded {count} rows into {DB_FILE}")

    conn.close()


if __name__ == "__main__":
    main()

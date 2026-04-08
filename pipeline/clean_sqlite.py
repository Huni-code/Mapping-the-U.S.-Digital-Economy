"""
Data cleaning pipeline in SQLite.
Run incrementally — each step adds a new table.

Step 1: Deduplicate by builtin_url (keep first occurrence)
Step 2: Normalize sectors — split "Fintech • Software" into rows
"""

import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"


def step1_deduplicate(cur):
    cur.executescript("""
        DROP TABLE IF EXISTS companies_deduped;
        CREATE TABLE companies_deduped AS
        SELECT *
        FROM companies_raw
        WHERE id IN (
            SELECT MIN(id)
            FROM companies_raw
            GROUP BY builtin_url
        );
    """)
    count = cur.execute("SELECT COUNT(*) FROM companies_deduped").fetchone()[0]
    print(f"Step 1 — Deduplicated: {count} unique companies")


def step2_normalize_sectors(conn, cur):
    cur.executescript("""
        DROP TABLE IF EXISTS company_sectors;
        CREATE TABLE company_sectors (
            company_id  INTEGER,
            sector      TEXT
        );
    """)

    rows = cur.execute("SELECT id, sectors FROM companies_deduped").fetchall()

    pairs = []
    for company_id, sectors in rows:
        if not sectors:
            continue
        for s in sectors.split("•"):
            s = s.strip()
            if s:
                pairs.append((company_id, s))

    cur.executemany("INSERT INTO company_sectors VALUES (?, ?)", pairs)

    # Top sectors
    top = cur.execute("""
        SELECT sector, COUNT(*) as count
        FROM company_sectors
        GROUP BY sector
        ORDER BY count DESC
        LIMIT 15
    """).fetchall()

    print(f"Step 2 — {len(pairs)} sector tags across {len(rows)} companies")
    print("  Top 15 sectors:")
    for sector, count in top:
        print(f"    {sector:<40} {count}")


def step3_normalize_employees(conn, cur):
    # Add columns if not exist
    try:
        cur.execute("ALTER TABLE companies_deduped ADD COLUMN employees_count INTEGER")
        cur.execute("ALTER TABLE companies_deduped ADD COLUMN company_size TEXT")
    except Exception:
        pass  # columns already exist

    rows = cur.execute("SELECT id, employees FROM companies_deduped").fetchall()

    updates = []
    for company_id, employees in rows:
        if not employees:
            updates.append((None, None, company_id))
            continue
        num = int(employees.replace(",", "").replace(" Employees", "").strip())
        if num < 50:
            size = "Startup"
        elif num < 500:
            size = "Small"
        elif num < 5000:
            size = "Mid-size"
        else:
            size = "Enterprise"
        updates.append((num, size, company_id))

    cur.executemany("""
        UPDATE companies_deduped
        SET employees_count = ?, company_size = ?
        WHERE id = ?
    """, updates)

    dist = cur.execute("""
        SELECT company_size, COUNT(*) as count
        FROM companies_deduped
        GROUP BY company_size
        ORDER BY count DESC
    """).fetchall()

    print("Step 3 — Employees normalized. Size distribution:")
    for size, count in dist:
        print(f"    {str(size):<12} {count}")


US_STATES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
    "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
    "Utah", "Vermont", "Virginia", "Washington", "West Virginia",
    "Wisconsin", "Wyoming", "District of Columbia",
}

HUB_TO_STATE = {
    "new-york-city": "New York",
    "chicago": "Illinois",
    "los-angeles": "California",
    "san-francisco": "California",
    "seattle": "Washington",
    "boston": "Massachusetts",
    "austin": "Texas",
    "denver": "Colorado",
    "atlanta": "Georgia",
    "dallas": "Texas",
    "washington-dc": "District of Columbia",
    "miami": "Florida",
    "philadelphia": "Pennsylvania",
    "raleigh": "North Carolina",
    "minneapolis": "Minnesota",
    "portland": "Oregon",
    "nashville": "Tennessee",
    "san-diego": "California",
    "michigan": "Michigan",
}


def step4_extract_state(conn, cur):
    try:
        cur.execute("ALTER TABLE companies_deduped ADD COLUMN state TEXT")
    except Exception:
        pass

    rows = cur.execute("SELECT id, location, hub FROM companies_deduped").fetchall()

    updates = []
    for company_id, location, hub in rows:
        state = None
        if location and "," in location:
            # "City, State, USA" → extract state part
            parts = [p.strip() for p in location.split(",")]
            for part in parts:
                if part in US_STATES:
                    state = part
                    break
        # Fallback to hub if no state found
        if not state and hub:
            state = HUB_TO_STATE.get(hub)
        updates.append((state, company_id))

    cur.executemany("UPDATE companies_deduped SET state = ? WHERE id = ?", updates)

    dist = cur.execute("""
        SELECT state, COUNT(*) as count
        FROM companies_deduped
        GROUP BY state
        ORDER BY count DESC
        LIMIT 15
    """).fetchall()

    null_count = cur.execute("SELECT COUNT(*) FROM companies_deduped WHERE state IS NULL").fetchone()[0]
    print("Step 4 — State extracted. Top 15 states:")
    for state, count in dist:
        print(f"    {str(state):<25} {count}")
    print(f"    NULL: {null_count}")


def main():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    step1_deduplicate(cur)
    step2_normalize_sectors(conn, cur)
    step3_normalize_employees(conn, cur)
    step4_extract_state(conn, cur)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()

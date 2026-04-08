"""
BLS (Bureau of Labor Statistics) Enrichment
Fetches employment count + average hourly wages by tech sector (2015-2024)

API: https://api.bls.gov/publicAPI/v2/timeseries/data/
No API key required (up to 25 series per request, 10 years)

Output: bls_employment table in data/companies.db
"""

import sqlite3
import time
import requests
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"

HEADERS = {"Content-Type": "application/json"}

# Verified BLS CES series IDs (tested against API)
# employment series: ends in 0001, wage series: ends in 0003
# Mapped to professor's 16-sector taxonomy
SECTORS = {
    # → Developer tooling, AI assistants, Productivity, Enterprise, Cybersecurity
    "Computer Systems Design (5415)":   ("CES6054150001", "CES6054150003"),
    # → Enterprise / ERP / HRM, Management consulting
    "Management Consulting (5416)":     ("CES6054160001", "CES6054160003"),
    # → AI foundation models, Inventing axis, Scientific R&D
    "Scientific R&D Services (5417)":   ("CES6054170001", "CES6054170003"),
    # → Advertising & attention
    "Advertising & PR (5418)":          ("CES6054180001", "CES6054180003"),
    # → AI, Cloud, Data Processing & Hosting
    "Data Processing & Hosting (518)":  ("CES5051800001", "CES5051800003"),
    # → Search engines, internet platforms, Subscription content
    "Internet & Info Services (519)":   ("CES5051900001", "CES5051900003"),
    # → Smartphones & OS, Telecom infrastructure
    "Telecommunications (517)":         ("CES5051700001", "CES5051700003"),
    # → Wired infrastructure
    "Wired Telecom (5171)":             ("CES5051710001", "CES5051710003"),
    # → Fintech & payments (broad financial sector)
    "Financial Activities (broad)":     ("CES5500000001", "CES5500000003"),
    # → All information sector aggregate
    "Information Sector (broad)":       ("CES5000000001", "CES5000000003"),
    # → Professional & Business Services aggregate
    "Professional & Business (broad)":  ("CES6000000001", "CES6000000003"),
}

START_YEAR = "2015"
END_YEAR   = "2025"


def fetch_series(series_ids: list[str]) -> dict:
    """Fetch multiple series. Returns {series_id: {year: annual_avg_value}}"""
    payload = {
        "seriesid": series_ids,
        "startyear": START_YEAR,
        "endyear": END_YEAR,
    }
    try:
        r = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json=payload,
            headers=HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        results = {}
        for series in data.get("Results", {}).get("series", []):
            sid = series["seriesID"]
            # Group monthly values by year, then average
            by_year = {}
            for item in series.get("data", []):
                period = item.get("period", "")
                if period.startswith("M") and period != "M13":
                    try:
                        year = int(item["year"])
                        val  = float(item["value"])
                        by_year.setdefault(year, []).append(val)
                    except (ValueError, KeyError):
                        pass
            results[sid] = {yr: sum(vals)/len(vals) for yr, vals in by_year.items()}
        return results
    except Exception as e:
        print(f"  ERROR: {e}")
        return {}


def setup_table(cur):
    cur.executescript("""
        DROP TABLE IF EXISTS bls_employment;
        CREATE TABLE bls_employment (
            sector           TEXT,
            year             INTEGER,
            employees        REAL,      -- thousands, annual avg
            avg_hourly_wage  REAL       -- USD/hr, annual avg
        );
    """)


def main():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    setup_table(cur)
    conn.commit()

    all_series = []
    for emp_sid, wage_sid in SECTORS.values():
        all_series.extend([emp_sid, wage_sid])

    print(f"Fetching BLS data for {len(SECTORS)} sectors ({START_YEAR}-{END_YEAR})...")
    # BLS allows 25 series per request — split into batches
    all_data = {}
    for i in range(0, len(all_series), 24):
        batch = all_series[i:i+24]
        result = fetch_series(batch)
        all_data.update(result)
        time.sleep(1)

    rows = []
    for sector, (emp_sid, wage_sid) in SECTORS.items():
        emp_by_year  = all_data.get(emp_sid, {})
        wage_by_year = all_data.get(wage_sid, {})
        all_years = set(emp_by_year) | set(wage_by_year)
        for year in sorted(all_years):
            rows.append((
                sector, year,
                emp_by_year.get(year),
                wage_by_year.get(year),
            ))

    cur.executemany("INSERT INTO bls_employment VALUES (?, ?, ?, ?)", rows)
    conn.commit()

    print(f"Done! Inserted {len(rows)} rows\n")
    print("--- Preview ---")
    for row in cur.execute("""
        SELECT sector, year, ROUND(employees,1), ROUND(avg_hourly_wage,2)
        FROM bls_employment ORDER BY sector, year LIMIT 30
    """):
        print(f"  {row[0]:<40} {row[1]}  emp={row[2]}k  wage=${row[3]}/hr")

    conn.close()


if __name__ == "__main__":
    main()

"""
SEC EDGAR Enrichment
Fetches Revenue + R&D expense (yearly) for companies in companies_deduped.
Output: sec_financials table in data/companies.db

SEC EDGAR API (free, no key needed):
  - Company search: https://efts.sec.gov/LATEST/search-index?q="name"&forms=10-K
  - Company facts:  https://data.sec.gov/api/xbrl/companyfacts/{CIK}.json
"""

import sqlite3
import time
import requests
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"

HEADERS = {"User-Agent": "SunghunKim sunghunk@umich.edu"}  # SEC requires User-Agent

# Revenue field candidates (try in order)
REVENUE_FIELDS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
]
RD_FIELD = "ResearchAndDevelopmentExpense"
NET_INCOME_FIELDS = [
    "NetIncomeLoss",
    "ProfitLoss",
]


def search_cik(company_name: str) -> tuple[str, str] | None:
    """Returns (cik, matched_name) or None."""
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {"q": f'"{company_name}"', "forms": "10-K"}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
        if hits:
            src = hits[0]["_source"]
            # ciks is a list like ["0000789019"]
            ciks = src.get("ciks", [])
            if not ciks:
                return None
            cik = str(int(ciks[0]))  # remove leading zeros
            display_names = src.get("display_names", [])
            if display_names:
                # format: "MICROSOFT CORP  (MSFT)  (CIK 0000789019)"
                name = display_names[0].split("(")[0].strip()
            else:
                name = company_name
            return cik, name
    except Exception:
        pass
    return None


def search_cik_v2(company_name: str) -> tuple[str, str] | None:
    """Fallback: broader match without quotes."""
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {"q": company_name, "forms": "10-K"}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
        if hits:
            src = hits[0]["_source"]
            ciks = src.get("ciks", [])
            if not ciks:
                return None
            cik = str(int(ciks[0]))
            display_names = src.get("display_names", [])
            name = display_names[0].split("(")[0].strip() if display_names else company_name
            return cik, name
    except Exception:
        pass
    return None


def fetch_company_facts(cik: str) -> dict | None:
    """Fetch all XBRL facts for a company."""
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def extract_annual(facts: dict, field: str) -> list[tuple[int, float]]:
    """Extract annual (10-K) values for a given us-gaap field. Returns [(year, value)]."""
    try:
        units = facts["facts"]["us-gaap"][field]["units"]
        usd_data = units.get("USD", [])
        annual = {}
        for entry in usd_data:
            if entry.get("form") == "10-K" and entry.get("fp") == "FY":
                year = int(entry["end"][:4])
                if 2015 <= year <= 2024:
                    annual[year] = entry["val"]
        return sorted(annual.items())
    except Exception:
        return []


def setup_tables(cur):
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS sec_financials (
            company_id  INTEGER,
            cik         TEXT,
            year        INTEGER,
            revenue     REAL,
            rd_expense  REAL,
            net_income  REAL
        );

        CREATE TABLE IF NOT EXISTS sec_cik_map (
            company_id   INTEGER PRIMARY KEY,
            cik          TEXT,
            matched_name TEXT
        );
    """)


def main():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    setup_tables(cur)
    conn.commit()

    companies = cur.execute(
        "SELECT id, name FROM companies_deduped ORDER BY id"
    ).fetchall()

    # Checkpoint: skip already processed
    done = {r[0] for r in cur.execute("SELECT company_id FROM sec_cik_map")}
    print(f"Total: {len(companies)} | Already done: {len(done)} | Remaining: {len(companies) - len(done)}\n")

    matched = 0
    for i, (company_id, name) in enumerate(companies):
        if company_id in done:
            continue

        # Search CIK
        result = search_cik(name) or search_cik_v2(name)

        if not result:
            if i % 100 == 0:
                print(f"  [{i}/{len(companies)}] {name} — not found")
            time.sleep(0.12)
            continue

        cik, matched_name = result
        cur.execute(
            "INSERT INTO sec_cik_map VALUES (?, ?, ?)",
            (company_id, cik, matched_name)
        )

        # Fetch financials
        facts = fetch_company_facts(cik)
        if not facts:
            time.sleep(0.12)
            continue

        # Revenue
        revenue_by_year = {}
        for field in REVENUE_FIELDS:
            data = extract_annual(facts, field)
            if data:
                for year, val in data:
                    revenue_by_year[year] = val
                break

        # R&D
        rd_by_year = dict(extract_annual(facts, RD_FIELD))

        # Net Income
        net_income_by_year = {}
        for field in NET_INCOME_FIELDS:
            data = extract_annual(facts, field)
            if data:
                for year, val in data:
                    net_income_by_year[year] = val
                break

        # Merge years
        all_years = set(revenue_by_year) | set(rd_by_year) | set(net_income_by_year)
        for year in sorted(all_years):
            cur.execute(
                "INSERT INTO sec_financials VALUES (?, ?, ?, ?, ?, ?)",
                (company_id, cik, year,
                 revenue_by_year.get(year),
                 rd_by_year.get(year),
                 net_income_by_year.get(year))
            )

        matched += 1
        print(f"  [{i}/{len(companies)}] {name} → CIK {cik} ({len(all_years)} years)")

        conn.commit()
        time.sleep(0.12)  # SEC rate limit: ~10 req/sec

    print(f"\nDone! Matched {matched}/{len(companies)} companies with SEC data")
    conn.close()


if __name__ == "__main__":
    main()

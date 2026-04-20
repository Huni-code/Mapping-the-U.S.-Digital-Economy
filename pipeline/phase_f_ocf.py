"""
Phase F: Enrich sec_financials with operating_cash_flow from SEC XBRL Company Facts.

Source:
  https://data.sec.gov/api/xbrl/companyfacts/CIK{10-digit}.json

Tag fallback order:
  1. NetCashProvidedByUsedInOperatingActivities     (post-2016 GAAP)
  2. NetCashProvidedByOperatingActivities           (pre-2016 GAAP)
  3. NetCashProvidedByUsedInOperatingActivitiesContinuingOperations

Annual filter:
  - us-gaap namespace, USD unit
  - form='10-K', fp='FY'
  - duplicate (cik, year): pick latest `filed` date

Upsert:
  - If sec_financials row exists for (company_id, year) -> UPDATE operating_cash_flow
  - If not -> INSERT with revenue/rd_expense/net_income = NULL (OCF trend still useful)

Outputs:
  - ALTER TABLE sec_financials ADD COLUMN operating_cash_flow REAL
  - data/ocf_cache.json (CIK -> {year: ocf})
  - data/phase_f_ocf_errors.csv (fetch/parse failures)
"""

import csv
import json
import sqlite3
import time
from pathlib import Path
import requests

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
CACHE_FILE = Path(__file__).parent.parent / "data" / "ocf_cache.json"
ERRORS_CSV = Path(__file__).parent.parent / "data" / "phase_f_ocf_errors.csv"

HEADERS = {"User-Agent": "SeniorProject sunghun.kim@calvin.edu"}

OCF_TAGS = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
]


def ensure_schema(cur):
    cols = [r[1] for r in cur.execute("PRAGMA table_info(sec_financials)").fetchall()]
    if "operating_cash_flow" not in cols:
        cur.execute("ALTER TABLE sec_financials ADD COLUMN operating_cash_flow REAL")
        print("  ALTER TABLE: added operating_cash_flow column")


def load_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f)


def fetch_companyfacts(cik: str) -> dict | None:
    cik_10 = cik.zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_10}.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        print(f"    fetch error {cik}: {e}")
        return None


def extract_ocf(facts: dict) -> dict[int, float]:
    """Return {year: ocf_usd} annual from first available tag."""
    usgaap = facts.get("facts", {}).get("us-gaap", {})
    for tag in OCF_TAGS:
        node = usgaap.get(tag)
        if not node:
            continue
        units = node.get("units", {}).get("USD")
        if not units:
            continue
        year_to_entry = {}
        for entry in units:
            if entry.get("form") != "10-K" or entry.get("fp") != "FY":
                continue
            year = entry.get("fy")
            if not isinstance(year, int):
                continue
            val = entry.get("val")
            filed = entry.get("filed", "")
            prev = year_to_entry.get(year)
            if prev is None or filed > prev[1]:
                year_to_entry[year] = (val, filed)
        if year_to_entry:
            return {y: v[0] for y, v in year_to_entry.items()}
    return {}


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    ensure_schema(cur)

    rows = cur.execute("""
        SELECT scm.company_id, cd.name, scm.cik
        FROM sec_cik_map scm
        JOIN companies_deduped cd ON cd.id = scm.company_id
        ORDER BY scm.company_id
    """).fetchall()
    print(f"Fetching OCF for {len(rows)} companies...")

    cache = load_cache()
    errors = []
    inserted = updated = 0
    companies_with_ocf = 0
    fetch_count = 0

    for i, (company_id, name, cik) in enumerate(rows, 1):
        if cik in cache:
            ocf_by_year = {int(y): v for y, v in cache[cik].items()}
        else:
            facts = fetch_companyfacts(cik)
            fetch_count += 1
            if facts is None:
                errors.append((company_id, name, cik, "fetch_failed"))
                cache[cik] = {}
                time.sleep(0.11)
                if fetch_count % 25 == 0:
                    save_cache(cache)
                    print(f"  progress: {i}/{len(rows)} (fetched {fetch_count})")
                continue
            ocf_by_year = extract_ocf(facts)
            cache[cik] = {str(y): v for y, v in ocf_by_year.items()}
            time.sleep(0.11)
            if fetch_count % 25 == 0:
                save_cache(cache)
                print(f"  progress: {i}/{len(rows)} (fetched {fetch_count})")

        if not ocf_by_year:
            errors.append((company_id, name, cik, "no_ocf_tag"))
            continue

        companies_with_ocf += 1
        existing_years = {
            r[0] for r in cur.execute(
                "SELECT year FROM sec_financials WHERE company_id = ?", (company_id,)
            )
        }
        for year, ocf in ocf_by_year.items():
            if year in existing_years:
                cur.execute(
                    "UPDATE sec_financials SET operating_cash_flow = ? WHERE company_id = ? AND year = ?",
                    (ocf, company_id, year),
                )
                updated += 1
            else:
                cur.execute(
                    """INSERT INTO sec_financials
                       (company_id, cik, year, revenue, rd_expense, net_income, operating_cash_flow)
                       VALUES (?, ?, ?, NULL, NULL, NULL, ?)""",
                    (company_id, cik, year, ocf),
                )
                inserted += 1

    conn.commit()
    save_cache(cache)

    with open(ERRORS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "company_name", "cik", "error"])
        w.writerows(errors)

    total_fin = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]
    with_ocf = cur.execute(
        "SELECT COUNT(*) FROM sec_financials WHERE operating_cash_flow IS NOT NULL"
    ).fetchone()[0]

    print()
    print("=== Phase F: OCF Enrichment ===")
    print(f"Companies processed:         {len(rows)}")
    print(f"Companies with OCF data:     {companies_with_ocf}")
    print(f"Companies with errors:       {len(errors)}")
    print(f"sec_financials rows updated: {updated}")
    print(f"sec_financials rows inserted:{inserted}")
    print(f"sec_financials total:        {total_fin} ({with_ocf} with OCF)")

    print("\nSector-level OCF coverage (companies with >=1 OCF year):")
    for sector, total, covered in cur.execute("""
        SELECT cc.sector,
               COUNT(DISTINCT scm.company_id),
               COUNT(DISTINCT CASE WHEN sf.operating_cash_flow IS NOT NULL
                                    THEN scm.company_id END)
        FROM sec_cik_map scm
        JOIN company_classifications cc ON cc.company_id = scm.company_id
        LEFT JOIN sec_financials sf ON sf.company_id = scm.company_id
        GROUP BY cc.sector
        ORDER BY cc.sector
    """).fetchall():
        flag = "  (weak)" if covered < 5 else ""
        print(f"  {sector:<32}  {covered:>3}/{total:<3}{flag}")

    conn.close()


if __name__ == "__main__":
    main()

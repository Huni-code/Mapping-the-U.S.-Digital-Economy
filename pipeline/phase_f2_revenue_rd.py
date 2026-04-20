"""
Phase F2: Enrich sec_financials with revenue + R&D from SEC XBRL Company Facts.

Fills gaps left by the original enrich_sec.py (broken full-text-search matcher)
and covers Phase B/E additions (Adobe, Coinbase, Elastic, etc.) that have OCF
from Phase F but no revenue/R&D.

Tag fallback order:
  Revenue:
    1. Revenues
    2. RevenueFromContractWithCustomerExcludingAssessedTax
    3. SalesRevenueNet
  R&D:
    1. ResearchAndDevelopmentExpense
    2. ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost

Annual filter: us-gaap / USD / form='10-K' / fp='FY'. Duplicate (cik, year): pick
latest `filed`.

Upsert policy:
  - NULL-only fill. Existing non-NULL values are left untouched so revenue/R&D
    stay consistent with the tag originally used for that year.
  - If no sec_financials row exists for (company_id, year) -> INSERT with whatever
    metrics we have (OCF may also be NULL for years outside Phase F coverage).

Outputs:
  data/revenue_rd_cache.json          (CIK -> {year: [rev, rd]})
  data/phase_f2_enrichment_log.csv    (per-company action summary)
"""

import csv
import json
import sqlite3
import time
from pathlib import Path
import requests

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
CACHE_FILE = Path(__file__).parent.parent / "data" / "revenue_rd_cache.json"
LOG_CSV = Path(__file__).parent.parent / "data" / "phase_f2_enrichment_log.csv"

HEADERS = {"User-Agent": "SeniorProject sunghun.kim@calvin.edu"}

REVENUE_TAGS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
]
RD_TAGS = [
    "ResearchAndDevelopmentExpense",
    "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
]


def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
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


def extract_annual(facts: dict, tags: list[str]) -> dict[int, float]:
    usgaap = facts.get("facts", {}).get("us-gaap", {})
    for tag in tags:
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

    rows = cur.execute("""
        SELECT scm.company_id, cd.name, scm.cik
        FROM sec_cik_map scm
        JOIN companies_deduped cd ON cd.id = scm.company_id
        ORDER BY scm.company_id
    """).fetchall()
    print(f"Enriching revenue + R&D for {len(rows)} companies...")

    cache = load_cache()
    log = []
    rev_updated = rd_updated = rows_inserted = 0
    companies_touched = 0
    fetch_count = 0

    for i, (company_id, name, cik) in enumerate(rows, 1):
        if cik in cache:
            years = {int(y): tuple(v) for y, v in cache[cik].items()}
        else:
            facts = fetch_companyfacts(cik)
            fetch_count += 1
            if facts is None:
                cache[cik] = {}
                log.append((company_id, name, cik, 0, 0, 0, "fetch_failed"))
                time.sleep(0.11)
                if fetch_count % 25 == 0:
                    save_cache(cache)
                    print(f"  progress: {i}/{len(rows)} (fetched {fetch_count})")
                continue
            rev = extract_annual(facts, REVENUE_TAGS)
            rd = extract_annual(facts, RD_TAGS)
            all_years = set(rev) | set(rd)
            years = {y: (rev.get(y), rd.get(y)) for y in all_years}
            cache[cik] = {str(y): [v[0], v[1]] for y, v in years.items()}
            time.sleep(0.11)
            if fetch_count % 25 == 0:
                save_cache(cache)
                print(f"  progress: {i}/{len(rows)} (fetched {fetch_count})")

        if not years:
            log.append((company_id, name, cik, 0, 0, 0, "no_tags"))
            continue

        c_rev_upd = c_rd_upd = c_inserted = 0
        for year, (rev, rd) in years.items():
            existing = cur.execute(
                "SELECT revenue, rd_expense FROM sec_financials WHERE company_id = ? AND year = ?",
                (company_id, year),
            ).fetchone()
            if existing is None:
                cur.execute(
                    """INSERT INTO sec_financials
                       (company_id, cik, year, revenue, rd_expense, net_income, operating_cash_flow)
                       VALUES (?, ?, ?, ?, ?, NULL, NULL)""",
                    (company_id, cik, year, rev, rd),
                )
                c_inserted += 1
            else:
                cur_rev, cur_rd = existing
                updates, params = [], []
                if cur_rev is None and rev is not None:
                    updates.append("revenue = ?")
                    params.append(rev)
                    c_rev_upd += 1
                if cur_rd is None and rd is not None:
                    updates.append("rd_expense = ?")
                    params.append(rd)
                    c_rd_upd += 1
                if updates:
                    params.extend([company_id, year])
                    cur.execute(
                        f"UPDATE sec_financials SET {', '.join(updates)} "
                        f"WHERE company_id = ? AND year = ?",
                        params,
                    )

        if c_rev_upd or c_rd_upd or c_inserted:
            companies_touched += 1
        rev_updated += c_rev_upd
        rd_updated += c_rd_upd
        rows_inserted += c_inserted
        log.append((company_id, name, cik, c_rev_upd, c_rd_upd, c_inserted, "ok"))

    conn.commit()
    save_cache(cache)

    with open(LOG_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "name", "cik", "rev_updated", "rd_updated",
                    "rows_inserted", "status"])
        w.writerows(log)

    total = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]
    with_rev = cur.execute(
        "SELECT COUNT(*) FROM sec_financials WHERE revenue IS NOT NULL"
    ).fetchone()[0]
    with_rd = cur.execute(
        "SELECT COUNT(*) FROM sec_financials WHERE rd_expense IS NOT NULL"
    ).fetchone()[0]
    with_ocf = cur.execute(
        "SELECT COUNT(*) FROM sec_financials WHERE operating_cash_flow IS NOT NULL"
    ).fetchone()[0]

    print()
    print("=== Phase F2: Revenue + R&D Enrichment ===")
    print(f"Companies processed:        {len(rows)}")
    print(f"Companies touched:          {companies_touched}")
    print(f"Revenue NULLs filled:       {rev_updated}")
    print(f"R&D NULLs filled:           {rd_updated}")
    print(f"New sec_financials rows:    {rows_inserted}")
    print(f"sec_financials total:       {total}")
    print(f"  with revenue:             {with_rev}")
    print(f"  with rd_expense:          {with_rd}")
    print(f"  with operating_cash_flow: {with_ocf}")

    coverage = cur.execute("""
        SELECT cc.sector,
               COUNT(DISTINCT scm.company_id) AS total,
               COUNT(DISTINCT CASE WHEN sf.revenue IS NOT NULL THEN scm.company_id END) AS with_rev,
               COUNT(DISTINCT CASE WHEN sf.rd_expense IS NOT NULL THEN scm.company_id END) AS with_rd,
               COUNT(DISTINCT CASE WHEN sf.operating_cash_flow IS NOT NULL THEN scm.company_id END) AS with_ocf
        FROM sec_cik_map scm
        JOIN company_classifications cc ON cc.company_id = scm.company_id
        LEFT JOIN sec_financials sf ON sf.company_id = scm.company_id
            AND sf.year BETWEEN 2020 AND 2024
        GROUP BY cc.sector
        ORDER BY cc.sector
    """).fetchall()

    print("\nPer-sector coverage (2020-2024 window):")
    print(f"  {'Sector':<32} {'tot':>4} {'rev':>4} {'rd':>4} {'ocf':>4}")
    for sector, tot, rev, rd, ocf in coverage:
        print(f"  {sector:<32} {tot:>4} {rev:>4} {rd:>4} {ocf:>4}")

    conn.close()


if __name__ == "__main__":
    main()

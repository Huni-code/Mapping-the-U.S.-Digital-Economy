"""
Phase C: Sanity checks on surviving sec_cik_map entries.

Checks:
  1. Revenue scale: Startup/Small company_size but sec_financials max revenue > $1B -> DROP
  2. SIC code: CIK's SIC not in tech set, for tech-relevant sectors only -> FLAG (no drop)
  3. Duplicate CIK: same CIK -> multiple company_id -> keep highest Jaccard (tie -> lowest company_id)

Outputs:
  data/phase_c_flagged.csv (all check results)
  DB: cascade DELETE on Revenue scale drops + duplicate drops
"""

import csv
import re
import sqlite3
import time
from pathlib import Path
import requests

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
OUT_CSV = Path(__file__).parent.parent / "data" / "phase_c_flagged.csv"
SIC_CACHE = Path(__file__).parent.parent / "data" / "sic_cache.json"

HEADERS = {"User-Agent": "SeniorProject sunghun.kim@calvin.edu"}

STOP_WORDS = {
    "inc", "corp", "corporation", "llc", "ltd", "limited", "co",
    "ai", "the", "and", "of",
    "technologies", "technology", "solutions", "services",
    "group", "holdings", "international",
}

TECH_SIC_RANGES = [(7370, 7379), (7380, 7389)]
TECH_SIC_EXACT = {5045, 3674, 3672, 5112, 4813}

TECH_SCOPED_SECTORS = {
    "AI foundation models",
    "AI assistants & copilots",
    "Developer tooling",
    "Productivity & collaboration",
    "Search engines",
    "Creative & design tools",
    "Gaming & virtual environments",
    "Subscription content",
    "Enterprise / ERP / HRM",
    "E-commerce platforms",
}


def normalize(name: str) -> set[str]:
    if not name:
        return set()
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return {t for t in s.split() if t and t not in STOP_WORDS and len(t) > 1}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def is_tech_sic(sic: int | None) -> bool:
    if sic is None:
        return False
    for lo, hi in TECH_SIC_RANGES:
        if lo <= sic <= hi:
            return True
    return sic in TECH_SIC_EXACT


def fetch_sic(cik: str, cache: dict) -> int | None:
    if cik in cache:
        return cache[cik]
    cik_10 = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_10}.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            cache[cik] = None
            return None
        data = r.json()
        sic_str = data.get("sic")
        sic = int(sic_str) if sic_str else None
        cache[cik] = sic
        return sic
    except Exception as e:
        print(f"    SIC fetch error {cik}: {e}")
        cache[cik] = None
        return None


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    before_cik = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]
    before_fin = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]

    rows = cur.execute("""
        SELECT scm.company_id, cd.name, scm.cik, scm.matched_name,
               cd.company_size, cc.sector,
               (SELECT MAX(revenue) FROM sec_financials WHERE company_id = cd.id) AS max_rev
        FROM sec_cik_map scm
        JOIN companies_deduped cd ON cd.id = scm.company_id
        LEFT JOIN company_classifications cc ON cc.company_id = cd.id
    """).fetchall()
    print(f"Sanity checking {len(rows)} entries...")

    flagged = []
    to_drop_ids = set()

    # Check 1: Revenue scale
    for company_id, name, cik, matched, size, sector, max_rev in rows:
        if size in ("Startup", "Small") and max_rev and max_rev > 1e9:
            flagged.append((company_id, name, cik, "revenue_scale",
                           f"{size} but max_rev=${max_rev/1e9:.2f}B"))
            to_drop_ids.add(company_id)

    # Check 3: Duplicate CIK (before SIC check to reduce API calls)
    cik_to_entries = {}
    for company_id, name, cik, matched, size, sector, max_rev in rows:
        cik_to_entries.setdefault(cik, []).append((company_id, name, matched))

    for cik, entries in cik_to_entries.items():
        if len(entries) <= 1:
            continue
        scored = []
        for company_id, name, matched in entries:
            j = jaccard(normalize(name), normalize(matched))
            scored.append((j, company_id, name, matched))
        scored.sort(key=lambda x: (-x[0], x[1]))
        winner_id = scored[0][1]
        for j, company_id, name, matched in scored[1:]:
            flagged.append((company_id, name, cik, "duplicate_cik",
                           f"cik shared with winner company_id={winner_id} (jaccard={j:.2f})"))
            to_drop_ids.add(company_id)

    # Check 2: SIC code for tech-scoped sectors only
    print(f"Fetching SIC for tech-scoped entries (rate-limited 10 req/sec)...")
    import json
    sic_cache = {}
    if SIC_CACHE.exists():
        with open(SIC_CACHE, encoding="utf-8") as f:
            sic_cache = json.load(f)

    sic_check_count = 0
    for company_id, name, cik, matched, size, sector, max_rev in rows:
        if company_id in to_drop_ids:
            continue
        if sector not in TECH_SCOPED_SECTORS:
            continue
        sic = fetch_sic(cik, sic_cache)
        sic_check_count += 1
        if sic_check_count % 25 == 0:
            print(f"  SIC checked: {sic_check_count}")
            with open(SIC_CACHE, "w", encoding="utf-8") as f:
                json.dump(sic_cache, f)
        if sic is not None and not is_tech_sic(sic):
            flagged.append((company_id, name, cik, "non_tech_sic",
                           f"sector='{sector}' but SIC={sic}"))
        time.sleep(0.11)

    with open(SIC_CACHE, "w", encoding="utf-8") as f:
        json.dump(sic_cache, f)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "company_name", "cik", "check_type", "detail"])
        w.writerows(flagged)

    if to_drop_ids:
        ids = list(to_drop_ids)
        placeholders = ",".join("?" * len(ids))
        cur.execute(f"DELETE FROM sec_financials WHERE company_id IN ({placeholders})", ids)
        fin_deleted = cur.rowcount
        cur.execute(f"DELETE FROM sec_cik_map WHERE company_id IN ({placeholders})", ids)
        cik_deleted = cur.rowcount
    else:
        fin_deleted = cik_deleted = 0

    conn.commit()

    after_cik = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]
    after_fin = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]

    check_counts = {}
    for f in flagged:
        check_counts[f[3]] = check_counts.get(f[3], 0) + 1

    print("\n=== Phase C Sanity Check ===")
    print(f"sec_cik_map:    {before_cik} -> {after_cik} (dropped {cik_deleted})")
    print(f"sec_financials: {before_fin} -> {after_fin} (dropped {fin_deleted})")
    print("\nFlags raised:")
    for t, c in sorted(check_counts.items(), key=lambda x: -x[1]):
        note = "(dropped)" if t in ("revenue_scale", "duplicate_cik") else "(flagged only)"
        print(f"  {t:<20} {c:>4} {note}")
    print(f"\nOutput: {OUT_CSV.name}")

    conn.close()


if __name__ == "__main__":
    main()

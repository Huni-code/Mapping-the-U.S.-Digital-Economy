"""
Phase A: Clean corrupted CIK matches in sec_cik_map + cascade delete sec_financials.

Drop conditions (OR):
  1. Zero token overlap between company_name and matched_name
  2. Mega-cap hijack: matched_name contains {amazon, alphabet, google, tesla, microsoft,
     meta, apple, nvidia, berkshire, jpmorgan, walmart} AND company_name does not
  3. Jaccard < 0.5

Outputs:
  data/phase_a_dropped.csv, data/phase_a_kept.csv
  + cascade DELETE on sec_cik_map, sec_financials
"""

import csv
import re
import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
DROP_CSV = Path(__file__).parent.parent / "data" / "phase_a_dropped.csv"
KEEP_CSV = Path(__file__).parent.parent / "data" / "phase_a_kept.csv"

STOP_WORDS = {
    "inc", "corp", "corporation", "llc", "ltd", "limited", "co",
    "ai", "the", "and", "of",
    "technologies", "technology", "solutions", "services",
    "group", "holdings", "international",
}

MEGA_CAPS = {
    "amazon", "alphabet", "google", "tesla", "microsoft",
    "meta", "apple", "nvidia", "berkshire", "jpmorgan", "walmart",
}


def normalize(name: str) -> set[str]:
    if not name:
        return set()
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    tokens = {t for t in s.split() if t and t not in STOP_WORDS and len(t) > 1}
    return tokens


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def drop_reason(company_name: str, matched_name: str) -> str | None:
    a = normalize(company_name)
    b = normalize(matched_name)

    if not (a & b):
        return "zero_overlap"

    matched_lower = (matched_name or "").lower()
    company_lower = (company_name or "").lower()
    for mega in MEGA_CAPS:
        if mega in matched_lower and mega not in company_lower:
            return f"mega_cap_hijack:{mega}"

    if jaccard(a, b) < 0.5:
        return "low_jaccard"

    return None


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT scm.company_id, cd.name, scm.cik, scm.matched_name
        FROM sec_cik_map scm
        JOIN companies_deduped cd ON cd.id = scm.company_id
    """).fetchall()

    before_cik = len(rows)
    before_fin = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]

    dropped = []
    kept = []
    for company_id, name, cik, matched in rows:
        reason = drop_reason(name, matched)
        if reason:
            dropped.append((company_id, name, cik, matched, reason))
        else:
            kept.append((company_id, name, cik, matched))

    with open(DROP_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "company_name", "cik", "matched_name", "reason"])
        w.writerows(dropped)

    with open(KEEP_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "company_name", "cik", "matched_name"])
        w.writerows(kept)

    drop_ids = [d[0] for d in dropped]
    if drop_ids:
        placeholders = ",".join("?" * len(drop_ids))
        cur.execute(f"DELETE FROM sec_financials WHERE company_id IN ({placeholders})", drop_ids)
        fin_deleted = cur.rowcount
        cur.execute(f"DELETE FROM sec_cik_map WHERE company_id IN ({placeholders})", drop_ids)
        cik_deleted = cur.rowcount
    else:
        fin_deleted = cik_deleted = 0

    conn.commit()

    after_cik = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]
    after_fin = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]

    reason_counts = {}
    for d in dropped:
        r = d[4].split(":")[0]
        reason_counts[r] = reason_counts.get(r, 0) + 1

    print("=== Phase A Cleanup ===")
    print(f"sec_cik_map:    {before_cik} -> {after_cik} (dropped {cik_deleted})")
    print(f"sec_financials: {before_fin} -> {after_fin} (dropped {fin_deleted})")
    print(f"Drop rate:      {cik_deleted / before_cik * 100:.1f}%")
    print("\nDrop reasons:")
    for r, c in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"  {r:<25} {c}")
    print(f"\nOutputs: {DROP_CSV.name}, {KEEP_CSV.name}")

    conn.close()


if __name__ == "__main__":
    main()

"""
Phase D: Impact report comparing pre-cleanup (backup DB) vs post-cleanup (current DB).

Output:
  Console report with global + per-sector survival counts, flagging sectors
  with fewer than 5 survivors (weak statistical power for downstream analysis).
"""

import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
BACKUP_DB = Path(__file__).parent.parent / "data" / "companies_backup_20260419.db"

WEAK_THRESHOLD = 5


def sector_counts(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT cc.sector, COUNT(DISTINCT scm.company_id)
        FROM sec_cik_map scm
        JOIN company_classifications cc ON cc.company_id = scm.company_id
        GROUP BY cc.sector
    """).fetchall()
    conn.close()
    return dict(rows)


def global_counts(db_path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cik = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]
    fin = cur.execute("SELECT COUNT(*) FROM sec_financials").fetchone()[0]
    conn.close()
    return cik, fin


def main():
    before_cik, before_fin = global_counts(BACKUP_DB)
    after_cik, after_fin = global_counts(DB_FILE)

    before_sectors = sector_counts(BACKUP_DB)
    after_sectors = sector_counts(DB_FILE)

    all_sectors = sorted(set(before_sectors) | set(after_sectors))

    print("=== CIK Cleanup Impact Report ===")
    print(f"Before: {before_cik:,} CIK mappings / {before_fin:,} financial records")
    print(f"After:  {after_cik:,} CIK mappings / {after_fin:,} financial records")
    drop_rate = (before_cik - after_cik) / before_cik * 100
    print(f"Drop rate: {drop_rate:.1f}%")
    print()
    print("Sector survival:")
    max_name = max(len(s) for s in all_sectors)
    weak = []
    for sector in all_sectors:
        b = before_sectors.get(sector, 0)
        a = after_sectors.get(sector, 0)
        pct = (a / b * 100) if b else 0.0
        print(f"  {sector:<{max_name}}  {b:>4} -> {a:>4} ({pct:>5.1f}%)")
        if a < WEAK_THRESHOLD:
            weak.append((sector, a))

    print()
    if weak:
        print(f"Sectors with < {WEAK_THRESHOLD} surviving companies (weak statistical power):")
        for sector, a in weak:
            print(f"  {sector:<{max_name}}  n={a}")
    else:
        print(f"All sectors have >= {WEAK_THRESHOLD} surviving companies.")


if __name__ == "__main__":
    main()

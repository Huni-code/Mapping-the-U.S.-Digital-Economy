"""
Phase E: Manual patches for bellwether brand/legal-name mismatches.

Identified by pipeline/bellwether_check.py over a 120-company canonical list:
  - Coinbase (id=62): unmatched; SEC title 'Coinbase Global, Inc.' fails token-set
    match because STOP_WORDS doesn't strip 'global'.
  - Elastic (id=658): wrongly matched to defunct ELASTIC NETWORKS INC (Jaccard
    exactly 0.5, passed Phase A threshold). Real entity is Elastic N.V. (ESTC).

For Elastic, delete existing sec_financials tied to the wrong CIK before the
UPDATE so downstream queries don't surface defunct-CIK data.
"""

import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"

PATCHES = [
    {
        "company_id": 62,
        "name": "Coinbase",
        "action": "INSERT",
        "cik": "1679788",
        "matched_name": "Coinbase Global, Inc.",
    },
    {
        "company_id": 658,
        "name": "Elastic",
        "action": "UPDATE",
        "cik": "1707753",
        "matched_name": "Elastic N.V.",
    },
]


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    before = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]

    for p in PATCHES:
        current = cur.execute(
            "SELECT cik, matched_name FROM sec_cik_map WHERE company_id = ?",
            (p["company_id"],),
        ).fetchone()

        if p["action"] == "INSERT":
            if current:
                print(f"  [{p['name']}] already matched ({current}), skipping INSERT")
                continue
            cur.execute(
                "INSERT INTO sec_cik_map (company_id, cik, matched_name) VALUES (?, ?, ?)",
                (p["company_id"], p["cik"], p["matched_name"]),
            )
            print(f"  [{p['name']}] INSERT id={p['company_id']} cik={p['cik']} -> {p['matched_name']}")

        elif p["action"] == "UPDATE":
            if not current:
                print(f"  [{p['name']}] no existing row to update, skipping")
                continue
            cur.execute(
                "DELETE FROM sec_financials WHERE company_id = ?",
                (p["company_id"],),
            )
            fin_cleared = cur.rowcount
            cur.execute(
                "UPDATE sec_cik_map SET cik = ?, matched_name = ? WHERE company_id = ?",
                (p["cik"], p["matched_name"], p["company_id"]),
            )
            print(f"  [{p['name']}] UPDATE id={p['company_id']} "
                  f"old=({current[0]}, {current[1]}) -> new=({p['cik']}, {p['matched_name']}), "
                  f"cleared {fin_cleared} stale financial rows")

    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]
    print(f"\nsec_cik_map: {before} -> {after}")
    conn.close()


if __name__ == "__main__":
    main()

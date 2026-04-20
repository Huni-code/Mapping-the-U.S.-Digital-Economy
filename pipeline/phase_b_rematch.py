"""
Phase B: Strict re-matching using SEC company_tickers.json (public filers only).

Input:
  companies_deduped rows that are NOT currently in sec_cik_map
  (includes Phase A drops + those never attempted)

Strategy (precision > recall):
  1. Exact match: normalized name == normalized ticker title
  2. Token set match: normalized token sets identical
  3. Neither -> drop (no fuzzy fallback)

Outputs:
  data/company_tickers.json (cached download)
  data/phase_b_new_matches.csv
  + INSERT into sec_cik_map
"""

import csv
import json
import re
import sqlite3
from pathlib import Path
import requests

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
TICKERS_CACHE = Path(__file__).parent.parent / "data" / "company_tickers.json"
OUT_CSV = Path(__file__).parent.parent / "data" / "phase_b_new_matches.csv"

HEADERS = {"User-Agent": "SeniorProject sunghun.kim@calvin.edu"}
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

STOP_WORDS = {
    "inc", "corp", "corporation", "llc", "ltd", "limited", "co",
    "ai", "the", "and", "of",
    "technologies", "technology", "solutions", "services",
    "group", "holdings", "international",
}


def normalize_tokens(name: str) -> tuple[str, frozenset[str]]:
    if not name:
        return "", frozenset()
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    tokens = [t for t in s.split() if t and t not in STOP_WORDS and len(t) > 1]
    return " ".join(tokens), frozenset(tokens)


def fetch_tickers() -> dict:
    if TICKERS_CACHE.exists():
        with open(TICKERS_CACHE, encoding="utf-8") as f:
            return json.load(f)
    print(f"Fetching {TICKERS_URL} ...")
    r = requests.get(TICKERS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    with open(TICKERS_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print(f"Cached {len(data)} entries -> {TICKERS_CACHE.name}")
    return data


def build_index(tickers: dict) -> tuple[dict, dict]:
    by_exact = {}
    by_tokenset = {}
    for entry in tickers.values():
        cik = str(int(entry["cik_str"]))
        title = entry["title"]
        norm_str, norm_set = normalize_tokens(title)
        if norm_str:
            by_exact.setdefault(norm_str, (cik, title))
        if norm_set:
            by_tokenset.setdefault(norm_set, (cik, title))
    return by_exact, by_tokenset


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    unmatched = cur.execute("""
        SELECT cd.id, cd.name
        FROM companies_deduped cd
        LEFT JOIN sec_cik_map scm ON scm.company_id = cd.id
        WHERE scm.company_id IS NULL
    """).fetchall()
    print(f"Unmatched companies to attempt: {len(unmatched)}")

    tickers = fetch_tickers()
    by_exact, by_tokenset = build_index(tickers)
    print(f"Ticker index: {len(by_exact)} exact / {len(by_tokenset)} tokensets\n")

    new_matches = []
    stats = {"exact": 0, "tokenset": 0, "no_match": 0}

    for company_id, name in unmatched:
        norm_str, norm_set = normalize_tokens(name)
        if not norm_set:
            stats["no_match"] += 1
            continue

        hit = by_exact.get(norm_str)
        method = "exact" if hit else None
        if not hit:
            hit = by_tokenset.get(norm_set)
            method = "tokenset" if hit else None

        if hit:
            cik, title = hit
            new_matches.append((company_id, name, cik, title, method))
            stats[method] += 1
        else:
            stats["no_match"] += 1

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_id", "company_name", "cik", "matched_title", "match_method"])
        w.writerows(new_matches)

    for company_id, _, cik, title, _ in new_matches:
        cur.execute(
            "INSERT OR REPLACE INTO sec_cik_map (company_id, cik, matched_name) VALUES (?, ?, ?)",
            (company_id, cik, title),
        )
    conn.commit()

    after_cik = cur.execute("SELECT COUNT(*) FROM sec_cik_map").fetchone()[0]

    print("=== Phase B Rematch ===")
    print(f"Attempted:       {len(unmatched)}")
    print(f"Exact match:     {stats['exact']}")
    print(f"Token set match: {stats['tokenset']}")
    print(f"No match:        {stats['no_match']}")
    print(f"\nsec_cik_map: now {after_cik} entries (+{len(new_matches)})")
    print(f"Output: {OUT_CSV.name}")

    conn.close()


if __name__ == "__main__":
    main()

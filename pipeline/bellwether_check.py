"""
Bellwether check: per-sector list of canonical public tech companies.
For each, report:
  - in builtin source (companies_deduped)?
  - currently matched in sec_cik_map?
  - if dropped in Phase A, what was the wrong match?
  - if simply unmatched, which CIK should it be?
"""

import csv
import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
PHASE_A_DROPPED = Path(__file__).parent.parent / "data" / "phase_a_dropped.csv"

BELLWETHERS = {
    "Enterprise / ERP / HRM": ["Salesforce", "Workday", "ServiceNow", "SAP", "Oracle",
                                "Adobe", "IBM", "Dell", "Accenture", "Atlassian"],
    "Fintech & payments": ["PayPal", "Block", "Square", "Coinbase", "SoFi", "Affirm",
                            "Robinhood", "Visa", "Mastercard", "Chime"],
    "E-commerce platforms": ["Shopify", "Etsy", "eBay", "Chewy", "Wayfair", "GoDaddy",
                              "Squarespace", "Opendoor"],
    "Cybersecurity & identity": ["CrowdStrike", "Palo Alto Networks", "Palo Alto",
                                  "Fortinet", "Okta", "Zscaler", "SentinelOne", "Rubrik",
                                  "Cloudflare"],
    "Developer tooling": ["GitLab", "Datadog", "Twilio", "Elastic", "HashiCorp",
                           "Snowflake", "MongoDB", "Confluent", "DigitalOcean",
                           "CoreWeave", "Cloudflare"],
    "Advertising & attention": ["The Trade Desk", "Trade Desk", "AppLovin", "DoubleVerify",
                                 "Criteo", "Magnite", "Digital Turbine", "Zeta Global",
                                 "Pinterest", "Snap"],
    "Productivity & collaboration": ["Zoom", "Slack", "Asana", "Dropbox", "Box",
                                      "monday.com", "HubSpot", "Smartsheet", "Atlassian"],
    "AI assistants & copilots": ["SoundHound", "C3.ai", "BigBear.ai", "C3", "BigBear"],
    "AI foundation models": ["NVIDIA", "Palantir", "Tempus", "SoundHound"],
    "Marketplaces & gig platforms": ["Airbnb", "Uber", "DoorDash", "Lyft", "Match Group",
                                      "Grab", "Upwork", "Fiverr", "CarGurus", "Expedia"],
    "Subscription content": ["Netflix", "Spotify", "Disney", "Warner Bros Discovery",
                              "Paramount", "Sirius XM", "Roku", "Comcast"],
    "Gaming & virtual environments": ["Electronic Arts", "Take-Two", "Roblox", "Unity",
                                       "Zynga", "EA"],
    "Creative & design tools": ["Adobe", "Autodesk", "Figma"],
    "Smartphones & OS": ["Apple", "Samsung"],
    "Search engines": ["Alphabet", "Google", "Microsoft"],
    "GovTech / RegTech / MedTech": ["Palantir", "Tyler Technologies", "Veeva", "Doximity",
                                     "Teladoc", "Hims", "Waystar"],
    "E-learning & skill platforms": ["Coursera", "Duolingo", "Chegg", "Udemy", "2U"],
}


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    dropped_by_name = {}
    with open(PHASE_A_DROPPED, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dropped_by_name.setdefault(row["company_name"].lower(), []).append(row)

    print(f"{'SECTOR':<32} {'BELLWETHER':<24} {'STATUS'}")
    print("=" * 100)

    stats = {"matched": 0, "missing_from_builtin": 0, "unmatched_in_builtin": 0,
             "dropped_in_phase_a": 0}

    for sector, names in BELLWETHERS.items():
        for name in names:
            rows = cur.execute(
                """SELECT cd.id, cd.name FROM companies_deduped cd
                   WHERE LOWER(cd.name) = LOWER(?)
                      OR LOWER(cd.name) LIKE LOWER(?)""",
                (name, name + "%")
            ).fetchall()
            exact = [r for r in rows if r[1].lower() == name.lower()]
            rows = exact if exact else rows

            if not rows:
                print(f"  {sector:<30} {name:<24} not in builtin")
                stats["missing_from_builtin"] += 1
                continue

            for company_id, company_name in rows[:1]:
                scm = cur.execute(
                    "SELECT cik, matched_name FROM sec_cik_map WHERE company_id = ?",
                    (company_id,)
                ).fetchone()
                if scm:
                    print(f"  {sector:<30} {name:<24} MATCHED -> {scm[1]} (CIK {scm[0]})")
                    stats["matched"] += 1
                else:
                    drop = dropped_by_name.get(company_name.lower())
                    if drop:
                        d = drop[0]
                        print(f"  {sector:<30} {name:<24} DROPPED in Phase A: "
                              f"was -> {d['matched_name']} ({d['reason']})")
                        stats["dropped_in_phase_a"] += 1
                    else:
                        print(f"  {sector:<30} {name:<24} in builtin (id={company_id}, "
                              f"'{company_name}') but never matched")
                        stats["unmatched_in_builtin"] += 1

    print()
    print("=== Summary ===")
    total = sum(stats.values())
    print(f"Total bellwethers checked:      {total}")
    print(f"  Currently matched:            {stats['matched']}")
    print(f"  Not in builtin source:        {stats['missing_from_builtin']}")
    print(f"  In builtin, never matched:    {stats['unmatched_in_builtin']}")
    print(f"  In builtin, dropped Phase A:  {stats['dropped_in_phase_a']}")


if __name__ == "__main__":
    main()

"""
Schema creation and data loading for michigan_tech_map PostgreSQL database.
Loads: companies_manual.csv + github_raw.csv
"""

import csv
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "michigan_tech_map"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD"),
)

DATA_DIR = Path(__file__).parent.parent / "data"

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    city                TEXT,
    employees           TEXT,
    sectors             TEXT,
    description         TEXT,
    -- Learning
    tech_stack          TEXT,
    -- Inventing
    invent_category     TEXT,
    -- Investing
    total_funding       TEXT,
    latest_round_type   TEXT,
    latest_round_date   TEXT,
    key_investors       TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tech_stack (
    id              SERIAL PRIMARY KEY,
    company_name    TEXT NOT NULL UNIQUE,
    github_org      TEXT,
    frameworks      TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS github_stats (
    id              SERIAL PRIMARY KEY,
    company_name    TEXT NOT NULL UNIQUE,
    github_org      TEXT,
    found           BOOLEAN,
    repo_count      INTEGER,
    total_stars     INTEGER,
    total_forks     INTEGER,
    top_languages   TEXT,
    last_push       TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""


def load_companies(cur):
    path = DATA_DIR / "companies_manual.csv"
    if not path.exists():
        print(f"Skipping companies: {path} not found")
        return

    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    data = [
        (
            r["name"], r["city"], r["employees"], r["sectors"],
            r["description"], r["tech_stack"], r["invent_category"],
            r["total_funding"], r["latest_round_type"], r["latest_round_date"],
            r["key_investors"],
        )
        for r in rows if r["name"].strip()
    ]

    execute_values(cur, """
        INSERT INTO companies
            (name, city, employees, sectors, description,
             tech_stack, invent_category,
             total_funding, latest_round_type, latest_round_date, key_investors)
        VALUES %s
        ON CONFLICT (name) DO UPDATE SET
            city              = EXCLUDED.city,
            employees         = EXCLUDED.employees,
            sectors           = EXCLUDED.sectors,
            description       = EXCLUDED.description,
            tech_stack        = EXCLUDED.tech_stack,
            invent_category   = EXCLUDED.invent_category,
            total_funding     = EXCLUDED.total_funding,
            latest_round_type = EXCLUDED.latest_round_type,
            latest_round_date = EXCLUDED.latest_round_date,
            key_investors     = EXCLUDED.key_investors
    """, data)
    print(f"Loaded {len(data)} companies")


def load_github(cur):
    path = DATA_DIR / "github_raw.csv"
    if not path.exists():
        print(f"Skipping github: {path} not found")
        return

    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    data = [
        (
            r["company_name"],
            r.get("github_org", ""),
            r.get("found", "no").lower() == "yes",
            int(r["repo_count"]) if r.get("repo_count") else 0,
            int(r["total_stars"]) if r.get("total_stars") else 0,
            int(r["total_forks"]) if r.get("total_forks") else 0,
            r.get("top_languages", ""),
            r.get("last_push", ""),
        )
        for r in rows if r.get("company_name", "").strip()
    ]

    execute_values(cur, """
        INSERT INTO github_stats
            (company_name, github_org, found, repo_count,
             total_stars, total_forks, top_languages, last_push)
        VALUES %s
        ON CONFLICT (company_name) DO UPDATE SET
            github_org    = EXCLUDED.github_org,
            found         = EXCLUDED.found,
            repo_count    = EXCLUDED.repo_count,
            total_stars   = EXCLUDED.total_stars,
            total_forks   = EXCLUDED.total_forks,
            top_languages = EXCLUDED.top_languages,
            last_push     = EXCLUDED.last_push
    """, data)
    print(f"Loaded {len(data)} github stats")


def load_tech_stack(cur):
    path = DATA_DIR / "tech_stack_raw.csv"
    if not path.exists():
        print(f"Skipping tech_stack: {path} not found")
        return

    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    data = [(r["company_name"], r["github_org"], r["frameworks"]) for r in rows if r["company_name"].strip()]

    execute_values(cur, """
        INSERT INTO tech_stack (company_name, github_org, frameworks)
        VALUES %s
        ON CONFLICT (company_name) DO UPDATE SET
            github_org = EXCLUDED.github_org,
            frameworks = EXCLUDED.frameworks
    """, data)
    print(f"Loaded {len(data)} tech stack entries")


def main():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    print("Creating schema...")
    cur.execute(SCHEMA)

    print("Clearing old data...")
    cur.execute("TRUNCATE TABLE companies RESTART IDENTITY CASCADE")
    cur.execute("TRUNCATE TABLE tech_stack RESTART IDENTITY CASCADE")

    print("Loading data...")
    load_companies(cur)
    load_github(cur)
    load_tech_stack(cur)

    conn.commit()
    cur.close()
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()

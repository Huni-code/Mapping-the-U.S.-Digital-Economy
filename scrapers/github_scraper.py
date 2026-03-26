"""
GitHub API Scraper — Inventing Dimension
For each company in companies_filtered.csv, finds their GitHub org and collects:
  - top languages, repo count, stars, forks, open issues, last push date
Output: data/github_raw.csv
"""

import csv
import time
import requests
from pathlib import Path

GITHUB_TOKEN   = "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"
COMPANIES_FILE = Path(__file__).parent.parent / "data" / "companies_filtered.csv"
OUTPUT_FILE    = Path(__file__).parent.parent / "data" / "github_raw.csv"

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

FIELDS = [
    "company_name", "github_org", "repo_count", "total_stars",
    "total_forks", "top_languages", "last_push", "found",
]


def search_org(company_name: str) -> str | None:
    """Search GitHub for the company org. Returns login name or None."""
    # Try exact org lookup first (slug from company name)
    slug = company_name.lower().replace(" ", "-").replace(".", "").replace(",", "")
    r = requests.get(f"https://api.github.com/orgs/{slug}", headers=HEADERS)
    if r.status_code == 200:
        return r.json()["login"]

    # Fall back to search
    query = company_name.replace(" ", "+")
    r = requests.get(
        f"https://api.github.com/search/users?q={query}+type:org&per_page=1",
        headers=HEADERS,
    )
    if r.status_code == 200:
        items = r.json().get("items", [])
        if items:
            return items[0]["login"]
    return None


def get_org_stats(org_login: str) -> dict:
    """Fetch repos and aggregate stats."""
    repos = []
    page = 1
    while True:
        r = requests.get(
            f"https://api.github.com/orgs/{org_login}/repos"
            f"?per_page=100&page={page}&sort=pushed",
            headers=HEADERS,
        )
        if r.status_code != 200 or not r.json():
            break
        batch = r.json()
        repos.extend(batch)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.5)

    if not repos:
        return {}

    lang_counts: dict[str, int] = {}
    total_stars = 0
    total_forks = 0
    last_push   = ""

    for repo in repos:
        total_stars += repo.get("stargazers_count", 0)
        total_forks += repo.get("forks_count", 0)
        lang = repo.get("language")
        if lang:
            lang_counts[lang] = lang_counts.get(lang, 0) + 1
        pushed = repo.get("pushed_at") or ""
        if pushed > last_push:
            last_push = pushed

    top_langs = sorted(lang_counts, key=lang_counts.get, reverse=True)[:5]

    return {
        "repo_count":    len(repos),
        "total_stars":   total_stars,
        "total_forks":   total_forks,
        "top_languages": ", ".join(top_langs),
        "last_push":     last_push[:10] if last_push else "",
    }


def check_rate_limit():
    r = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
    if r.status_code == 200:
        core = r.json()["resources"]["core"]
        remaining = core["remaining"]
        reset_in  = core["reset"] - int(time.time())
        print(f"  Rate limit: {remaining} remaining, resets in {reset_in}s")
        if remaining < 20:
            wait = max(reset_in + 5, 0)
            print(f"  Waiting {wait}s for rate limit reset...")
            time.sleep(wait)


def main():
    with open(COMPANIES_FILE, encoding="utf-8") as f:
        companies = list(csv.DictReader(f))

    # Resume support
    done = set()
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row["company_name"])
        print(f"Resuming — {len(done)} already done.")

    remaining = [c for c in companies if c["name"] not in done]
    print(f"Processing {len(remaining)} companies...\n")

    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    mode = "a" if done else "w"

    with open(OUTPUT_FILE, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not done:
            writer.writeheader()

        for i, company in enumerate(remaining, len(done) + 1):
            name = company["name"]
            print(f"[{i}/{len(companies)}] {name} ...", end=" ", flush=True)

            # Check rate limit every 50 companies
            if i % 50 == 0:
                check_rate_limit()

            org = search_org(name)
            if not org:
                print("not found")
                writer.writerow({
                    "company_name": name, "github_org": "", "repo_count": 0,
                    "total_stars": 0, "total_forks": 0, "top_languages": "",
                    "last_push": "", "found": "no",
                })
                f.flush()
                time.sleep(0.5)
                continue

            stats = get_org_stats(org)
            row = {
                "company_name": name,
                "github_org":   org,
                "found":        "yes",
                **stats,
            }
            writer.writerow(row)
            f.flush()

            print(f"✓ {org} | {stats.get('repo_count', 0)} repos | "
                  f"★{stats.get('total_stars', 0)} | "
                  f"{stats.get('top_languages', '')}")

            time.sleep(1.0)  # be polite to GitHub API

    print(f"\nDone! Results -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

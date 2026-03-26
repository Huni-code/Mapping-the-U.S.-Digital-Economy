"""
GitHub dependency scraper
For each company org, fetches dependency files (requirements.txt, package.json, go.mod, etc.)
from top repos and extracts frameworks/libraries.
Output: data/tech_stack_raw.csv
"""

import csv
import json
import time
import re
import base64
from pathlib import Path
import requests
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent.parent / ".env")

TOKEN    = os.getenv("GITHUB_TOKEN")
HEADERS  = {"Authorization": f"token {TOKEN}", "Accept": "application/vnd.github.v3+json"}
GITHUB_INPUT  = Path(__file__).parent.parent / "data" / "github_raw.csv"
OUTPUT_FILE   = Path(__file__).parent.parent / "data" / "tech_stack_raw.csv"

# Key frameworks/libraries to detect (lowercase)
KNOWN_FRAMEWORKS = {
    # Python
    "tensorflow", "torch", "pytorch", "keras", "scikit-learn", "sklearn",
    "fastapi", "flask", "django", "sqlalchemy", "celery", "pydantic",
    "pandas", "numpy", "scipy", "matplotlib", "airflow", "spark", "kafka-python",
    "boto3", "redis", "elasticsearch", "grpc",
    # JavaScript / TypeScript
    "react", "vue", "angular", "next", "nuxt", "svelte", "express",
    "nestjs", "graphql", "apollo", "webpack", "vite", "jest",
    "typescript", "socket.io", "mongoose", "sequelize", "prisma",
    # Go
    "gin", "echo", "fiber", "grpc", "gorm", "cobra", "viper",
    # Ruby
    "rails", "sinatra", "sidekiq", "devise", "rspec",
    # Java
    "spring", "hibernate", "kafka", "spark", "junit",
    # DevOps / Infra
    "kubernetes", "docker", "terraform", "ansible", "prometheus",
}

DEP_FILES = [
    "requirements.txt",
    "package.json",
    "go.mod",
    "Gemfile",
    "pom.xml",
    "build.gradle",
    "Pipfile",
    "pyproject.toml",
]


def gh_get(url, params=None):
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    if resp.status_code == 403:
        reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(reset - time.time() + 5, 10)
        print(f"  Rate limit hit, waiting {wait:.0f}s...")
        time.sleep(wait)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    return resp


def get_top_repos(org, n=5):
    resp = gh_get(f"https://api.github.com/orgs/{org}/repos",
                  params={"sort": "stars", "per_page": n})
    if resp.status_code != 200:
        return []
    return [r["name"] for r in resp.json() if isinstance(r, dict)]


def get_file_content(org, repo, filename):
    resp = gh_get(f"https://api.github.com/repos/{org}/{repo}/contents/{filename}")
    if resp.status_code != 200:
        return None
    data = resp.json()
    if isinstance(data, dict) and data.get("encoding") == "base64":
        return base64.b64decode(data["content"]).decode("utf-8", errors="ignore")
    return None


def parse_deps(filename, content):
    deps = set()
    if filename == "requirements.txt" or filename == "Pipfile":
        for line in content.splitlines():
            line = line.strip().lower()
            if line and not line.startswith("#"):
                pkg = re.split(r"[>=<!~\[]", line)[0].strip().replace("-", "_")
                if pkg in KNOWN_FRAMEWORKS or pkg.replace("_", "-") in KNOWN_FRAMEWORKS:
                    deps.add(pkg)

    elif filename == "package.json":
        try:
            data = json.loads(content)
            all_deps = {}
            all_deps.update(data.get("dependencies", {}))
            all_deps.update(data.get("devDependencies", {}))
            for pkg in all_deps:
                clean = pkg.lstrip("@").split("/")[-1].lower()
                if clean in KNOWN_FRAMEWORKS:
                    deps.add(clean)
        except Exception:
            pass

    elif filename == "go.mod":
        for line in content.splitlines():
            line = line.strip().lower()
            for fw in KNOWN_FRAMEWORKS:
                if fw in line:
                    deps.add(fw)

    elif filename in ("pom.xml", "build.gradle"):
        for fw in KNOWN_FRAMEWORKS:
            if fw in content.lower():
                deps.add(fw)

    elif filename == "pyproject.toml":
        for line in content.splitlines():
            line = line.strip().lower()
            for fw in KNOWN_FRAMEWORKS:
                if fw in line:
                    deps.add(fw)

    return deps


def scrape_org_deps(org):
    repos = get_top_repos(org, n=5)
    all_deps = set()
    for repo in repos:
        for dep_file in DEP_FILES:
            content = get_file_content(org, repo, dep_file)
            if content:
                found = parse_deps(dep_file, content)
                all_deps.update(found)
        time.sleep(0.3)
    return sorted(all_deps)


def main():
    # Known GitHub orgs for the 15 Michigan companies
    found = [
        {"company_name": "Rocket Companies",    "github_org": "rocketmortgage"},
        {"company_name": "Stryker",             "github_org": "stryker-mutator"},
        {"company_name": "Duo Security (Cisco)","github_org": "duosecurity"},
        {"company_name": "StockX",              "github_org": "stockx"},
        {"company_name": "May Mobility",        "github_org": "maymobility"},
        {"company_name": "TechSmith",           "github_org": "TechSmith"},
        {"company_name": "Altair Engineering",  "github_org": "altair-viz"},
        {"company_name": "Plex Systems",        "github_org": "PlexSystems"},
        {"company_name": "Censys",              "github_org": "censys"},
        {"company_name": "Benzinga",            "github_org": "Benzinga"},
        {"company_name": "Detroit Labs",        "github_org": "detroit-labs"},
        {"company_name": "Atomic Object",       "github_org": "atomicobject"},
        {"company_name": "OneMagnify",          "github_org": "onemagnify"},
        {"company_name": "Gentherm",            "github_org": "gentherm"},
        {"company_name": "Domino's Pizza",      "github_org": "dominos-pizza-engineering"},
    ]

    # Resume support
    done = set()
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done.add(row["company_name"])

    out_f = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=["company_name", "github_org", "frameworks"])
    if not done:
        writer.writeheader()

    try:
        for i, c in enumerate(found):
            name = c["company_name"]
            org  = c["github_org"]
            if name in done:
                continue

            print(f"[{i+1}/{len(found)}] {name} ({org})...", end=" ", flush=True)
            deps = scrape_org_deps(org)
            fw_str = ", ".join(deps) if deps else ""
            print(fw_str if fw_str else "none")

            writer.writerow({"company_name": name, "github_org": org, "frameworks": fw_str})
            out_f.flush()
            time.sleep(0.5)
    finally:
        out_f.close()

    print(f"\nDone! -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

"""
Stack Overflow Developer Survey Analysis (2017-2025)
Extracts:
  1. Tools & Frameworks trend (yearly usage %)
  2. AI tool adoption rate (2023-2025)
  3. Salary trend (yearly median, USD)
  4. Developer type distribution (AI/ML engineer growth)

Output: 4 tables in data/companies.db
"""

import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
import statistics

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
SURVEY_DIR = Path(__file__).parent.parent / "SO survey"

# ── Column mappings per year ─────────────────────────────────────────────────

MISC_TECH_COL = {
    2017: None,
    2018: "PlatformWorkedWith",
    2019: "MiscTechWorkedWith",
    2020: "MiscTechWorkedWith",
    2021: "MiscTechHaveWorkedWith",
    2022: "MiscTechHaveWorkedWith",
    2023: "MiscTechHaveWorkedWith",
    2024: "MiscTechHaveWorkedWith",
    2025: None,
}

WEBFRAME_COL = {
    2017: None,
    2018: None,
    2019: "WebFrameWorkedWith",
    2020: None,
    2021: "WebframeHaveWorkedWith",
    2022: "WebframeHaveWorkedWith",
    2023: "WebframeHaveWorkedWith",
    2024: "WebframeHaveWorkedWith",
    2025: "WebframeHaveWorkedWith",
}

SALARY_COL = {
    2017: "Salary",
    2018: "ConvertedSalary",
    2019: "ConvertedComp",
    2020: "ConvertedComp",
    2021: "ConvertedCompYearly",
    2022: "ConvertedCompYearly",
    2023: "ConvertedCompYearly",
    2024: "ConvertedCompYearly",
    2025: "ConvertedCompYearly",
}

DEVTYPE_COL = {
    2017: "DeveloperType",
    2018: "DevType",
    2019: "DevType",
    2020: "DevType",
    2021: "DevType",
    2022: "DevType",
    2023: "DevType",
    2024: "DevType",
    2025: "DevType",
}

AI_COLS = {
    2023: ["AISearchHaveWorkedWith", "AIDevHaveWorkedWith"],
    2024: ["AISearchDevHaveWorkedWith"],
    2025: ["AIModelsHaveWorkedWith"],
}

# Tools/frameworks to track
TRACKED_TOOLS = [
    "Docker", "Kubernetes", "AWS", "Azure", "Google Cloud",
    "TensorFlow", "PyTorch", "Pandas", "NumPy", "Spark",
    "React", "Node.js", "Vue.js", "Angular",
    "PostgreSQL", "MongoDB", "Redis", "Elasticsearch",
    "Git", "Linux",
]


PROFESSIONAL_DEV = "I am a developer by profession"


def read_csv(year: int, pro_only: bool = True):
    """Read survey CSV. If pro_only=True, keep only professional developers."""
    path = SURVEY_DIR / str(year) / "survey_results_public.csv"
    if not path.exists():
        return []
    rows = []
    with open(path, encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        while True:
            try:
                row = next(reader)
                if pro_only and row.get("MainBranch", "") != PROFESSIONAL_DEV:
                    continue
                rows.append(row)
            except StopIteration:
                break
            except Exception:
                continue
    return rows


def setup_tables(cur):
    cur.executescript("""
        DROP TABLE IF EXISTS so_tools_trend;
        CREATE TABLE so_tools_trend (
            year        INTEGER,
            tool        TEXT,
            usage_pct   REAL    -- % of respondents using this tool
        );

        DROP TABLE IF EXISTS so_ai_adoption;
        CREATE TABLE so_ai_adoption (
            year        INTEGER,
            ai_tool     TEXT,
            usage_pct   REAL
        );

        DROP TABLE IF EXISTS so_salary_trend;
        CREATE TABLE so_salary_trend (
            year            INTEGER,
            median_salary   REAL,   -- USD/year
            p25_salary      REAL,
            p75_salary      REAL,
            respondents     INTEGER
        );

        DROP TABLE IF EXISTS so_devtype_trend;
        CREATE TABLE so_devtype_trend (
            year        INTEGER,
            dev_type    TEXT,
            count       INTEGER,
            pct         REAL
        );

        DROP TABLE IF EXISTS so_desire_gap;
        CREATE TABLE so_desire_gap (
            year        INTEGER,
            tool        TEXT,
            have_pct    REAL,   -- % of pro devs currently using
            want_pct    REAL,   -- % of pro devs wanting to use
            gap         REAL    -- want_pct - have_pct (positive = growing demand)
        );
    """)


def analyze_tools(rows, year):
    """Returns [(tool, usage_pct)]"""
    misc_col  = MISC_TECH_COL.get(year)
    frame_col = WEBFRAME_COL.get(year)

    tool_counts = defaultdict(int)
    total = 0

    for row in rows:
        combined = []
        if misc_col and row.get(misc_col):
            combined += row[misc_col].split(";")
        if frame_col and row.get(frame_col):
            combined += row[frame_col].split(";")
        if combined:
            total += 1
            for item in combined:
                item = item.strip()
                for tracked in TRACKED_TOOLS:
                    if tracked.lower() in item.lower():
                        tool_counts[tracked] += 1

    if total == 0:
        return []
    return [(tool, round(count / total * 100, 2)) for tool, count in tool_counts.items()]


def analyze_ai(rows, year):
    """Returns [(ai_tool, usage_pct)]"""
    cols = AI_COLS.get(year, [])
    if not cols:
        return []

    ai_counts = defaultdict(int)
    total = 0

    for row in rows:
        used = []
        for col in cols:
            val = row.get(col, "")
            if val and val not in ("NA", ""):
                used += val.split(";")
        if used:
            total += 1
            for item in used:
                item = item.strip()
                if item:
                    ai_counts[item] += 1

    if total == 0:
        return []
    return [(tool, round(count / total * 100, 2)) for tool, count in ai_counts.items()]


def analyze_salary(rows, year):
    """Returns (median, p25, p75, count)"""
    col = SALARY_COL.get(year)
    if not col:
        return None

    salaries = []
    for row in rows:
        val = row.get(col, "")
        try:
            s = float(str(val).replace(",", "").strip())
            # Filter outliers: $5k - $500k
            if 5000 <= s <= 500000:
                salaries.append(s)
        except (ValueError, TypeError):
            pass

    if len(salaries) < 100:
        return None

    salaries.sort()
    n = len(salaries)
    median = statistics.median(salaries)
    p25 = salaries[int(n * 0.25)]
    p75 = salaries[int(n * 0.75)]
    return round(median), round(p25), round(p75), n


def analyze_desire_gap(rows, year):
    """Returns [(tool, have_pct, want_pct, gap)] for tracked tools.
    Only years with WantToWorkWith columns (2021+) are meaningful.
    """
    misc_have  = MISC_TECH_COL.get(year)
    misc_want  = "MiscTechWantToWorkWith" if year >= 2021 else None
    frame_have = WEBFRAME_COL.get(year)
    frame_want = "WebframeWantToWorkWith" if year >= 2021 else None

    if not misc_want and not frame_want:
        return []

    have_counts = {t: 0 for t in TRACKED_TOOLS}
    want_counts = {t: 0 for t in TRACKED_TOOLS}
    total = len(rows)
    if total == 0:
        return []

    for row in rows:
        have_items = []
        want_items = []
        if misc_have and row.get(misc_have):
            have_items += row[misc_have].split(";")
        if misc_want and row.get(misc_want):
            want_items += row[misc_want].split(";")
        if frame_have and row.get(frame_have):
            have_items += row[frame_have].split(";")
        if frame_want and row.get(frame_want):
            want_items += row[frame_want].split(";")

        for item in have_items:
            for tracked in TRACKED_TOOLS:
                if tracked.lower() in item.strip().lower():
                    have_counts[tracked] += 1
        for item in want_items:
            for tracked in TRACKED_TOOLS:
                if tracked.lower() in item.strip().lower():
                    want_counts[tracked] += 1

    results = []
    for tool in TRACKED_TOOLS:
        have_pct = round(have_counts[tool] / total * 100, 2)
        want_pct = round(want_counts[tool] / total * 100, 2)
        gap = round(want_pct - have_pct, 2)
        if have_pct > 0 or want_pct > 0:
            results.append((tool, have_pct, want_pct, gap))
    return results


def analyze_devtype(rows, year):
    """Returns [(dev_type, count, pct)]"""
    col = DEVTYPE_COL.get(year)
    if not col:
        return []

    type_counts = defaultdict(int)
    total = 0

    for row in rows:
        val = row.get(col, "")
        if not val or val in ("NA", ""):
            continue
        types = val.split(";")
        total += 1
        for t in types:
            t = t.strip()
            if t:
                type_counts[t] += 1

    if total == 0:
        return []
    return [(t, c, round(c / total * 100, 2)) for t, c in type_counts.items()]


def main():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    setup_tables(cur)

    years = range(2017, 2026)

    for year in years:
        path = SURVEY_DIR / str(year) / "survey_results_public.csv"
        if not path.exists():
            print(f"  {year}: file not found, skipping")
            continue

        print(f"Processing {year}...", end=" ")
        rows = read_csv(year)
        print(f"{len(rows)} respondents")

        # Tools trend
        tools = analyze_tools(rows, year)
        for tool, pct in tools:
            cur.execute("INSERT INTO so_tools_trend VALUES (?,?,?)", (year, tool, pct))

        # AI adoption
        ai = analyze_ai(rows, year)
        for tool, pct in ai:
            cur.execute("INSERT INTO so_ai_adoption VALUES (?,?,?)", (year, tool, pct))

        # Salary
        sal = analyze_salary(rows, year)
        if sal:
            cur.execute("INSERT INTO so_salary_trend VALUES (?,?,?,?,?)", (year, *sal))

        # DevType
        devtypes = analyze_devtype(rows, year)
        for t, c, pct in devtypes:
            cur.execute("INSERT INTO so_devtype_trend VALUES (?,?,?,?)", (year, t, c, pct))

        # Desire gap (pro devs only, 2021+)
        desire = analyze_desire_gap(rows, year)
        for tool, have_pct, want_pct, gap in desire:
            cur.execute("INSERT INTO so_desire_gap VALUES (?,?,?,?,?)",
                        (year, tool, have_pct, want_pct, gap))

    conn.commit()

    # Summary
    print("\n=== Tools trend (top 5 in 2024) ===")
    for row in cur.execute("""
        SELECT tool, usage_pct FROM so_tools_trend
        WHERE year=2024 ORDER BY usage_pct DESC LIMIT 5
    """):
        print(f"  {row[0]:<25} {row[1]}%")

    print("\n=== AI adoption (2023-2025 top tools) ===")
    for row in cur.execute("""
        SELECT year, ai_tool, usage_pct FROM so_ai_adoption
        ORDER BY year, usage_pct DESC
    """):
        print(f"  {row[0]}  {row[1]:<40} {row[2]}%")

    print("\n=== Salary trend ===")
    for row in cur.execute("""
        SELECT year, median_salary, respondents FROM so_salary_trend ORDER BY year
    """):
        print(f"  {row[0]}  median=${row[1]:,}  n={row[2]}")

    print("\n=== DevType trend (AI/ML) ===")
    for row in cur.execute("""
        SELECT year, dev_type, pct FROM so_devtype_trend
        WHERE dev_type LIKE '%data%' OR dev_type LIKE '%machine%' OR dev_type LIKE '%ML%'
           OR dev_type LIKE '%AI%' OR dev_type LIKE '%scientist%'
        ORDER BY year, pct DESC
    """):
        print(f"  {row[0]}  {row[1]:<45} {row[2]}%")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()

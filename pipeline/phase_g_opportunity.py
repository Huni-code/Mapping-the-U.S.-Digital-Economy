"""
Phase G: Compute sector-level Opportunity Score Investing layer from the
enriched sec_financials table.

Metrics (per company, 2020-2024):
  - CAGR:   (rev_2024 / rev_2020)^(1/4) - 1       [requires both endpoints, both > 0]
  - SFR:    median_year(OCF / rd_expense)          [pairs where both > 0]
  - Margin: median_year(OCF / revenue)             [pairs where revenue > 0]

Normalization:
  5th-95th percentile clipping across all companies, then linear scale to [0, 1].
  Uniform across metrics — robust to outliers.

Per-company Investing score:
  0.4 * CAGR_n + 0.3 * SFR_n + 0.3 * Margin_n
  If a metric is missing, its weight is dropped and the remaining weights are
  renormalized (sum to 1). Companies with no valid metric get NaN.

Sector Investing score:
  Median of per-company Investing scores.
  n_scored < 5 -> insufficient_data flag (but value still emitted for inspection).

Outputs:
  - DB table: sector_opportunity_metrics
  - data/sector_opportunity_metrics.csv
  - data/company_opportunity_metrics.csv  (per-company detail for audit)
"""

import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"
SECTOR_CSV = Path(__file__).parent.parent / "data" / "sector_opportunity_metrics.csv"
COMPANY_CSV = Path(__file__).parent.parent / "data" / "company_opportunity_metrics.csv"

WEIGHTS = {"cagr": 0.4, "sfr": 0.3, "margin": 0.3}
WINDOW_START = 2020
WINDOW_END = 2024
CAGR_YEARS = WINDOW_END - WINDOW_START
WEAK_THRESHOLD = 5


def pct_norm(s: pd.Series) -> pd.Series:
    """Clip to 5-95th percentile, scale to [0, 1]."""
    valid = s.dropna()
    if len(valid) < 2:
        return s
    lo, hi = valid.quantile(0.05), valid.quantile(0.95)
    if hi == lo:
        return s.where(s.isna(), 0.5)
    return (s.clip(lo, hi) - lo) / (hi - lo)


def per_company_score(row: pd.Series) -> float:
    parts, total_w = 0.0, 0.0
    for key, col in [("cagr", "cagr_n"), ("sfr", "sfr_n"), ("margin", "margin_n")]:
        v = row[col]
        if pd.notna(v):
            parts += WEIGHTS[key] * v
            total_w += WEIGHTS[key]
    return parts / total_w if total_w > 0 else np.nan


def main():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql(
        f"""SELECT sf.company_id, cd.name, cc.sector, sf.year,
                   sf.revenue, sf.rd_expense, sf.operating_cash_flow
            FROM sec_financials sf
            JOIN sec_cik_map scm ON scm.company_id = sf.company_id
            JOIN companies_deduped cd ON cd.id = sf.company_id
            JOIN company_classifications cc ON cc.company_id = sf.company_id
            WHERE sf.year BETWEEN {WINDOW_START} AND {WINDOW_END}""",
        conn,
    )

    rev_pivot = df.pivot_table(
        index="company_id", columns="year", values="revenue", aggfunc="first"
    )
    cagr = {}
    for cid, row in rev_pivot.iterrows():
        r0 = row.get(WINDOW_START)
        r1 = row.get(WINDOW_END)
        if pd.notna(r0) and pd.notna(r1) and r0 > 0 and r1 > 0:
            cagr[cid] = (r1 / r0) ** (1 / CAGR_YEARS) - 1

    sfr_df = df[df["operating_cash_flow"].notna()
                & df["rd_expense"].notna()
                & (df["rd_expense"] > 0)].copy()
    sfr_df["ratio"] = sfr_df["operating_cash_flow"] / sfr_df["rd_expense"]
    sfr = sfr_df.groupby("company_id")["ratio"].median()

    margin_df = df[df["operating_cash_flow"].notna()
                   & df["revenue"].notna()
                   & (df["revenue"] > 0)].copy()
    margin_df["ratio"] = margin_df["operating_cash_flow"] / margin_df["revenue"]
    margin = margin_df.groupby("company_id")["ratio"].median()

    name_by_id = df.groupby("company_id")["name"].first()
    sector_by_id = df.groupby("company_id")["sector"].first()

    comp = pd.DataFrame({
        "company_id": sector_by_id.index,
        "name": name_by_id.reindex(sector_by_id.index).values,
        "sector": sector_by_id.values,
        "cagr": pd.Series(cagr).reindex(sector_by_id.index).values,
        "sfr": sfr.reindex(sector_by_id.index).values,
        "margin": margin.reindex(sector_by_id.index).values,
    }).dropna(subset=["sector"])

    comp["cagr_n"] = pct_norm(comp["cagr"])
    comp["sfr_n"] = pct_norm(comp["sfr"])
    comp["margin_n"] = pct_norm(comp["margin"])

    comp["investing_score"] = comp.apply(per_company_score, axis=1)

    sec_rows = []
    for sector, g in comp.groupby("sector"):
        n_total = len(g)
        n_scored = int(g["investing_score"].notna().sum())
        sec_rows.append({
            "sector": sector,
            "n_companies": n_total,
            "n_scored": n_scored,
            "cagr_median": g["cagr"].median(),
            "sfr_median": g["sfr"].median(),
            "margin_median": g["margin"].median(),
            "cagr_score": g["cagr_n"].median(),
            "sfr_score": g["sfr_n"].median(),
            "margin_score": g["margin_n"].median(),
            "investing_score": g["investing_score"].median(),
            "insufficient_data": int(n_scored < WEAK_THRESHOLD),
        })
    sec_df = pd.DataFrame(sec_rows).sort_values("investing_score", ascending=False)

    sec_df.to_csv(SECTOR_CSV, index=False)
    comp[["company_id", "name", "sector", "cagr", "sfr", "margin",
          "investing_score"]].to_csv(COMPANY_CSV, index=False)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS sector_opportunity_metrics")
    cur.execute("""
        CREATE TABLE sector_opportunity_metrics (
            sector TEXT PRIMARY KEY,
            n_companies INTEGER,
            n_scored INTEGER,
            cagr_median REAL,
            sfr_median REAL,
            margin_median REAL,
            cagr_score REAL,
            sfr_score REAL,
            margin_score REAL,
            investing_score REAL,
            insufficient_data INTEGER
        )
    """)
    for r in sec_rows:
        cur.execute("""INSERT INTO sector_opportunity_metrics VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (r["sector"], r["n_companies"], r["n_scored"],
                     r["cagr_median"], r["sfr_median"], r["margin_median"],
                     r["cagr_score"], r["sfr_score"], r["margin_score"],
                     r["investing_score"], r["insufficient_data"]))
    conn.commit()
    conn.close()

    print("=== Phase G: Investing Layer ===")
    print(f"Window: {WINDOW_START}-{WINDOW_END}   Weights: CAGR 40% / SFR 30% / Margin 30%")
    print(f"Weak threshold: n_scored < {WEAK_THRESHOLD}")
    print()
    print(f"{'Sector':<32} {'n':>4} {'CAGR':>8} {'SFR':>7} {'Margin':>8} {'Score':>7}  Flag")
    print("-" * 85)

    def fmt_pct(x, dp=1):
        return "   n/a" if pd.isna(x) else f"{x * 100:>6.{dp}f}%"

    def fmt_num(x, dp=2):
        return "   n/a" if pd.isna(x) else f"{x:>6.{dp}f}"

    for _, r in sec_df.iterrows():
        flag = "  INSUFFICIENT" if r["insufficient_data"] else ""
        print(f"  {r['sector']:<30} {r['n_scored']:>4} "
              f"{fmt_pct(r['cagr_median']):>8} "
              f"{fmt_num(r['sfr_median']):>7} "
              f"{fmt_pct(r['margin_median']):>8} "
              f"{fmt_num(r['investing_score'], 3):>7}"
              f"{flag}")


if __name__ == "__main__":
    main()

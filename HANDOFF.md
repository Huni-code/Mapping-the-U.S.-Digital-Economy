# Handoff — CIK Cleanup + OCF Enrichment Pipeline

Session date: 2026-04-19
Working directory: `Mapping-the-U.S.-Digital-Economy`

---

## Context

Original `enrich_sec.py` used SEC full-text search to match company → CIK. This
caught investor/acquirer filings for private companies (Anthropic → Amazon,
xAI → Tesla, Character.AI → Alphabet). **2,937 CIK mappings, ~50% corrupted.**

Built a 7-phase cleanup + enrichment pipeline. Final: **481 clean CIKs, all 3
metrics (revenue / R&D / OCF) populated via XBRL Company Facts API.**

Spec source: `phase.txt` (Phase 0-D) + on-the-fly extensions (Phase E-G).

---

## Completed phases

| Phase | Script | Result |
|-------|--------|--------|
| 0 | `cp data/companies.db data/companies_backup_20260419.db` | Backup made |
| A | `pipeline/phase_a_clean.py` | 2,937 → 438 (zero_overlap / mega_cap_hijack / jaccard<0.5) |
| B | `pipeline/phase_b_rematch.py` | +61 strict matches via `company_tickers.json` → 499 |
| C | `pipeline/phase_c_sanity.py` | 499 → 480 (revenue scale + duplicate CIK). 116 non_tech_sic flags |
| D | `pipeline/phase_d_report.py` | Impact report + per-sector survival |
| — | `pipeline/bellwether_check.py` | 120-company canonical check across 16 sectors. 98% accuracy |
| E | `pipeline/phase_e_manual_patches.py` | +Coinbase (INSERT), Elastic (UPDATE to correct CIK). 480 → 481 |
| F | `pipeline/phase_f_ocf.py` | `ALTER TABLE ADD operating_cash_flow`. 413/481 companies got OCF |
| F2 | `pipeline/phase_f2_revenue_rd.py` | NULL-only fill of revenue + R&D. 975 rev + 689 R&D filled |
| G | `pipeline/phase_g_opportunity.py` | Sector Investing score: 40% CAGR + 30% SFR + 30% Cash Margin |

**Current state: 481 CIK mappings, 4,161 sec_financials rows (3,188 rev / 2,353
R&D / 3,499 OCF).** Sector scores written to `sector_opportunity_metrics` table.

---

## Key decisions (don't re-litigate)

1. **480-ish is the right number.** builtin.com editorial policy excludes
   mega-caps (Oracle, Uber, Shopify, Spotify, etc.) — not a pipeline bug.
2. **CAGR window: 2020 → 2024 fixed.** 4-year CAGR. Companies missing either
   endpoint are excluded from CAGR but still participate via SFR/Margin.
3. **Phase F NULL insert allowed.** OCF-only years stored with revenue/R&D
   NULL — trend still visible.
4. **Phase F2 fills NULL only, doesn't overwrite.** Preserves per-year tag
   consistency; avoids revenue jumping between `Revenues` and `SalesRevenueNet`
   across years of the same company.
5. **Opportunity Score weights:**
   - Top level: Learning 40% / Inventing 30% / Investing 30% (from original report)
   - Investing sub-layer: CAGR 40% / SFR 30% / Margin 30%
6. **Normalization:** 5th–95th percentile clipping across all companies,
   scaled to [0, 1]. Per-company Investing score = weighted sum with
   re-weighting when CAGR is missing.
7. **Weak sector threshold: n_scored < 5** → `insufficient_data` flag. Currently
   flagged: AI foundation (3), AI assistants (4), Creative & design (1),
   Search engines (1).

---

## Output files

```
data/
  companies.db                         # cleaned + enriched
  companies_backup_20260419.db         # pre-cleanup backup
  company_tickers.json                 # SEC cached (Phase B)
  sic_cache.json                       # Phase C
  ocf_cache.json                       # Phase F
  revenue_rd_cache.json                # Phase F2
  phase_a_dropped.csv / phase_a_kept.csv
  phase_b_new_matches.csv
  phase_c_flagged.csv
  phase_f_ocf_errors.csv
  phase_f2_enrichment_log.csv
  sector_opportunity_metrics.csv       # final Investing scores
  company_opportunity_metrics.csv      # per-company audit trail
```

New DB table: `sector_opportunity_metrics` (sector, n_companies, n_scored,
cagr_median, sfr_median, margin_median, cagr_score, sfr_score, margin_score,
investing_score, insufficient_data).

sec_financials schema now has: company_id, cik, year, revenue, rd_expense,
net_income, **operating_cash_flow**.

---

## Remaining tasks

### 1. Integrate new Investing score into `dashboard.py`

**Current dashboard** computes Investing from `rev_pivot["growth_pct"]`
(2019→2024 total growth, not CAGR) — min-max normalized across sectors. Lines
~966–994.

**Replace with:** pull `investing_score` directly from
`sector_opportunity_metrics` table. No normalization needed (already 0-1).

- `dashboard.py:966` → replace `rev_cagr = rev_pivot.set_index("sector")["growth_pct"] / 100`
  with a SQL pull from the new table.
- `dashboard.py:984-985` → drop the median-fill pattern for Investing; use
  `insufficient_data` flag to mark sectors instead.
- Adjust `SECTOR_CONTEXT` strings (lines ~1000+) if they cite specific growth
  numbers now stale.

### 2. Clear Streamlit cache

`dashboard.py` uses `@st.cache_data` on `load_data()`. After schema + value
changes, add a cache bust (change a constant in `load_data` or use `st.cache_data.clear()`).

### 3. New dashboard visualizations to add

- **Self-Funding Ratio chart** (sector median, 2020-2024). Bar or ranked scatter.
  Highlights sectors that generate their own R&D budget.
- **Cash Margin chart** (sector median). Shows profitability quality.
- **Score decomposition chart:** stacked bar per sector showing CAGR / SFR /
  Margin contribution to Investing layer.
- Opportunity Score ranking chart (existing, line ~1130) will shift because
  Investing layer values change — expect AI sectors to drop (insufficient data),
  Gaming / Cybersecurity / Developer to rise.

### 4. Update methodology expander

Lines ~1096-1120. Mention new formula:
```
Investing = 0.4 × CAGR + 0.3 × Self-Funding Ratio + 0.3 × Cash Margin
Window: 2020-2024 · n<5 flagged as insufficient
```

### 5. Weak-sector UX

4 sectors have `insufficient_data=1`. Dashboard should show these grayed out
or behind an "insufficient data" badge instead of silently outputting a score.

### 6. Optional — Re-run pipeline after SEC filing updates

`revenue_rd_cache.json` and `ocf_cache.json` are local caches. To refresh later:
delete the cache files and re-run Phase F + F2. Takes ~2 min total.

---

## How to resume on a new machine

```bash
git clone <repo>
cd Mapping-the-U.S.-Digital-Economy
pip install streamlit plotly pandas scikit-learn requests

# DB is committed (data/companies.db). Caches too.
# To see current state:
python pipeline/phase_d_report.py     # survival counts
python pipeline/phase_g_opportunity.py # opportunity scores

# To run dashboard:
streamlit run dashboard.py
```

---

## Notes for Claude Code on next session

- Permission: `.claude/settings.local.json` has `"Bash"` blanket allow.
- `HANDOFF.md` (this file) is the single source of truth on what's done.
- Don't re-run pipeline unless user asks — current DB is the product.
- User prefers terse responses, Korean for chat, English for code/comments.
- Dashboard integration is the next concrete step; user said
  "대시보드는 점수 로직 확정된 다음에 붙여야 두 번 안 고쳐" — Investing logic is
  now confirmed, so dashboard can be modified.

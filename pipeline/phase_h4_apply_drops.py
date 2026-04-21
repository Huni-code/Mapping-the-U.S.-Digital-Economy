"""
Phase H4: Apply user-approved drops from name-similarity audit.
- 19 name-collision drops from original Phase H3 audit
- 2 SIC exclusions (Marsh McLennan, Raymond James)
- 2 late additions from post-F2 revenue_scale audit (see comment below)

Late additions (post-F2 Phase C review): these slipped past Phase H's SIC
filter because their wrong-match SEC entity sits inside the "tech" SIC range
(7370-7379) despite being semantically different businesses. Detected via
Startup/Small builtin companies with >$100M matched-public revenue.
"""
import sqlite3

DB = 'data/companies.db'

# (company_name, exclude_reason)
APPROVED_DROPS = [
    # Pharma mismatches (STRONG)
    ('Array', 'Name collision: mapped to Array Biopharma (pharma)'),
    ('Cognition', 'Name collision: mapped to Cognition Therapeutics (pharma)'),
    ('EDGE', 'Name collision: mapped to Edge Therapeutics (pharma)'),
    ('Quince', 'Name collision: mapped to Quince Therapeutics (pharma)'),
    # Pharma-SIC mismatches (REVIEW)
    ('CIMA+', 'Name collision: mapped to CIMA Labs (pharma SIC 2834)'),
    ('Home Alliance', 'Name collision: mapped to American Home Alliance (pharma SIC 2834)'),
    ('Disruptive Technologies', 'Name collision: mapped to Online Disruptive Tech (pharma SIC 2835)'),
    # Banking/insurance mismatches
    ('Compass', 'Name collision: mapped to Compass Bancshares (bank)'),
    ('Kepler', 'Name collision: mapped to Kepler Group (insurance)'),
    ('Octave', 'Name collision: mapped to Octave Specialty (surety insurance)'),
    ('Winton', 'Name collision: mapped to Winton Financial (savings institution)'),
    # Construction
    ('Mobile Solutions', 'Name collision: mapped to Xstream Mobile Solutions (construction SIC 1700)'),
    # Other name collisions
    ('Path', 'Name collision: mapped to PATH 1 NETWORK TECH; UiPath tracked under separate CIK'),
    ('Firefly', 'Name collision: mapped to Firefly Neuroscience, not Firefly Aerospace'),
    ('Hinge', 'Name collision: mapped to Hinge Health; DB Hinge is dating app (Match Group)'),
    ('MERGE', 'Name collision: mapped to Merge Healthcare, not the video tech firm'),
    ('AXS', 'Name collision: mapped to AXS ONE INC, not AXS ticketing'),
    ('Capital Group', 'Name collision: mapped to Internet Capital Group, not Capital Group mutual funds'),
    ('DNA', 'Name collision: mapped to DNA Brands (ad agency)'),
    # Additional SIC exclusions (non-tech financial services)
    ('Marsh McLennan', 'SIC 6411 Insurance Agents/Brokers — not tech'),
    ('Raymond James', 'SIC 6211 Security Brokers — not tech'),
    # Late additions (post-F2 revenue_scale audit)
    ('Q ANALYSTS LLC', 'Name collision: QA testing services vs Analysts International (IT staffing, $143M)'),
    ('Telos Media', 'Name collision: digital entertainment studio vs Telos Corp (cybersecurity/gov, $145M)'),
]


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    applied = []
    skipped = []

    for name, reason in APPROVED_DROPS:
        cur.execute('''
            UPDATE sec_cik_map
            SET excluded = 1, exclude_reason = ?
            WHERE company_id = (SELECT id FROM companies_deduped WHERE name = ?)
              AND COALESCE(excluded, 0) = 0
        ''', (reason, name))
        if cur.rowcount > 0:
            applied.append((name, reason))
        else:
            skipped.append(name)

    conn.commit()
    conn.close()

    print(f'=== Phase H4 ===')
    print(f'Applied: {len(applied)}')
    print(f'Skipped (already excluded or missing): {len(skipped)}')
    for name, reason in applied:
        print(f'  [DROP] {name:30} {reason[:70]}')
    if skipped:
        print(f'\nSkipped:')
        for n in skipped:
            print(f'  {n}')


if __name__ == '__main__':
    main()

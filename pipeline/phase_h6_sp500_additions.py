"""
Phase H6: Add S&P 500 IT / Communication Services members missing from DB.
Methodology: S&P 500 GICS Information Technology + Communication Services constituents.
Skips Semiconductors (already n=14) and Search (Google dominates).
Focuses on INSUF sectors (Creative, Subscription) + strategic completeness
(Cybersecurity, Developer tooling, Enterprise, E-commerce).
"""
import sqlite3, json, urllib.request
from pathlib import Path

DB = 'data/companies.db'
TICKERS_CACHE = 'data/company_tickers.json'

ADDITIONS = [
    # Cybersecurity (n=6 -> target 12+)
    ('Palo Alto Networks', 'PANW', 'Cybersecurity & identity', 'Subscription (SaaS)', 'Next-gen firewall, XDR'),
    ('CrowdStrike', 'CRWD', 'Cybersecurity & identity', 'Subscription (SaaS)', 'Endpoint/cloud security'),
    ('Fortinet', 'FTNT', 'Cybersecurity & identity', 'Licensing (enterprise software)', 'Network security'),
    ('Okta', 'OKTA', 'Cybersecurity & identity', 'Subscription (SaaS)', 'Identity/SSO'),
    ('Zscaler', 'ZS', 'Cybersecurity & identity', 'Subscription (SaaS)', 'Zero trust cloud security'),
    ('SentinelOne', 'S', 'Cybersecurity & identity', 'Subscription (SaaS)', 'AI endpoint security'),
    ('CyberArk', 'CYBR', 'Cybersecurity & identity', 'Subscription (SaaS)', 'Privileged access mgmt'),
    ('Rapid7', 'RPD', 'Cybersecurity & identity', 'Subscription (SaaS)', 'Vuln mgmt, SIEM'),
    ('Check Point Software', 'CHKP', 'Cybersecurity & identity', 'Licensing (enterprise software)', 'Network security'),

    # Developer tooling (n=10 -> target 18+)
    ('Datadog', 'DDOG', 'Developer tooling', 'Usage-based metered billing (cloud, APIs)', 'Monitoring/observability'),
    ('Cloudflare', 'NET', 'Developer tooling', 'Usage-based metered billing (cloud, APIs)', 'Edge network, security'),
    ('Confluent', 'CFLT', 'Developer tooling', 'Usage-based metered billing (cloud, APIs)', 'Kafka data streaming'),
    ('Fastly', 'FSLY', 'Developer tooling', 'Usage-based metered billing (cloud, APIs)', 'Edge compute/CDN'),
    ('JFrog', 'FROG', 'Developer tooling', 'Subscription (SaaS)', 'DevOps artifact mgmt'),
    ('Twilio', 'TWLO', 'Developer tooling', 'Usage-based metered billing (cloud, APIs)', 'Comms API'),
    ('Atlassian', 'TEAM', 'Developer tooling', 'Subscription (SaaS)', 'Jira, Confluence'),
    ('Braze', 'BRZE', 'Developer tooling', 'Subscription (SaaS)', 'Customer engagement platform'),
    ('Alteryx', 'AYX', 'Developer tooling', 'Subscription (SaaS)', 'Data analytics platform'),
    ('Appian', 'APPN', 'Developer tooling', 'Subscription (SaaS)', 'Low-code platform'),
    ('Teradata', 'TDC', 'Developer tooling', 'Licensing (enterprise software)', 'Data warehouse'),

    # Enterprise / ERP / HRM — fill with HR/payroll/enterprise SaaS
    ('Intuit', 'INTU', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'QuickBooks, TurboTax'),
    ('Automatic Data Processing', 'ADP', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'Payroll/HR outsourcing'),
    ('Paychex', 'PAYX', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'Payroll/HR SMB'),
    ('Paycom', 'PAYC', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'HCM SaaS'),
    ('Open Text', 'OTEX', 'Enterprise / ERP / HRM', 'Licensing (enterprise software)', 'Info mgmt'),
    ('Manhattan Associates', 'MANH', 'Enterprise / ERP / HRM', 'Licensing (enterprise software)', 'Supply chain SaaS'),
    ('NICE', 'NICE', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'CX/contact center'),
    ('Box', 'BOX', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'Cloud content mgmt'),
    ('Smartsheet', 'SMAR', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'Work mgmt SaaS'),
    ('Dayforce', 'DAY', 'Enterprise / ERP / HRM', 'Subscription (SaaS)', 'HCM (Ceridian)'),

    # E-commerce (n=7 -> target 12)
    ('eBay', 'EBAY', 'E-commerce platforms', 'Transaction fees / marketplace cut', 'Consumer marketplace'),
    ('Etsy', 'ETSY', 'E-commerce platforms', 'Transaction fees / marketplace cut', 'Handmade/vintage marketplace'),
    ('Chewy', 'CHWY', 'E-commerce platforms', 'Transaction fees / marketplace cut', 'Pet e-commerce'),
    ('Wayfair', 'W', 'E-commerce platforms', 'Transaction fees / marketplace cut', 'Furniture e-commerce'),
    ('MercadoLibre', 'MELI', 'E-commerce platforms', 'Transaction fees / marketplace cut', 'LatAm e-commerce'),

    # Advertising & attention
    ('The Trade Desk', 'TTD', 'Advertising & attention', 'Advertising', 'Programmatic ad DSP'),
    ('Reddit', 'RDDT', 'Advertising & attention', 'Advertising', 'Community platform + ads'),
    ('DoubleVerify', 'DV', 'Advertising & attention', 'Advertising', 'Ad verification'),

    # Subscription content (n=4 -> 6)
    ('Roku', 'ROKU', 'Subscription content', 'Advertising', 'Streaming platform'),
    ('Sirius XM', 'SIRI', 'Subscription content', 'Subscription (SaaS)', 'Satellite radio/streaming'),

    # Gaming
    ('NetEase', 'NTES', 'Gaming & virtual environments', 'Transaction fees / marketplace cut', 'Games publisher (China)'),

    # Productivity
    ('Monday.com', 'MNDY', 'Productivity & collaboration', 'Subscription (SaaS)', 'Work OS'),

    # Hardware & Networking (megacaps)
    ('Apple', 'AAPL', 'Hardware & Networking', 'Licensing (enterprise software)', 'iPhone, Mac, services'),
    ('Super Micro Computer', 'SMCI', 'Hardware & Networking', 'Licensing (enterprise software)', 'AI servers'),
    ('Logitech', 'LOGI', 'Hardware & Networking', 'Licensing (enterprise software)', 'Peripherals'),

    # Creative & design tools (n=4 -> target 6)
    ('Pinterest', 'PINS', 'Creative & design tools', 'Advertising', 'Visual discovery'),
    ('DocuSign', 'DOCU', 'Creative & design tools', 'Subscription (SaaS)', 'E-signature, docs'),
]


def load_tickers():
    with open(TICKERS_CACHE) as f:
        return json.load(f)


def find_cik(ticker, tickers_data):
    t = ticker.upper()
    for _, entry in tickers_data.items():
        if entry['ticker'].upper() == t:
            return str(entry['cik_str']).zfill(10), entry['title']
    return None, None


def main():
    tickers = load_tickers()
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    added = []
    missing = []

    for name, ticker, sector, revmodel, reason in ADDITIONS:
        cur.execute('SELECT id FROM companies_deduped WHERE LOWER(name) = LOWER(?)', (name,))
        row = cur.fetchone()
        if row:
            cid = row[0]
            print(f'[SKIP-EXISTS] {name} id={cid}')
        else:
            cur.execute('''INSERT INTO companies_deduped (name, hub, state, company_size, employees_count)
                           VALUES (?, ?, ?, ?, ?)''', (name, 'Unknown', 'Unknown', 'Large', None))
            cid = cur.lastrowid
            cur.execute('UPDATE companies_deduped SET id = ? WHERE rowid = ? AND id IS NULL', (cid, cid))
            print(f'[INSERT] {name} id={cid}')

        cik, title = find_cik(ticker, tickers)
        if not cik:
            missing.append((name, ticker))
            print(f'  [NO-CIK] {ticker}')
            continue

        cur.execute('SELECT cik FROM sec_cik_map WHERE company_id = ?', (cid,))
        existing = cur.fetchone()
        if existing:
            cur.execute('UPDATE sec_cik_map SET cik=?, matched_name=?, excluded=0 WHERE company_id=?',
                        (cik, title, cid))
        else:
            cur.execute('INSERT INTO sec_cik_map (company_id, cik, matched_name) VALUES (?, ?, ?)',
                        (cid, cik, title))

        cur.execute('SELECT sector FROM company_classifications WHERE company_id = ?', (cid,))
        if cur.fetchone():
            cur.execute('UPDATE company_classifications SET sector=?, revenue_model=? WHERE company_id=?',
                        (sector, revmodel, cid))
        else:
            cur.execute('INSERT INTO company_classifications (company_id, sector, revenue_model) VALUES (?, ?, ?)',
                        (cid, sector, revmodel))
        added.append((cid, cik, name))
        print(f'  CIK {cik} -> {sector}')

    conn.commit()
    conn.close()
    print(f'\n=== Summary === Added/updated: {len(added)}  Missing: {len(missing)}')
    for n, t in missing:
        print(f'  {n} ({t})')


if __name__ == '__main__':
    main()

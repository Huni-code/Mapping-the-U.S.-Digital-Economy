"""
Add 13 public AI companies missing from builtin.com scrape.
Strategy: strict match via company_tickers.json, then trigger Phase F + F2 enrichment.
"""
import sqlite3, json, urllib.request, time
from pathlib import Path

DB = 'data/companies.db'
TICKERS_CACHE = 'data/company_tickers.json'
HEADERS = {'User-Agent': 'SeniorProject sunghun.kim@calvin.edu'}

ADDITIONS = [
    # (company_name, ticker, sector, revenue_model, reason)
    ('Palantir Technologies', 'PLTR', 'AI foundation models', 'Subscription (SaaS)', 'Enterprise AI deployment (Gotham, Foundry, AIP)'),
    ('C3.ai', 'AI', 'AI foundation models', 'Subscription (SaaS)', 'Enterprise AI application platform'),
    ('AppLovin', 'APP', 'AI foundation models', 'Advertising', 'Axon 2 ML advertising engine'),
    ('BigBear.ai', 'BBAI', 'AI foundation models', 'Licensing (enterprise software)', 'Government AI analytics'),
    ('Veritone', 'VERI', 'AI foundation models', 'Subscription (SaaS)', 'aiWARE enterprise AI platform'),
    ('Pegasystems', 'PEGA', 'AI foundation models', 'Licensing (enterprise software)', 'AI workflow automation'),
    ('UiPath', 'PATH', 'AI foundation models', 'Subscription (SaaS)', 'RPA + agentic AI'),
    ('Cerence', 'CRNC', 'AI foundation models', 'Licensing (enterprise software)', 'Voice AI for automotive'),
    ('SoundHound AI', 'SOUN', 'AI foundation models', 'Usage-based metered billing (cloud, APIs)', 'Voice AI, reclassify from AI assistants'),
    ('Tempus AI', 'TEM', 'AI foundation models', 'Subscription (SaaS)', 'AI-driven precision medicine'),
    ('Recursion Pharmaceuticals', 'RXRX', 'AI foundation models', 'Licensing (enterprise software)', 'AI drug discovery'),
    ('Symbotic', 'SYM', 'AI foundation models', 'Transaction fees / marketplace cut', 'AI warehouse robotics'),
    ('Upstart', 'UPST', 'AI foundation models', 'Transaction fees / marketplace cut', 'AI-native lending'),
]


def load_tickers():
    if Path(TICKERS_CACHE).exists():
        with open(TICKERS_CACHE) as f:
            return json.load(f)
    url = 'https://www.sec.gov/files/company_tickers.json'
    req = urllib.request.Request(url, headers=HEADERS)
    data = json.loads(urllib.request.urlopen(req).read())
    with open(TICKERS_CACHE, 'w') as f:
        json.dump(data, f)
    return data


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
        # Check if already in DB
        cur.execute('SELECT id FROM companies_deduped WHERE LOWER(name) = LOWER(?)', (name,))
        row = cur.fetchone()
        if row:
            company_id = row[0]
            print(f'[SKIP-EXISTS] {name} already in DB as id={company_id}')
        else:
            cur.execute('''INSERT INTO companies_deduped (name, hub, state, company_size, employees_count)
                           VALUES (?, ?, ?, ?, ?)''', (name, 'Unknown', 'Unknown', 'Large', None))
            company_id = cur.lastrowid
            print(f'[INSERT] {name} -> id={company_id}')

        cik, title = find_cik(ticker, tickers)
        if not cik:
            missing.append((name, ticker))
            print(f'  [NO-CIK] {ticker} not found in company_tickers.json')
            continue

        # Insert/update CIK mapping
        cur.execute('SELECT cik FROM sec_cik_map WHERE company_id = ?', (company_id,))
        existing = cur.fetchone()
        if existing:
            if existing[0] != cik:
                cur.execute('UPDATE sec_cik_map SET cik = ?, matched_name = ? WHERE company_id = ?', (cik, title, company_id))
                print(f'  [CIK-UPDATE] {existing[0]} -> {cik} ({title})')
            else:
                print(f'  [CIK-OK] {cik} ({title})')
        else:
            cur.execute('INSERT INTO sec_cik_map (company_id, cik, matched_name) VALUES (?, ?, ?)',
                        (company_id, cik, title))
            print(f'  [CIK-INSERT] {cik} ({title})')

        # Insert/update classification
        cur.execute('SELECT sector FROM company_classifications WHERE company_id = ?', (company_id,))
        cls = cur.fetchone()
        if cls:
            cur.execute('UPDATE company_classifications SET sector = ?, revenue_model = ? WHERE company_id = ?',
                        (sector, revmodel, company_id))
            print(f'  [CLASS-UPDATE] {cls[0]} -> {sector}')
        else:
            cur.execute('INSERT INTO company_classifications (company_id, sector, revenue_model) VALUES (?, ?, ?)',
                        (company_id, sector, revmodel))
            print(f'  [CLASS-INSERT] {sector}')

        added.append((company_id, cik, name))

    conn.commit()
    conn.close()

    print(f'\n=== Summary ===')
    print(f'Added/updated: {len(added)}')
    print(f'Missing CIK: {len(missing)}')
    if missing:
        for n, t in missing:
            print(f'  {n} ({t})')
    print(f'\nNext: run Phase F (OCF) + Phase F2 (revenue/R&D) to enrich {len(added)} companies')


if __name__ == '__main__':
    main()

"""
LLM Classification using Claude Haiku
Classifies each company into:
  - sector (professor's 16-category taxonomy)
  - revenue_model (5 categories)

Output: company_classifications table in data/companies.db
"""

import json
import sqlite3
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import anthropic

DB_FILE = Path(__file__).parent.parent / "data" / "companies.db"

SECTORS = [
    "Advertising & attention",
    "Subscription content",
    "Search engines",
    "Productivity & collaboration",
    "Developer tooling",
    "Creative & design tools",
    "AI assistants & copilots",
    "E-learning & skill platforms",
    "Enterprise / ERP / HRM",
    "Fintech & payments",
    "Cybersecurity & identity",
    "GovTech / RegTech / MedTech",
    "Marketplaces & gig platforms",
    "Smartphones & OS",
    "E-commerce platforms",
    "AI foundation models",
    "Gaming & virtual environments",
]

REVENUE_MODELS = [
    "Subscription (SaaS)",
    "Advertising",
    "Transaction fees / marketplace cut",
    "Usage-based metered billing (cloud, APIs)",
    "Licensing (enterprise software)",
]

SYSTEM_PROMPT = f"""You are a tech industry analyst. Classify companies into exactly one sector and one revenue model.

Sectors:
{chr(10).join(f"- {s}" for s in SECTORS)}

Revenue models:
{chr(10).join(f"- {r}" for r in REVENUE_MODELS)}

Respond ONLY with valid JSON in this exact format:
{{"sector": "<sector>", "revenue_model": "<revenue_model>"}}

Use only the exact strings listed above. No explanation."""


def classify_company(client, name: str, description: str, builtin_sectors: str) -> dict | None:
    prompt = f"""Company: {name}
Description: {description or "N/A"}
Industry tags: {builtin_sectors or "N/A"}"""

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        if not text:
            print(f"    EMPTY RESPONSE (stop_reason: {msg.stop_reason})")
            return None
        # Strip markdown code block if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except anthropic.RateLimitError:
        print(f"    RATE LIMIT (429)")
        return None
    except Exception as e:
        print(f"    ERROR: {e}")
        return None


def setup_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_classifications (
            company_id    INTEGER PRIMARY KEY,
            sector        TEXT,
            revenue_model TEXT
        )
    """)


def main():
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    setup_table(cur)
    conn.commit()

    # Get already classified
    done = {r[0] for r in cur.execute("SELECT company_id FROM company_classifications")}

    companies = cur.execute("""
        SELECT id, name, description, sectors
        FROM companies_deduped
        ORDER BY id
    """).fetchall()

    remaining = [(id_, n, d, s) for id_, n, d, s in companies if id_ not in done]
    print(f"Total: {len(companies)} | Already done: {len(done)} | Remaining: {len(remaining)}\n")

    client = anthropic.Anthropic()

    def fuzzy_match(val, options):
        if not val:
            return None
        if val in options:
            return val
        for opt in options:
            if val.lower() in opt.lower() or opt.lower().startswith(val.lower()):
                return opt
        return None

    for i, (company_id, name, description, sectors) in enumerate(remaining):
        result = None
        for attempt in range(5):
            result = classify_company(client, name, description, sectors)
            if result:
                break
            wait = 2 + 2 ** attempt
            print(f"    rate limit, waiting {wait}s...")
            time.sleep(wait)

        if result:
            result["sector"] = fuzzy_match(result.get("sector"), SECTORS) or result.get("sector")
            result["revenue_model"] = fuzzy_match(result.get("revenue_model"), REVENUE_MODELS) or result.get("revenue_model")

        if result and result.get("sector") in SECTORS and result.get("revenue_model") in REVENUE_MODELS:
            cur.execute(
                "INSERT OR REPLACE INTO company_classifications VALUES (?, ?, ?)",
                (company_id, result["sector"], result["revenue_model"])
            )
            if i % 20 == 0:
                conn.commit()
            print(f"  [{i+1}/{len(remaining)}] {name} → {result['sector']} | {result['revenue_model']}")
        else:
            print(f"  [{i+1}/{len(remaining)}] {name} → INVALID: {result}")

        time.sleep(1.5)  # 50 req/min = 1 req per 1.2s, 1.5s to be safe

    conn.commit()

    # Summary
    print("\n--- Sector distribution ---")
    for row in cur.execute("SELECT sector, COUNT(*) FROM company_classifications GROUP BY sector ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]:<40} {row[1]}")

    print("\n--- Revenue model distribution ---")
    for row in cur.execute("SELECT revenue_model, COUNT(*) FROM company_classifications GROUP BY revenue_model ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]:<45} {row[1]}")

    conn.close()


if __name__ == "__main__":
    main()

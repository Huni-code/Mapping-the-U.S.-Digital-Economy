"""
Built In Michigan Company Scraper
Scrapes builtin.com/michigan for tech company listings.
Output: data/companies_raw.csv
"""

import asyncio
import csv
import random
from pathlib import Path
from playwright.async_api import async_playwright

BASE_URL    = "https://builtin.com/michigan"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "companies_raw.csv"
FIELDS      = ["name", "builtin_url", "sectors", "location", "employees", "description"]


async def scrape_page(page, page_num: int) -> list[dict]:
    url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    await page.wait_for_timeout(random.randint(2000, 4000))

    return await page.evaluate("""
        () => {
            const cards = document.querySelectorAll('.company-card-horizontal:not(.placeholder-wave)');
            return Array.from(cards).map(card => {
                const nameEl  = card.querySelector('h2 a');
                const secEl   = card.querySelector('.company-info-section .text-gray-04');
                const spans   = card.querySelectorAll('.company-stats-grid span.text-gray-03');
                const descEl  = card.querySelector('.company-tagline-4-rows p');

                return {
                    name:        nameEl ? nameEl.innerText.trim() : '',
                    builtin_url: nameEl ? 'https://builtin.com' + new URL(nameEl.href).pathname : '',
                    sectors:     secEl  ? secEl.innerText.trim() : '',
                    location:    spans[0] ? spans[0].innerText.trim() : '',
                    employees:   spans[1] ? spans[1].innerText.trim() : '',
                    description: descEl  ? descEl.innerText.trim() : '',
                };
            }).filter(c => c.name);
        }
    """)


async def has_next_page(page, current_page: int) -> bool:
    btn = await page.query_selector(
        f'a[aria-label="Go to page {current_page + 1}"], a[href*="page={current_page + 1}"]'
    )
    return btn is not None


async def main():
    OUTPUT_FILE.parent.mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        all_companies = []
        page_num = 1

        while True:
            print(f"Scraping page {page_num}...", end=" ", flush=True)
            companies = await scrape_page(page, page_num)
            print(f"{len(companies)} companies")

            if not companies:
                break

            all_companies.extend(companies)

            if not await has_next_page(page, page_num):
                break

            page_num += 1
            await asyncio.sleep(random.uniform(2.0, 4.0))

        await browser.close()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(all_companies)

    print(f"\nDone! {len(all_companies)} companies -> {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())

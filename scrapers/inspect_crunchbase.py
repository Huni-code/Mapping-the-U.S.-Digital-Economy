"""
Crunchbase page inspector - saves HTML and screenshot for selector analysis
"""
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

URL = "https://www.crunchbase.com/organization/datadog"
OUT_HTML = Path(__file__).parent.parent / "data" / "debug_crunchbase.html"
OUT_PNG  = Path(__file__).parent.parent / "data" / "debug_crunchbase.png"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=False so you can see + log in
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        print(f"Opening {URL} ...")
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        print("Waiting 10s for JS to load (log in manually if needed)...")
        await page.wait_for_timeout(10000)

        html = await page.content()
        OUT_HTML.write_text(html, encoding="utf-8")
        await page.screenshot(path=str(OUT_PNG), full_page=False)
        print(f"Saved HTML -> {OUT_HTML}")
        print(f"Saved PNG  -> {OUT_PNG}")

        await browser.close()

asyncio.run(main())

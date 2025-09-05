# status_api.py

import os
import asyncio
import json
import redis
from playwright.async_api import async_playwright
from loguru import logger

# Setup logging
logger.add("logs/app.log", rotation="5 MB", retention="7 days", level="INFO")

# Redis setup
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))
REDIS_KEY = "Po_status"

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

class ScraperConfig:
    email =  os.getenv("ERP_USERNAME")
    password = os.getenv("ERP_PASSWORD")
    base_url = (os.getenv("ERP_LOGIN_URL"))
    max_pages = 5
    page_load_timeout = 15000
    navigation_timeout = 20000
    sleep_interval = 5.0

class POScraper:
    def __init__(self, config):
        self.config = config
        self.records = []

    async def _login(self, page):
        await page.goto(self.config.base_url)
        await asyncio.sleep(self.config.sleep_interval)
        await page.fill('input[name="usernameField"]', self.config.email)
        await page.fill('input[name="passwordField"]', self.config.password)
        await page.press('input[name="passwordField"]', 'Enter')
        await asyncio.sleep(self.config.sleep_interval)

    async def _navigate_to_orders(self, page):
        await page.click("img[title='Expand']")
        await page.wait_for_selector("li >> text=Home Page", timeout=self.config.page_load_timeout)
        await page.click("li >> text=Home Page")
        await page.wait_for_selector("a:has-text('Orders')", timeout=self.config.page_load_timeout)
        await page.click("a:has-text('Orders')")
        await page.wait_for_selector("span#ResultRN1", timeout=self.config.navigation_timeout)

    async def _scrape_page(self, page):
        await page.wait_for_selector("span#ResultRN1 table tbody tr", timeout=self.config.page_load_timeout)
        rows = await page.query_selector_all("span#ResultRN1 table tbody tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) >= 13:
                po_text = (await cells[0].inner_text()).strip()
                if po_text and "Previous" not in po_text and "Next" not in po_text:
                    self.records.append({
                        "po_number": po_text,
                        "status": (await cells[12].inner_text()).strip()
                    })

    async def scrape_data(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await self._login(page)
            await self._navigate_to_orders(page)

            for _ in range(self.config.max_pages):
                await self._scrape_page(page)
                next_button = await page.query_selector("a:has-text('Next')")
                if next_button:
                    class_attr = await next_button.get_attribute("class") or ""
                    if "disabled" not in class_attr.lower():
                        await next_button.click()
                        await asyncio.sleep(self.config.sleep_interval)
                    else:
                        break
                else:
                    break

            await browser.close()
            return {"status": "success", "records": self.records}

# 🟢 Scheduled job to run every 15 minutes
def scrape_and_store_in_redis():
    try:
        config = ScraperConfig()
        scraper = POScraper(config)
        result = asyncio.run(scraper.scrape_data())
        if result.get("status") == "success":
            redis_client.set(REDIS_KEY, json.dumps(result["records"]))
            logger.info(f"Scraped data stored in Redis under key '{REDIS_KEY}'")
        else:
            logger.error(f"Scraper returned error: {result}")
    except Exception as e:
        logger.exception(f"Error in scheduled Redis update: {e}")

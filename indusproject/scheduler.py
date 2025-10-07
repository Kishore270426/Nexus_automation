from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from time import sleep
from .scrapper import scrape_indus_po_data
from .status_scrapper import scrape_and_store_in_redis

scheduler = BackgroundScheduler()

# Indus PO scraping
scheduler.add_job(
    func=scrape_indus_po_data,
    trigger=CronTrigger(hour=13, minute=9),
    id='indus_po_scraper',
    replace_existing=True
)

# Zepto PO scraping
scheduler.add_job(
    func=scrape_and_store_in_redis,
    trigger=CronTrigger(hour=10, minute=38),
    id='scrape_and_store_in_redis',
    replace_existing=True
)

scheduler.start()
print("[Scheduler] Scheduler started")

# Keep the scheduler alive
try:
    while True:
        sleep(60)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    print("[Scheduler] Scheduler stopped")

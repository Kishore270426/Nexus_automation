from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from time import sleep
from .scrapper import scrape_indus_po_data
from .status_scrapper import scrape_and_store_in_redis

# ------------------------------
# Wrapped functions with print statements
# ------------------------------
def wrapped_scrape_indus_po_data():
    print("[Scheduler] Triggered job: scrape_indus_po_data")
    try:
        scrape_indus_po_data()
        print("[Scheduler] Job scrape_indus_po_data completed successfully")
    except Exception as e:
        print(f"[Scheduler] Job scrape_indus_po_data failed: {e}")

def wrapped_scrape_and_store_in_redis():
    print("[Scheduler] Triggered job: scrape_and_store_in_redis")
    try:
        scrape_and_store_in_redis()
        print("[Scheduler] Job scrape_and_store_in_redis completed successfully")
    except Exception as e:
        print(f"[Scheduler] Job scrape_and_store_in_redis failed: {e}")

# ------------------------------
# Scheduler setup
# ------------------------------
scheduler = BackgroundScheduler()

# Indus PO scraping
scheduler.add_job(
    func=wrapped_scrape_indus_po_data,
    trigger=CronTrigger(hour=11, minute=25),
    id='indus_po_scraper',
    replace_existing=True
)

# Zepto PO scraping
scheduler.add_job(
    func=wrapped_scrape_and_store_in_redis,
    trigger=CronTrigger(hour=11, minute=30),
    id='scrape_and_store_in_redis',
    replace_existing=True
)

scheduler.start()
print("[Scheduler] Scheduler started and jobs scheduled")

# ------------------------------
# Keep the scheduler alive
# ------------------------------
try:
    while True:
        sleep(60)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    print("[Scheduler] Scheduler stopped")

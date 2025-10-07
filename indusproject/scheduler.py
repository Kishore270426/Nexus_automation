# indusproject/scheduler.py

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from .scrapper import scrape_indus_po_data
from .status_scrapper import scrape_and_store_in_redis
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler")

def start_scheduler():
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        func=scrape_indus_po_data,
        trigger=CronTrigger(hour=16, minute=30),
        id='indus_po_scraper',
        replace_existing=True
    )

    scheduler.add_job(
        func=scrape_and_store_in_redis,
        trigger=CronTrigger(hour=17, minute=30),
        id='scrape_and_store_in_redis',
        replace_existing=True
    )

    scheduler.start()
    logger.info("Scheduler started with jobs added.")

    # Keep the script running
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped.")

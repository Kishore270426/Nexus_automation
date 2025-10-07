from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta
from .scrapper import scrape_indus_po_data
from .status_scrapper import scrape_and_store_in_redis



def start_scheduler():
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
    print("[Scheduler] Indus PO scraper scheduled at 11:20 (with retry).")
    print("[Scheduler] Zepto PO scraper scheduled at 12:40 (with retry).")

from apscheduler.schedulers.background import BackgroundScheduler
from .scrapper import scrape_indus_po_data
from .status_scrapper import scrape_and_store_in_redis 
from apscheduler.triggers.interval import IntervalTrigger


def start_scheduler():
    scheduler = BackgroundScheduler()

    # Job 1: Indus PO scraping every 15 minutes
    scheduler.add_job(
        scrape_indus_po_data,
        trigger=IntervalTrigger(minutes=5),
        id='indus_po_scraper',
        name='Scrape Indus PO data every 10 minutes',
        replace_existing=True
    )

    # Job 2: Zepto PO scraping every 5 minutes
    scheduler.add_job(
        scrape_and_store_in_redis,
        trigger=IntervalTrigger(minutes=60),
        id='scrape_and_store_in_redis',
        name='Scrape Zepto PO data every 5 minutes',
        replace_existing=True
    )

    scheduler.start()
    print("[Scheduler] Indus PO scraper scheduled every 15 minutes.")
    print("[Scheduler] PO scraper scheduled every 5 minutes.")

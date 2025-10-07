from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis

def start_scheduler():
    scheduler = BlockingScheduler()

    # Indus PO scraping
    scheduler.add_job(
        func=scrape_indus_po_data,
        trigger=CronTrigger(hour=16, minute=0),
        id='indus_po_scraper',
        replace_existing=True
    )

    # Zepto PO scraping
    scheduler.add_job(
        func=scrape_and_store_in_redis,
        trigger=CronTrigger(hour=16, minute=45),
        id='scrape_and_store_in_redis',
        replace_existing=True
    )

    print("[Scheduler] Indus PO scraper scheduled at 4:00 pm")
    print("[Scheduler] Zepto PO scraper scheduled at 5:00 pm")
    
    scheduler.start()  # <-- blocks and keeps service alive

if __name__ == "__main__":
    start_scheduler()

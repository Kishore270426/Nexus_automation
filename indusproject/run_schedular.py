#!/usr/bin/env python
import os
import django
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Setup Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "indusproject.settings")
django.setup()

from scrapper import scrape_indus_po_data
from status_scrapper import scrape_and_store_in_redis

def start_scheduler():
    scheduler = BlockingScheduler()

    # Job 1: Indus PO scraping every 5 minutes
    scheduler.add_job(
        scrape_indus_po_data,
        trigger=IntervalTrigger(minutes=5),
        id='indus_po_scraper',
        name='Scrape Indus PO data every 5 minutes',
        replace_existing=True
    )

    # Job 2: Zepto PO scraping every 60 minutes
    scheduler.add_job(
        scrape_and_store_in_redis,
        trigger=IntervalTrigger(minutes=60),
        id='scrape_and_store_in_redis',
        name='Scrape Zepto PO data every 60 minutes',
        replace_existing=True
    )

    print("[Scheduler] Scheduler started...")
    scheduler.start()


if __name__ == "__main__":
    start_scheduler()

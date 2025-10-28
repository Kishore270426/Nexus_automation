from apscheduler.schedulers.background import BackgroundScheduler
from .scrapper import scrape_indus_po_data
from .status_scrapper import scrape_and_store_in_redis 
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler()

def start_scheduler():
    

    # Job 1: Indus PO scraping every 15 minutes
    scheduler.add_job(
        scrape_indus_po_data,
        trigger=CronTrigger(hour=13, minute=8),
        id='indus_po_scraper',
        name='Scrape Indus PO data every 10 minutes',
        replace_existing=True
    )

    # Job 2: Zepto PO scraping every 5 minutes
    scheduler.add_job(
        scrape_and_store_in_redis,
        trigger=CronTrigger(hour=15, minute=21),
        id='scrape_and_store_in_redis',
        name='Scrape Zepto PO data every 5 minutes',
        replace_existing=True
    )

    scheduler.start()
    print("[Scheduler] Indus PO scraper scheduled every 15 minutes.")
    print("[Scheduler] PO scraper scheduled every 5 minutes.")


# update existing job schedule dynamically
def update_job_schedule(job_id, hour, minute):
    
    try:
        job = scheduler.get_job(job_id)
        if not job:
            return f"No job found with id: {job_id}"

        # Create a new cron trigger with the new time
        new_trigger = CronTrigger(hour=hour, minute=minute)
        job.reschedule(trigger=new_trigger)

        print(f"[Scheduler] Updated {job_id} to run at {hour}:{minute}")
        return f"Updated {job_id} to run at {hour}:{minute}"
    except Exception as e:
        print(f"[Scheduler Error] {str(e)}")
        return f"Error updating job: {str(e)}"

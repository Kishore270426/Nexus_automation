# indusproject/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis

# -------------------- GLOBAL SCHEDULER INSTANCE --------------------
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")


# -------------------- JOB DEFINITIONS --------------------
def add_jobs():
    """Add scheduled jobs if not already added."""
    if not scheduler.get_job('indus_po_scraper'):
        scheduler.add_job(
            scrape_indus_po_data,
            trigger=CronTrigger(hour=13, minute=8),
            id='indus_po_scraper',
            name='Scrape Indus PO data',
            replace_existing=True
        )
        print("[Scheduler] Added job: indus_po_scraper")

    if not scheduler.get_job('scrape_and_store_in_redis'):
        scheduler.add_job(
            scrape_and_store_in_redis,
            trigger=CronTrigger(hour=15, minute=21),
            id='scrape_and_store_in_redis',
            name='Scrape Zepto PO data',
            replace_existing=True
        )
        print("[Scheduler] Added job: scrape_and_store_in_redis")

    print(f"[Scheduler] Current jobs: {scheduler.get_jobs()}")


# -------------------- UPDATE JOB SCHEDULE --------------------
def update_job_schedule(job_id: str, hour: int, minute: int):
    """Update job trigger time dynamically."""
    try:
        job = scheduler.get_job(job_id)
        if not job:
            return False, f"No job found with id: {job_id}"

        new_trigger = CronTrigger(hour=hour, minute=minute)
        job.reschedule(trigger=new_trigger)
        print(f"[Scheduler] Job '{job_id}' rescheduled to {hour:02d}:{minute:02d}")
        return True, f"Updated job '{job_id}' to run at {hour:02d}:{minute:02d}"

    except JobLookupError:
        return False, f"Job with id '{job_id}' not found"
    except Exception as e:
        return False, f"Error updating job '{job_id}': {str(e)}"


# -------------------- START SCHEDULER --------------------
def start_scheduler():
    """Start background scheduler safely."""
    add_jobs()
    if not scheduler.running:
        scheduler.start()
        print("[Scheduler] Background scheduler started")


# -------------------- STANDALONE MODE --------------------
if __name__ == "__main__":
    from time import sleep
    print("[Scheduler] Running standalone scheduler for testing...")
    start_scheduler()

    try:
        while True:
            sleep(10)
    except (KeyboardInterrupt, SystemExit):
        print("[Scheduler] Stopped manually")

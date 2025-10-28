# scheduler.py
import os
import django
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis

# -------------------- Django setup --------------------
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'indusproject.settings')
django.setup()

# -------------------- Scheduler setup --------------------
scheduler = BlockingScheduler()

# -------------------- Add Jobs --------------------
scheduler.add_job(
    scrape_indus_po_data,
    trigger=CronTrigger(hour=13, minute=8),
    id='indus_po_scraper',
    name='Scrape Indus PO data',
    replace_existing=True
)

scheduler.add_job(
    scrape_and_store_in_redis,
    trigger=CronTrigger(hour=15, minute=21),
    id='scrape_and_store_in_redis',
    name='Scrape Zepto PO data',
    replace_existing=True
)

# -------------------- Function to update job times dynamically --------------------
def update_job_schedule(job_id: str, hour: int, minute: int):
    """
    Update the schedule time of an existing job.
    
    Args:
        job_id: ID of the job to update
        hour: New hour (0-23)
        minute: New minute (0-59)
    Returns:
        Status message
    """
    try:
        job = scheduler.get_job(job_id)
        if not job:
            return f"No job found with id: {job_id}"

        # Create a new CronTrigger with the updated time
        new_trigger = CronTrigger(hour=hour, minute=minute)
        job.reschedule(trigger=new_trigger)

        print(f"[Scheduler] Updated job '{job_id}' to run at {hour:02d}:{minute:02d}")
        return f"Updated job '{job_id}' to run at {hour:02d}:{minute:02d}"
    except Exception as e:
        print(f"[Scheduler Error] {str(e)}")
        return f"Error updating job '{job_id}': {str(e)}"

# -------------------- Start Scheduler --------------------
if __name__ == "__main__":
    print("[Scheduler] Starting APScheduler...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[Scheduler] Stopped manually")

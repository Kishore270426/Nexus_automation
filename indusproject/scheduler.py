from apscheduler.schedulers.blocking import BlockingScheduler
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis
from apscheduler.triggers.cron import CronTrigger
import os
import django

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'indusproject.settings')
django.setup()

# Use BlockingScheduler so the process stays alive
scheduler = BlockingScheduler()

# Define jobs
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

# Optional: function to dynamically update jobs
def update_job_schedule(job_id, hour, minute):
    try:
        job = scheduler.get_job(job_id)
        if not job:
            return f"No job found with id: {job_id}"
        job.reschedule(trigger=CronTrigger(hour=hour, minute=minute))
        print(f"[Scheduler] Updated {job_id} to run at {hour}:{minute}")
        return f"Updated {job_id} to run at {hour}:{minute}"
    except Exception as e:
        print(f"[Scheduler Error] {str(e)}")
        return f"Error updating job: {str(e)}"

# Run scheduler
if __name__ == "__main__":
    print("[Scheduler] Starting scheduler...")
    scheduler.start()

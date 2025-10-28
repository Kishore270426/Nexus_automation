# indusproject/scheduler.py
import os, json
from time import sleep
from redis import Redis
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis
from dotenv import load_dotenv

load_dotenv()

# -------------------- Redis --------------------
redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB"))
)

# Redis keys for job times
JOB_TIME_KEY = "scheduler_job_times"

# -------------------- Scheduler --------------------
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

JOB_FUNCTIONS = {
    "indus_po_scraper": scrape_indus_po_data,
    "scrape_and_store_in_redis": scrape_and_store_in_redis
}

# -------------------- Helper Functions --------------------
def get_job_times():
    """Fetch job times from Redis. Use default if not set."""
    default_times = {
        "indus_po_scraper": {"hour": 13, "minute": 8},
        "scrape_and_store_in_redis": {"hour": 15, "minute": 21}
    }
    try:
        data = redis_client.get(JOB_TIME_KEY)
        if data:
            return json.loads(data)
    except Exception as e:
        print("[Scheduler] Error reading job times from Redis:", e)
    return default_times

def add_jobs():
    """Add jobs dynamically from Redis values."""
    job_times = get_job_times()
    for job_id, func in JOB_FUNCTIONS.items():
        time = job_times.get(job_id, {})
        hour = time.get("hour", 0)
        minute = time.get("minute", 0)

        if not scheduler.get_job(job_id):
            scheduler.add_job(
                func,
                trigger=CronTrigger(hour=hour, minute=minute),
                id=job_id,
                name=f"Job: {job_id}",
                replace_existing=True
            )
            print(f"[Scheduler] Added job: {job_id} at {hour:02d}:{minute:02d}")

def start_scheduler():
    """Start the scheduler safely."""
    add_jobs()
    if not scheduler.running:
        scheduler.start()
        print("[Scheduler] Background scheduler started")

def update_job_schedule(job_id: str, hour: int, minute: int):
    """Update job time in Redis, scheduler will reschedule automatically."""
    try:
        # Update Redis
        job_times = get_job_times()
        job_times[job_id] = {"hour": hour, "minute": minute}
        redis_client.set(JOB_TIME_KEY, json.dumps(job_times))

        # Update in-memory job if scheduler is running
        job = scheduler.get_job(job_id)
        if job:
            job.reschedule(trigger=CronTrigger(hour=hour, minute=minute))
            print(f"[Scheduler] Job '{job_id}' rescheduled to {hour:02d}:{minute:02d}")

        return True, f"Updated job '{job_id}' to run at {hour:02d}:{minute:02d}"

    except Exception as e:
        return False, str(e)

# -------------------- Standalone --------------------
if __name__ == "__main__":
    print("[Scheduler] Starting standalone scheduler...")
    start_scheduler()
    try:
        while True:
            sleep(10)
    except (KeyboardInterrupt, SystemExit):
        print("[Scheduler] Stopped manually")

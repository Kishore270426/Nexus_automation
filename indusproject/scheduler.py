# indusproject/scheduler.py
import os
import json
from redis import Redis
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis
from dotenv import load_dotenv
import logging

load_dotenv()

# -------------------- Logging --------------------
LOG_FILE = "/home/ubuntu/Nexus_automation/logs/scheduler_job.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------- Redis --------------------
redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB"))
)

JOB_TIME_KEY = "scheduler_job_times"

# -------------------- Scheduler --------------------
scheduler = BlockingScheduler(timezone="Asia/Kolkata")

JOB_FUNCTIONS = {
    "indus_po_scraper": scrape_indus_po_data,
    "scrape_and_store_in_redis": scrape_and_store_in_redis
}

# -------------------- Helper Functions --------------------
def get_job_times():
    default_times = {
        "indus_po_scraper": {"hour": 16, "minute": 55},
        "scrape_and_store_in_redis": {"hour": 15, "minute": 21}
    }
    try:
        data = redis_client.get(JOB_TIME_KEY)
        if data:
            return json.loads(data)
    except Exception as e:
        logging.exception(f"Error reading job times from Redis: {e}")
    return default_times

def job_wrapper(func, job_id):
    logging.info(f"Job '{job_id}' started")
    try:
        func()
        logging.info(f"Job '{job_id}' finished successfully")
    except Exception as e:
        logging.exception(f"Job '{job_id}' failed: {e}")
    # flush logs immediately
    logging.getLogger().handlers[0].flush()

def add_jobs():
    job_times = get_job_times()
    for job_id, func in JOB_FUNCTIONS.items():
        time = job_times.get(job_id, {})
        hour = time.get("hour", 0)
        minute = time.get("minute", 0)

        if not scheduler.get_job(job_id):
            scheduler.add_job(
                job_wrapper,
                trigger=CronTrigger(hour=hour, minute=minute),
                args=[func, job_id],
                id=job_id,
                name=f"Job: {job_id}",
                replace_existing=True
            )
            logging.info(f"Added job '{job_id}' at {hour:02d}:{minute:02d}")
            logging.getLogger().handlers[0].flush()

def update_job_schedule(job_id: str, hour: int, minute: int):
    """
    Update job time in Redis and reschedule in APScheduler.
    Logs the update immediately.
    """
    try:
        # Get old time
        job_times = get_job_times()
        old_time = job_times.get(job_id, {"hour": None, "minute": None})

        # Update Redis
        job_times[job_id] = {"hour": hour, "minute": minute}
        redis_client.set(JOB_TIME_KEY, json.dumps(job_times))

        # Update in-memory job if scheduler is running
        job = scheduler.get_job(job_id)
        if job:
            job.reschedule(trigger=CronTrigger(hour=hour, minute=minute))
            logging.info(
                f"Job '{job_id}' rescheduled from "
                f"{old_time['hour']:02d}:{old_time['minute']:02d} "
                f"to {hour:02d}:{minute:02d}"
            )
        else:
            logging.warning(f"Job '{job_id}' not found in scheduler while updating")

        logging.getLogger().handlers[0].flush()
        return True, f"Updated job '{job_id}' to run at {hour:02d}:{minute:02d}"

    except Exception as e:
        logging.exception(f"Failed to update job '{job_id}': {e}")
        logging.getLogger().handlers[0].flush()
        return False, str(e)

# -------------------- Standalone --------------------
if __name__ == "__main__":
    logging.info("Starting standalone scheduler...")
    add_jobs()
    scheduler.start()  # Blocking call, systemd keeps service alive

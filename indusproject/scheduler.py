# indusproject/scheduler.py
import os
import json
import sys
from datetime import datetime
from redis import Redis
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_SCHEDULER_STARTED, EVENT_SCHEDULER_SHUTDOWN
from indusproject.scrapper import scrape_indus_po_data
from indusproject.status_scrapper import scrape_and_store_in_redis
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

load_dotenv()

# -------------------- Production-Level Logging --------------------
# Cross-platform log directory (works on Windows and Linux)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logger with both file and console handlers
logger = logging.getLogger('scheduler')
logger.setLevel(logging.INFO)

# File handler with rotation
file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, 'scheduler.log'),
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'  # Ensure UTF-8 encoding
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Console handler for real-time monitoring (UTF-8 compatible)
try:
    import io
    console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8'))
except (AttributeError, io.UnsupportedOperation):
    # Fallback for systems without buffer access
    console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("="*80)
logger.info("SCHEDULER MODULE LOADED")
logger.info(f"Python executable: {sys.executable}")
logger.info(f"Current working directory: {os.getcwd()}")
logger.info("="*80)
# -------------------- Redis --------------------
try:
    redis_client = Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB"))
    )
    # Test connection
    redis_client.ping()
    logger.info(f"Successfully connected to Redis at {os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT')}")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {str(e)}", exc_info=True)
    sys.exit(1)

JOB_TIME_KEY = "scheduler_job_times"
SCHEDULER_STATUS_KEY = "scheduler_status"

# -------------------- Scheduler --------------------
scheduler = BlockingScheduler(timezone="Asia/Kolkata")
logger.info("BlockingScheduler initialized with timezone: Asia/Kolkata")

JOB_FUNCTIONS = {
    "indus_po_scraper": scrape_indus_po_data,
    "scrape_and_store_in_redis": scrape_and_store_in_redis
}

# -------------------- Helper Functions --------------------
def update_scheduler_status(status, message=""):
    """Update scheduler status in Redis for monitoring"""
    try:
        status_data = {
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "pid": os.getpid()
        }
        redis_client.set(SCHEDULER_STATUS_KEY, json.dumps(status_data))
        logger.info(f"Scheduler status updated: {status} - {message}")
    except Exception as e:
        logger.error(f"Failed to update scheduler status: {str(e)}")

def get_job_times():
    default_times = {
        "indus_po_scraper": {"hour": 16, "minute": 55},
        "scrape_and_store_in_redis": {"hour": 15, "minute": 21}
    }
    try:
        data = redis_client.get(JOB_TIME_KEY)
        if data:
            job_times = json.loads(data)
            logger.info(f"Retrieved job times from Redis: {job_times}")
            return job_times
        else:
            logger.info(f"No job times found in Redis, using defaults: {default_times}")
    except Exception as e:
        logger.exception(f"Error reading job times from Redis: {e}")
    return default_times

def job_wrapper(func, job_id):
    logger.info(f"{'='*60}")
    logger.info(f"JOB STARTED: '{job_id}' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}")
    start_time = datetime.now()
    
    try:
        update_scheduler_status("running", f"Executing job: {job_id}")
        func()
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"{'='*60}")
        logger.info(f"JOB COMPLETED: '{job_id}' - Duration: {duration:.2f}s")
        logger.info(f"{'='*60}")
        update_scheduler_status("idle", f"Last job: {job_id} completed successfully")
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"{'='*60}")
        logger.exception(f"JOB FAILED: '{job_id}' after {duration:.2f}s - Error: {e}")
        logger.error(f"{'='*60}")
        update_scheduler_status("error", f"Job {job_id} failed: {str(e)}")

def scheduler_listener(event):
    """Listen to scheduler events for better monitoring"""
    if event.code == EVENT_JOB_EXECUTED:
        logger.info(f"Event: Job {event.job_id} executed successfully")
    elif event.code == EVENT_JOB_ERROR:
        logger.error(f"Event: Job {event.job_id} raised an exception")
    elif event.code == EVENT_SCHEDULER_STARTED:
        logger.info("Event: Scheduler started successfully")
        update_scheduler_status("started", "Scheduler is now running")
    elif event.code == EVENT_SCHEDULER_SHUTDOWN:
        logger.warning("Event: Scheduler shut down")
        update_scheduler_status("stopped", "Scheduler has been shut down")

def add_jobs():
    logger.info("="*80)
    logger.info("ADDING JOBS TO SCHEDULER")
    logger.info("="*80)
    
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
            logger.info(f"[OK] Job '{job_id}' scheduled at {hour:02d}:{minute:02d} IST (Asia/Kolkata)")
        else:
            logger.warning(f"Job '{job_id}' already exists in scheduler")
    
    # Log all scheduled jobs
    logger.info("="*80)
    logger.info("CURRENT SCHEDULED JOBS:")
    jobs = scheduler.get_jobs()
    if jobs:
        for job in jobs:
            # Get next run time safely from trigger
            try:
                next_run = job.trigger.get_next_fire_time(None, datetime.now())
                logger.info(f"  - {job.id}: Next run at {next_run}")
            except Exception as e:
                logger.warning(f"  - {job.id}: Cannot determine next run time - {e}")
    else:
        logger.warning("No jobs scheduled!")
    logger.info("="*80)

def update_job_schedule(job_id: str, hour: int, minute: int):
    """
    Update job time in Redis and reschedule in APScheduler.
    Logs the update immediately.
    """
    logger.info(f"Attempting to update job '{job_id}' to {hour:02d}:{minute:02d}")
    
    try:
        # Get old time
        job_times = get_job_times()
        old_time = job_times.get(job_id, {"hour": None, "minute": None})

        # Update Redis
        job_times[job_id] = {"hour": hour, "minute": minute}
        redis_client.set(JOB_TIME_KEY, json.dumps(job_times))
        logger.info(f"Updated job time in Redis for '{job_id}'")

        # Update in-memory job if scheduler is running
        job = scheduler.get_job(job_id)
        if job:
            job.reschedule(trigger=CronTrigger(hour=hour, minute=minute))
            logger.info(
                f"[OK] Job '{job_id}' rescheduled from "
                f"{old_time['hour']:02d}:{old_time['minute']:02d} "
                f"to {hour:02d}:{minute:02d}"
            )
            # Get next run time safely from trigger
            try:
                next_run = job.trigger.get_next_fire_time(None, datetime.now())
                logger.info(f"  Next run: {next_run}")
            except Exception:
                logger.info(f"  Job will run at {hour:02d}:{minute:02d} daily")
        else:
            logger.warning(f"Job '{job_id}' not found in scheduler while updating")

        return True, f"Updated job '{job_id}' to run at {hour:02d}:{minute:02d}"

    except Exception as e:
        logger.exception(f"Failed to update job '{job_id}': {e}")
        return False, str(e)

def get_scheduler_status():
    """Get current scheduler status from Redis"""
    try:
        status_data = redis_client.get(SCHEDULER_STATUS_KEY)
        if status_data:
            return json.loads(status_data)
        return {"status": "unknown", "message": "No status data available"}
    except Exception as e:
        logger.error(f"Error reading scheduler status: {str(e)}")
        return {"status": "error", "message": str(e)}

# -------------------- Standalone --------------------
if __name__ == "__main__":
    try:
        logger.info("="*80)
        logger.info("STARTING SCHEDULER SERVICE")
        logger.info(f"Process ID: {os.getpid()}")
        logger.info(f"Timezone: Asia/Kolkata")
        logger.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        # Add event listener
        scheduler.add_listener(scheduler_listener, 
                             EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | 
                             EVENT_SCHEDULER_STARTED | EVENT_SCHEDULER_SHUTDOWN)
        
        # Add jobs
        add_jobs()
        
        # Update status
        update_scheduler_status("starting", "Scheduler is starting...")
        
        logger.info("="*80)
        logger.info("SCHEDULER IS NOW RUNNING - Press Ctrl+C to stop")
        logger.info("="*80)
        
        # Start scheduler (blocking call)
        scheduler.start()
        
    except KeyboardInterrupt:
        logger.info("="*80)
        logger.warning("Scheduler stopped by user (Ctrl+C)")
        logger.info("="*80)
        update_scheduler_status("stopped", "Stopped by user interrupt")
        
    except Exception as e:
        logger.error("="*80)
        logger.exception(f"Scheduler crashed with error: {e}")
        logger.error("="*80)
        update_scheduler_status("crashed", f"Error: {str(e)}")
        raise
        
    finally:
        logger.info("Scheduler service terminated")

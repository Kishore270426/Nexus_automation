"""
Quick script to check if the scheduler is running
Run this from the command line to verify scheduler status
"""
import os
import json
from redis import Redis
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def check_scheduler_status():
    print("="*80)
    print("SCHEDULER STATUS CHECK")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    try:
        # Connect to Redis
        redis_client = Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            db=int(os.getenv("REDIS_DB"))
        )
        print("✓ Connected to Redis successfully")
        
        # Check scheduler status
        status_data = redis_client.get("scheduler_status")
        if status_data:
            status = json.loads(status_data)
            print("\n📊 SCHEDULER STATUS:")
            print(f"  Status: {status.get('status', 'unknown')}")
            print(f"  Message: {status.get('message', 'N/A')}")
            print(f"  Last Update: {status.get('timestamp', 'N/A')}")
            print(f"  Process ID: {status.get('pid', 'N/A')}")
            
            # Check if status is recent (within last 5 minutes)
            if status.get('timestamp'):
                last_update = datetime.fromisoformat(status['timestamp'])
                time_diff = (datetime.now() - last_update).total_seconds()
                if time_diff > 300:  # 5 minutes
                    print(f"\n⚠️  WARNING: Status is {time_diff/60:.1f} minutes old - scheduler may not be running!")
                else:
                    print(f"\n✓ Status is recent ({time_diff:.0f} seconds old)")
        else:
            print("\n❌ NO STATUS DATA FOUND")
            print("   Scheduler may not have been started yet")
        
        # Check job times
        job_times_data = redis_client.get("scheduler_job_times")
        if job_times_data:
            job_times = json.loads(job_times_data)
            print("\n📅 SCHEDULED JOB TIMES:")
            for job_id, times in job_times.items():
                print(f"  {job_id}: {times.get('hour', 0):02d}:{times.get('minute', 0):02d} IST")
        else:
            print("\n⚠️  No job times configured")
        
        print("\n" + "="*80)
        print("To start the scheduler, run: python -m indusproject.scheduler")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("\nPlease ensure:")
        print("  1. Redis server is running")
        print("  2. .env file contains correct Redis configuration")
        print("  3. Scheduler has been started at least once")

if __name__ == "__main__":
    check_scheduler_status()

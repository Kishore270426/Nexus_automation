#!/usr/bin/env python3
"""
Quick Scheduler Diagnostics Tool
Run this to get instant status of your scheduler
"""
import os
import sys
import json
from datetime import datetime
from redis import Redis
from dotenv import load_dotenv

load_dotenv()

def print_box(title, content, color=""):
    """Print content in a nice box"""
    width = 78
    colors = {
        "green": "\033[92m",
        "yellow": "\033[93m",
        "red": "\033[91m",
        "blue": "\033[94m",
        "end": "\033[0m"
    }
    
    c = colors.get(color, "")
    end = colors["end"] if color else ""
    
    print(f"\n{c}{'='*width}{end}")
    print(f"{c}{title.center(width)}{end}")
    print(f"{c}{'='*width}{end}")
    for line in content:
        print(f"{c}{line}{end}")
    print(f"{c}{'='*width}{end}\n")

def main():
    print("\n" + "🔍 SCHEDULER DIAGNOSTICS".center(80))
    print(f"{'Time: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n".center(80))
    
    # Check Redis Connection
    try:
        redis_client = Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0))
        )
        redis_client.ping()
        print_box("✅ REDIS CONNECTION", ["Status: CONNECTED", f"Host: {os.getenv('REDIS_HOST')}:{os.getenv('REDIS_PORT')}"], "green")
    except Exception as e:
        print_box("❌ REDIS CONNECTION FAILED", [f"Error: {str(e)}", "Please start Redis server"], "red")
        return
    
    # Check Scheduler Status
    status_key = "scheduler_status"
    status_data = redis_client.get(status_key)
    
    if status_data:
        status = json.loads(status_data)
        scheduler_status = status.get('status', 'unknown').upper()
        
        # Check if status is recent
        if status.get('timestamp'):
            last_update = datetime.fromisoformat(status['timestamp'])
            seconds_ago = (datetime.now() - last_update).total_seconds()
            time_str = f"{seconds_ago:.0f} seconds ago" if seconds_ago < 60 else f"{seconds_ago/60:.1f} minutes ago"
            
            if seconds_ago > 300:  # 5 minutes
                color = "yellow"
                warning = "⚠️  WARNING: Status is old - scheduler may be stopped"
            else:
                color = "green"
                warning = "✅ Status is recent - scheduler is running"
        else:
            time_str = "Unknown"
            color = "yellow"
            warning = "⚠️  No timestamp available"
        
        content = [
            f"Status: {scheduler_status}",
            f"Message: {status.get('message', 'N/A')}",
            f"Last Update: {time_str}",
            f"Process ID: {status.get('pid', 'N/A')}",
            "",
            warning
        ]
        print_box("📊 SCHEDULER STATUS", content, color)
    else:
        print_box("❌ NO SCHEDULER STATUS", [
            "Scheduler has never been started",
            "Or Redis was cleared",
            "",
            "To start: python -m indusproject.scheduler"
        ], "red")
    
    # Check Job Times
    job_times_data = redis_client.get("scheduler_job_times")
    if job_times_data:
        job_times = json.loads(job_times_data)
        content = ["Configured Job Schedule:"]
        for job_id, times in job_times.items():
            content.append(f"  • {job_id}: {times.get('hour', 0):02d}:{times.get('minute', 0):02d} IST")
        print_box("📅 SCHEDULED JOBS", content, "blue")
    else:
        print_box("⚠️  NO JOB SCHEDULE", ["Using default schedule", "Jobs may not be configured"], "yellow")
    
    # Check Log Files
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, 'logs')
    if os.path.exists(log_dir):
        logs = [f for f in os.listdir(log_dir) if f.endswith('.log')]
        if logs:
            content = ["Available log files:"]
            for log in sorted(logs):
                log_path = os.path.join(log_dir, log)
                size = os.path.getsize(log_path) / 1024  # KB
                content.append(f"  • {log} ({size:.1f} KB)")
            print_box("📁 LOG FILES", content, "blue")
    
    # Action Items
    print_box("🔧 QUICK ACTIONS", [
        "View scheduler log:  tail -f logs/scheduler.log",
        "View errors:         tail -f logs/error.log",
        "Start scheduler:     python -m indusproject.scheduler",
        "API status:          curl localhost:8000/api/scheduler-status/ -H 'Authorization: Bearer TOKEN'",
    ], "")
    
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDiagnostics cancelled by user\n")
    except Exception as e:
        print(f"\n❌ Error running diagnostics: {str(e)}\n")
        import traceback
        traceback.print_exc()

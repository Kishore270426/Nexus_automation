from django.apps import AppConfig
from indusproject.scheduler import start_scheduler
import os

class IndusScraperConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'indusapi'

    def ready(self):
        # Prevent double execution by checking for RUN_MAIN
        if os.environ.get('RUN_MAIN') != 'true':
            return
        print("[AppConfig] Starting scheduler")
        
        start_scheduler()

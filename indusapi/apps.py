# indusapi/apps.py
from django.apps import AppConfig
import os
#from indusproject.scheduler import start_scheduler

class IndusapiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'indusapi'

    def ready(self):
        # Avoid running scheduler twice in Django's autoreload
        #if os.environ.get('RUN_MAIN') != 'true':
        #   return
        #print("[AppConfig] Starting scheduler inside Django...")
        #start_scheduler()
        pass
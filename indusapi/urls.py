# indus_api/urls.py
from django.urls import path
from .views import get_po_data, bulk_scrape, update_erp_password, update_cron_time

urlpatterns = [
    path('api/po-data/', get_po_data),
    path('api/po-status/', bulk_scrape),
    path('api/update-password/', update_erp_password, name='update_erp_password'),
    path('api/update-time/', update_cron_time, name='update_cron_time'),
]

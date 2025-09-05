# indus_api/urls.py
from django.urls import path
from .views import get_po_data, bulk_scrape, change_credentials

urlpatterns = [
    path('api/po-data/', get_po_data),
    path('api/po-status/', bulk_scrape),
    path("change-credentials/", change_credentials),
]

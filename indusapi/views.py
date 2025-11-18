# indus_api/views.py
from rest_framework.decorators import api_view, permission_classes, authentication_classes
from rest_framework.response import Response
import os, json
from redis import Redis
from dotenv import load_dotenv
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from rest_framework.permissions import IsAuthenticated
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.contrib.auth.password_validation import validate_password
from rest_framework import status
from indusproject.scheduler import update_job_schedule
from .utils import token_required




load_dotenv()
redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB"))
)

@api_view(['POST'])
@authentication_classes([])  # Disable default DRF auth for this endpoint
@permission_classes([]) 
@token_required
def get_po_data(request):

    try:
        data = redis_client.get("indus_latest_data")
        if data:
            records = json.loads(data)
            return Response({
                "status": "success",
                "records": len(records),
                "data": records
            })
        return Response({
            "status": "error",
            "message": "No data available. Please try again later."
        })
    except Exception as e:
        return Response({"status": "error", "message": str(e)}, status=500)



@api_view(['POST'])
@authentication_classes([])  # Disable default DRF auth for this endpoint
@permission_classes([]) 
@token_required
def bulk_scrape(request):
    try:
        # Step 1: Parse JSON body
        try:
            body = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({"response": "error", "message": "Invalid JSON payload"}, status=400)

        po_numbers = body.get("po_numbers", [])
        if not isinstance(po_numbers, list):
            return JsonResponse({"response": "error", "message": "'po_numbers' must be a list"}, status=400)

        if not po_numbers:
            return JsonResponse({"response": "error", "message": "No PO numbers provided"}, status=400)

        cached_data = redis_client.get("Po_status")
        if not cached_data:
            return JsonResponse({"response": "error", "message": "No cached PO data found"}, status=500)

        try:
            records = json.loads(cached_data)
        except json.JSONDecodeError:
            return JsonResponse({"response": "error", "message": "Cached data format is invalid"}, status=500)

        record_map = {rec["po_number"]: rec["status"] for rec in records if isinstance(rec, dict)}
        response = [
            {
                "po number": po,
                "status": record_map.get(po, "Not found")
            }
            for po in po_numbers
        ]
        return JsonResponse({
            "response": response,
        }, status=200)

    except Exception as e:
        return JsonResponse({"response": "error", "message": f"Server error: {str(e)}"}, status=500)
    
@api_view(['POST'])
@authentication_classes([])  # Disable default DRF auth for this endpoint
@permission_classes([]) 
@token_required
def update_erp_password(request):
    try:
        new_password = request.data.get("new_password")
        if not new_password:
            return Response({"error": "Password not provided"}, status=400)

        # Absolute path to credentials.py
        credentials_path = "/home/ubuntu/Nexus_automation/indusproject/credentials.py"

        if not os.path.exists(credentials_path):
            return Response({"error": f"credentials.py not found at {credentials_path}"}, status=404)

        # Read the file
        with open(credentials_path, "r") as file:
            lines = file.readlines()

        # Modify only the ERP_PASSWORD line
        with open(credentials_path, "w") as file:
            for line in lines:
                if line.strip().startswith("ERP_PASSWORD"):
                    file.write(f'ERP_PASSWORD = "{new_password}"\n')
                else:
                    file.write(line)

        return Response({
            "message": f"ERP password updated successfully to '{new_password}'",
            "file_location": credentials_path
        }, status=200)

    except Exception as e:
        return Response({"error": str(e)}, status=500)


# indusapi/views.py
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from indusproject.scheduler import update_job_schedule
from .utils import token_required

@api_view(['POST'])
@authentication_classes([])  # Keep empty because token_required handles auth
@permission_classes([])
@token_required
def update_cron_time(request):

    try:
        job_id = request.data.get("job_id")
        hour = int(request.data.get("hour"))
        minute = int(request.data.get("minute"))

        if job_id not in ["indus_po_scraper", "scrape_and_store_in_redis"]:
            return Response({"error": "Invalid job_id"}, status=400)

        success, message = update_job_schedule(job_id, hour, minute)
        status_code = 200 if success else 400
        return Response({"status": "success" if success else "failed", "message": message}, status=status_code)

    except Exception as e:
        return Response({"status": "failed", "message": str(e)}, status=500)

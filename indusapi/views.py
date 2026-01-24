# indus_api/views.py
from rest_framework.decorators import api_view, permission_classes, authentication_classes
from rest_framework.response import Response
import os, json, logging
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

# Configure production-level logging
logger = logging.getLogger('indusapi.views')
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
    logger.info(f"Received request to get_po_data from {request.META.get('REMOTE_ADDR')}")
    try:
        data = redis_client.get("indus_po_data")
        if data:
            records = json.loads(data)
            logger.info(f"Successfully retrieved {len(records)} PO records from Redis")
            return Response({
                "status": "success",
                "records": len(records),
                "data": records
            })
        logger.warning("No PO data available in Redis")
        return Response({
            "status": "error",
            "message": "No data available. Please try again later."
        })
    except Exception as e:
        logger.error(f"Error in get_po_data: {str(e)}", exc_info=True)
        return Response({"status": "error", "message": str(e)}, status=500)



from django.views.decorators.http import require_POST

@csrf_exempt
@require_POST
def bulk_scrape(request):
    logger.info(f"bulk_scrape view function called from {request.META.get('REMOTE_ADDR')}")
    
    # Manual token check
    auth_header = request.headers.get("Authorization")
    logger.info(f"Received authorization header: {'Present' if auth_header else 'Missing'}")
    
    if not auth_header or not auth_header.startswith("Bearer "):
        logger.warning("Authorization header missing or invalid")
        return JsonResponse({"error": "Authorization header missing or invalid"}, status=401)
    
    token = auth_header.split(" ")[1]
    if token != os.getenv("STATIC_API_TOKEN", "0fad0954-ff69-42ff-bf53-6f12bee5da6f"):
        logger.warning("Invalid API token provided")
        return JsonResponse({"error": "Invalid token"}, status=403)
    
    try:
        logger.info(f"Request Content-Type: {request.content_type}, Body length: {len(request.body)}")
        
        # Check if body is empty
        if not request.body or len(request.body) == 0:
            logger.error("Empty request body received")
            return JsonResponse({
                "response": "error", 
                "message": "Request body is empty. Please send JSON data with 'po_numbers' field."
            }, status=400)
        
        # Step 1: Parse JSON body
        try:
            body = json.loads(request.body)
            logger.info(f"Successfully parsed request body with keys: {list(body.keys())}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            return JsonResponse({"response": "error", "message": f"Invalid JSON payload: {str(e)}"}, status=400)

        po_numbers = body.get("po_numbers", [])
        logger.info(f"Extracted {len(po_numbers) if isinstance(po_numbers, list) else 0} PO numbers from request")
        
        if not isinstance(po_numbers, list):
            logger.error("po_numbers field is not a list")
            return JsonResponse({"response": "error", "message": "'po_numbers' must be a list"}, status=400)

        if not po_numbers:
            logger.warning("Empty po_numbers list provided")
            return JsonResponse({"response": "error", "message": "No PO numbers provided"}, status=400)

        cached_data = redis_client.get("Po_status")
        if not cached_data:
            logger.warning("No cached PO status data found in Redis")
            return JsonResponse({
                "response": "error", 
                "message": "No cached PO data found. Please run the scraper first or wait for the scheduled job to populate the cache.",
                "hint": "Run: python -c 'from indusproject.status_scrapper import scrape_and_store_in_redis; scrape_and_store_in_redis()'"
            }, status=503)

        try:
            records = json.loads(cached_data)
            logger.info(f"Successfully retrieved {len(records)} PO status records from cache")
        except json.JSONDecodeError:
            logger.error("Failed to decode cached PO status data - invalid JSON format")
            return JsonResponse({"response": "error", "message": "Cached data format is invalid"}, status=500)

        record_map = {rec["po_number"]: rec["status"] for rec in records if isinstance(rec, dict)}
        response = [
            {
                "po number": po,
                "status": record_map.get(po, "Not found")
            }
            for po in po_numbers
        ]
        logger.info(f"Successfully processed bulk scrape request for {len(po_numbers)} PO numbers")
        return JsonResponse({
            "response": response,
        }, status=200)

    except Exception as e:
        logger.error(f"Server error in bulk_scrape: {str(e)}", exc_info=True)
        return JsonResponse({"response": "error", "message": f"Server error: {str(e)}"}, status=500)
    
@api_view(['POST'])
@authentication_classes([])  # Disable default DRF auth for this endpoint
@permission_classes([]) 
@token_required
def update_erp_password(request):
    logger.info(f"Received request to update ERP password from {request.META.get('REMOTE_ADDR')}")
    try:
        new_password = request.data.get("new_password")
        if not new_password:
            logger.warning("Password update request missing new_password field")
            return Response({"error": "Password not provided"}, status=400)

        # Cross-platform path to credentials.py
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        credentials_path = os.path.join(base_dir, 'indusproject', 'credentials.py')

        if not os.path.exists(credentials_path):
            logger.error(f"credentials.py not found at {credentials_path}")
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

        logger.info(f"Successfully updated ERP password in {credentials_path}")
        return Response({
            "message": f"ERP password updated successfully to '{new_password}'",
            "file_location": credentials_path
        }, status=200)

    except Exception as e:
        logger.error(f"Error updating ERP password: {str(e)}", exc_info=True)
        return Response({"error": str(e)}, status=500)



from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from indusproject.scheduler import update_job_schedule, get_scheduler_status
from .utils import token_required

@api_view(['POST'])
@authentication_classes([])  # Keep empty because token_required handles auth
@permission_classes([])
@token_required
def update_cron_time(request):
    logger.info(f"Received request to update cron time from {request.META.get('REMOTE_ADDR')}")
    logger.info(f"Request data: {request.data}")
    
    try:
        # Validate required parameters
        job_id = request.data.get("job_id")
        hour = request.data.get("hour")
        minute = request.data.get("minute")
        
        # Check for missing parameters
        missing_params = []
        if job_id is None:
            missing_params.append("job_id")
        if hour is None:
            missing_params.append("hour")
        if minute is None:
            missing_params.append("minute")
        
        if missing_params:
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.warning(error_msg)
            return Response({
                "status": "failed",
                "error": error_msg,
                "required_parameters": {
                    "job_id": "indus_po_scraper or scrape_and_store_in_redis",
                    "hour": "0-23",
                    "minute": "0-59"
                }
            }, status=400)
        
        # Convert to integers
        try:
            hour = int(hour)
            minute = int(minute)
        except ValueError as e:
            error_msg = f"Invalid parameter format: hour and minute must be integers"
            logger.warning(f"{error_msg} - hour={hour}, minute={minute}")
            return Response({"status": "failed", "error": error_msg}, status=400)
        
        # Validate ranges
        if not (0 <= hour <= 23):
            logger.warning(f"Hour out of range: {hour}")
            return Response({"status": "failed", "error": "Hour must be between 0 and 23"}, status=400)
        
        if not (0 <= minute <= 59):
            logger.warning(f"Minute out of range: {minute}")
            return Response({"status": "failed", "error": "Minute must be between 0 and 59"}, status=400)
        
        # Validate job_id
        if job_id not in ["indus_po_scraper", "scrape_and_store_in_redis"]:
            logger.warning(f"Invalid job_id provided: {job_id}")
            return Response({
                "status": "failed",
                "error": f"Invalid job_id: {job_id}",
                "valid_jobs": ["indus_po_scraper", "scrape_and_store_in_redis"]
            }, status=400)

        logger.info(f"Updating schedule for job_id: {job_id} to {hour:02d}:{minute:02d}")
        success, message = update_job_schedule(job_id, hour, minute)
        status_code = 200 if success else 400
        
        if success:
            logger.info(f"Successfully updated cron schedule: {message}")
        else:
            logger.error(f"Failed to update cron schedule: {message}")
            
        return Response({"status": "success" if success else "failed", "message": message}, status=status_code)

    except Exception as e:
        logger.error(f"Error updating cron time: {str(e)}", exc_info=True)
        return Response({"status": "failed", "message": str(e)}, status=500)

@api_view(['GET'])
@authentication_classes([])
@permission_classes([])
@token_required
def scheduler_status(request):
    """Check if scheduler is running and get its status"""
    logger.info(f"Received scheduler status check from {request.META.get('REMOTE_ADDR')}")
    try:
        status = get_scheduler_status()
        logger.info(f"Scheduler status: {status}")
        return Response({
            "status": "success",
            "scheduler": status
        }, status=200)
    except Exception as e:
        logger.error(f"Error getting scheduler status: {str(e)}", exc_info=True)
        return Response({
            "status": "error",
            "message": str(e)
        }, status=500)

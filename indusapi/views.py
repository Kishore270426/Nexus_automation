# indus_api/views.py
from rest_framework.decorators import api_view, permission_classes
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



load_dotenv()
redis_client = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB"))
)

@api_view(['POST'])
def get_po_data(request):
    body = json.loads(request.body)
    username = body.get("username")
    password = body.get("password")

    if not username or not password:
            return JsonResponse({"response": "error", "message": "Username and password required"}, status=401)
    
    user = authenticate(request, username=username, password=password)
    if user is None:
        return JsonResponse({"response": "error", "message": "Invalid credentials"}, status=401)


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



@api_view(["POST"])
def bulk_scrape(request):
    try:
        # Step 1: Parse JSON body
        try:
            body = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({"response": "error", "message": "Invalid JSON payload"}, status=400)

        username = body.get("username")
        password = body.get("password")

        if not username or not password:
            return JsonResponse({"response": "error", "message": "Username and password required"}, status=401)

        # Step 2: Authenticate user
        user = authenticate(request, username=username, password=password)
        if user is None:
            return JsonResponse({"response": "error", "message": "Invalid credentials"}, status=401)


        # Step 4: Proceed with the logic after authentication
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
    
@api_view(["POST"])
def change_credentials(request):
    try:
        body = json.loads(request.body)
    except Exception:
        return JsonResponse({"response": "error", "message": "Invalid request body"}, status=400)

    username = body.get("username")
    old_password = body.get("old_password")
    new_password = body.get("new_password")
    new_username = body.get("new_username")  # keep separate from current username

    # Step 1: Authenticate user with current credentials
    user = authenticate(request, username=username, password=old_password)
    if user is None:
        return JsonResponse({"response": "error", "message": "Invalid username or password"}, status=401)

    # Step 2: Update username if provided
    if new_username:
        if User.objects.filter(username=new_username).exists():
            return JsonResponse({"response": "error", "message": "New username already taken"}, status=400)
        user.username = new_username

    # Step 3: Update password if provided
    if new_password:
        try:
            validate_password(new_password, user)
            user.set_password(new_password)
        except Exception as e:
            return JsonResponse({"response": "error", "message": str(e)}, status=400)

    user.save()


    return Response({
        "status": "success",
        "message": "Credentials updated successfully",

    }, status=status.HTTP_200_OK)
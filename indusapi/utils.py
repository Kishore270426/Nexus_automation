
from rest_framework.response import Response
from functools import wraps
from django.conf import settings
import logging

# Configure production-level logging
logger = logging.getLogger('indusapi.utils')

def token_required(view_func):
    @wraps(view_func)
    def wrapped(request, *args, **kwargs):
        logger.info(f"Checking authorization for {request.path}")
        auth_header = request.headers.get("Authorization")
        
        if not auth_header or not auth_header.startswith("Bearer "):
            logger.warning(f"Missing or invalid auth header for {request.path}")
            return Response({"error": "Authorization header missing or invalid"}, status=401)

        token = auth_header.split(" ")[1]
        
        if token != settings.STATIC_API_TOKEN:
            logger.warning(f"Token mismatch for {request.path}")
            return Response({"error": "Invalid token"}, status=403)

        logger.info(f"Auth successful for {request.path}, proceeding to view")
        return view_func(request, *args, **kwargs)
    return wrapped

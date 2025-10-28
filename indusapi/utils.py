
from rest_framework.response import Response
from functools import wraps
from django.conf import settings

def token_required(view_func):
    @wraps(view_func)
    def wrapped(request, *args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return Response({"error": "Authorization header missing or invalid"}, status=401)

        token = auth_header.split(" ")[1]
        if token != settings.STATIC_API_TOKEN:
            return Response({"error": "Invalid token"}, status=403)

        return view_func(request, *args, **kwargs)
    return wrapped

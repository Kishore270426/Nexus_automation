import logging

# Configure production-level logging
logger = logging.getLogger('indusproject.middleware')

class DebugMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path == '/api/po-status/':
            logger.info(f"Request received for {request.path}")
            logger.info(f"Method: {request.method}, Content-Type: {request.content_type}")
            logger.info(f"Headers: {dict(request.headers)}")
            logger.info(f"Body length: {len(request.body)} bytes")

        response = self.get_response(request)
        
        if request.path == '/api/po-status/':
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response content length: {len(response.content)} bytes")

        return response

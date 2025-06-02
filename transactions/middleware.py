from django.utils.deprecation import MiddlewareMixin
from .models import APIAccessLog
import json


class APIAccessLogMiddleware(MiddlewareMixin):
    """Middleware to log all API access"""
    
    def process_request(self, request):
        # Store request data for later use
        request._body_data = request.body.decode('utf-8') if request.body else ''
        return None
    
    def process_response(self, request, response):
        # Only log API calls (not admin, static files, etc.)
        if request.path.startswith('/transactions/') or request.path.startswith('/auth/'):
            try:
                # Get client IP
                x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
                if x_forwarded_for:
                    ip = x_forwarded_for.split(',')[0]
                else:
                    ip = request.META.get('REMOTE_ADDR')
                
                # Create log entry
                APIAccessLog.objects.create(
                    user=request.user if request.user.is_authenticated else None,
                    method=request.method,
                    path=request.path,
                    query_params=request.GET.urlencode(),
                    request_body=getattr(request, '_body_data', ''),
                    response_status=response.status_code,
                    ip_address=ip or '127.0.0.1',
                    user_agent=request.META.get('HTTP_USER_AGENT', '')
                )
            except Exception as e:
                # Don't let logging errors break the response
                print(f"Logging error: {e}")
        
        return response
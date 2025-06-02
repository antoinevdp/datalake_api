from rest_framework.permissions import BasePermission
from .models import DataTablePermission


class HasTablePermission(BasePermission):
    """
    Custom permission to check if user has access to specific table
    """
    
    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        
        # Admins have access to everything
        if request.user.is_staff:
            return True
        
        # Check if user has permission for 'transactions' table
        table_name = 'transactions'  # You can make this dynamic
        
        return DataTablePermission.objects.filter(
            user=request.user,
            table_name=table_name,
            permission_type__in=['read', 'admin'],
            is_active=True
        ).exists()
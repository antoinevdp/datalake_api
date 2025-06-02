from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone


class DataTablePermission(models.Model):
    """Model to manage user permissions for data tables/files"""
    PERMISSION_CHOICES = [
        ('read', 'Read'),
        ('write', 'Write'),
        ('admin', 'Admin'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=255)  # e.g., 'transactions', 'users', etc.
    permission_type = models.CharField(max_length=10, choices=PERMISSION_CHOICES)
    granted_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='granted_permissions')
    granted_at = models.DateTimeField(default=timezone.now)
    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = ('user', 'table_name', 'permission_type')

    def __str__(self):
        return f"{self.user.username} - {self.table_name} - {self.permission_type}"


class APIAccessLog(models.Model):
    """Model to log all API access attempts"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    timestamp = models.DateTimeField(default=timezone.now)
    method = models.CharField(max_length=10)  # GET, POST, PUT, DELETE
    path = models.CharField(max_length=500)
    query_params = models.TextField(blank=True)
    request_body = models.TextField(blank=True)
    response_status = models.IntegerField()
    ip_address = models.GenericIPAddressField()
    user_agent = models.TextField(blank=True)

    def __str__(self):
        username = self.user.username if self.user else 'Anonymous'
        return f"{username} - {self.method} {self.path} - {self.timestamp}"

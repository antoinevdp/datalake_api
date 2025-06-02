from django.urls import path
from . import views, auth_views

urlpatterns = [
    # Transaction endpoints
    path("", views.TransactionListView.as_view(), name="transactions"),
    
    # Authentication endpoints
    path("auth/login/", auth_views.CustomAuthToken.as_view(), name="auth_login"),
    
    # Permission management endpoints
    path("permissions/grant/", auth_views.GrantPermissionView.as_view(), name="grant_permission"),
    path("permissions/revoke/", auth_views.RevokePermissionView.as_view(), name="revoke_permission"),
    path("permissions/list/", auth_views.ListPermissionsView.as_view(), name="list_permissions"),
]
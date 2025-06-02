from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework.permissions import IsAuthenticated
from django.contrib.auth.models import User
from django.contrib.auth import authenticate
from .models import DataTablePermission
from .serializers import DataTablePermissionSerializer
import json


class CustomAuthToken(ObtainAuthToken):
    """Custom authentication endpoint that returns user info with token"""
    
    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data,
                                           context={'request': request})
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data['user']
        token, created = Token.objects.get_or_create(user=user)
        
        return Response({
            'token': token.key,
            'user_id': user.pk,
            'username': user.username,
            'email': user.email,
            'message': 'Authentication successful'
        })


class GrantPermissionView(APIView):
    """Grant permissions to users for data tables"""
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        try:
            # Only admins can grant permissions
            if not request.user.is_staff:
                return Response({
                    'error': 'Only administrators can grant permissions'
                }, status=status.HTTP_403_FORBIDDEN)
            
            username = request.data.get('username')
            table_name = request.data.get('table_name')
            permission_type = request.data.get('permission_type')
            
            if not all([username, table_name, permission_type]):
                return Response({
                    'error': 'username, table_name, and permission_type are required'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Check if user exists
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                return Response({
                    'error': f'User {username} does not exist'
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Create or update permission
            permission, created = DataTablePermission.objects.get_or_create(
                user=user,
                table_name=table_name,
                permission_type=permission_type,
                defaults={
                    'granted_by': request.user,
                    'is_active': True
                }
            )
            
            if not created:
                permission.is_active = True
                permission.granted_by = request.user
                permission.save()
            
            return Response({
                'message': f'Permission {permission_type} granted to {username} for {table_name}',
                'permission_id': permission.id
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            return Response({
                'error': f'Internal server error: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class RevokePermissionView(APIView):
    """Revoke permissions from users"""
    permission_classes = [IsAuthenticated]
    
    def delete(self, request):
        try:
            # Only admins can revoke permissions
            if not request.user.is_staff:
                return Response({
                    'error': 'Only administrators can revoke permissions'
                }, status=status.HTTP_403_FORBIDDEN)
            
            username = request.data.get('username')
            table_name = request.data.get('table_name')
            permission_type = request.data.get('permission_type')
            
            if not all([username, table_name, permission_type]):
                return Response({
                    'error': 'username, table_name, and permission_type are required'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                user = User.objects.get(username=username)
                permission = DataTablePermission.objects.get(
                    user=user,
                    table_name=table_name,
                    permission_type=permission_type,
                    is_active=True
                )
                permission.is_active = False
                permission.save()
                
                return Response({
                    'message': f'Permission {permission_type} revoked from {username} for {table_name}'
                })
                
            except User.DoesNotExist:
                return Response({
                    'error': f'User {username} does not exist'
                }, status=status.HTTP_404_NOT_FOUND)
            except DataTablePermission.DoesNotExist:
                return Response({
                    'error': f'Permission not found'
                }, status=status.HTTP_404_NOT_FOUND)
                
        except Exception as e:
            return Response({
                'error': f'Internal server error: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ListPermissionsView(APIView):
    """List all permissions for a user or table"""
    permission_classes = [IsAuthenticated]
    
    def get(self, request):
        try:
            username = request.query_params.get('username')
            table_name = request.query_params.get('table_name')
            
            permissions = DataTablePermission.objects.filter(is_active=True)
            
            if username:
                try:
                    user = User.objects.get(username=username)
                    permissions = permissions.filter(user=user)
                except User.DoesNotExist:
                    return Response({
                        'error': f'User {username} does not exist'
                    }, status=status.HTTP_404_NOT_FOUND)
            
            if table_name:
                permissions = permissions.filter(table_name=table_name)
            
            # If not admin, only show own permissions
            if not request.user.is_staff:
                permissions = permissions.filter(user=request.user)
            
            serializer = DataTablePermissionSerializer(permissions, many=True)
            return Response({
                'permissions': serializer.data
            })
            
        except Exception as e:
            return Response({
                'error': f'Internal server error: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
from rest_framework import serializers
from .models import DataTablePermission, APIAccessLog
from django.contrib.auth.models import User

class TransactionSerializer(serializers.Serializer):
    TRANSACTION_ID = serializers.CharField()
    TIMESTAMP = serializers.DateTimeField()
    USER_ID = serializers.CharField()
    USER_NAME = serializers.CharField(allow_null=True)
    PRODUCT_ID = serializers.CharField()
    AMOUNT_USD = serializers.FloatField()
    CURRENCY = serializers.CharField()
    TRANSACTION_TYPE = serializers.CharField()
    STATUS = serializers.CharField()
    LOCATION_CITY = serializers.CharField()
    LOCATION_COUNTRY = serializers.CharField()
    PAYMENT_METHOD = serializers.CharField()
    PRODUCT_CATEGORY = serializers.CharField()
    QUANTITY = serializers.IntegerField()
    SHIPPING_STREET = serializers.CharField(allow_null=True)
    SHIPPING_ZIP = serializers.CharField(allow_null=True)
    SHIPPING_CITY = serializers.CharField(allow_null=True)
    SHIPPING_COUNTRY = serializers.CharField(allow_null=True)
    DEVICE_OS = serializers.CharField()
    DEVICE_BROWSER = serializers.CharField()
    DEVICE_IP_ADDRESS = serializers.CharField()
    CUSTOMER_RATING = serializers.IntegerField(allow_null=True)
    DISCOUNT_CODE = serializers.CharField(allow_null=True)
    TAX_AMOUNT = serializers.FloatField()
    THREAD = serializers.IntegerField()
    MESSAGE_NUMBER = serializers.IntegerField()
    TIMESTAMP_OF_RECEPTION_LOG = serializers.CharField()


class DataTablePermissionSerializer(serializers.ModelSerializer):
    username = serializers.CharField(source='user.username', read_only=True)
    granted_by_username = serializers.CharField(source='granted_by.username', read_only=True)

    class Meta:
        model = DataTablePermission
        fields = ['id', 'username', 'table_name', 'permission_type',
                  'granted_by_username', 'granted_at', 'is_active']


class APIAccessLogSerializer(serializers.ModelSerializer):
    username = serializers.CharField(source='user.username', read_only=True)

    class Meta:
        model = APIAccessLog
        fields = ['id', 'username', 'timestamp', 'method', 'path',
                  'query_params', 'request_body', 'response_status', 'ip_address']
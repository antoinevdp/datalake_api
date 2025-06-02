from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework import status
from .serializers import TransactionSerializer
from .permissions import HasTablePermission
from .pagination import CustomTransactionPagination
import pandas as pd
import os
import glob





class TransactionListView(APIView):
    """
    Get transactions from parquet files with authentication and authorization
    """
    permission_classes = [IsAuthenticated, HasTablePermission]
    pagination_class = CustomTransactionPagination

    def get(self, request):
        try:
            # Load all parquet files
            transactions_df = self.load_all_parquet_files()

            if transactions_df.empty:
                return Response({
                    'count': 0,
                    'next': None,
                    'previous': None,
                    'results': []
                })

            # Convert DataFrame to list of dictionaries
            transactions_list = transactions_df.to_dict('records')

            # Handle NaN values
            for transaction in transactions_list:
                for key, value in transaction.items():
                    if pd.isna(value):
                        transaction[key] = None

            # Apply pagination
            paginator = self.pagination_class()
            paginated_transactions = paginator.paginate_queryset(transactions_list, request)

            # Serialize the data
            serializer = TransactionSerializer(paginated_transactions, many=True)

            return paginator.get_paginated_response(serializer.data)

        except Exception as e:
            return Response({
                'error': f'Internal server error: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def load_all_parquet_files(self):
        """Load and combine all parquet files"""
        data_lake_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_lake')
        parquet_files = glob.glob(os.path.join(data_lake_path, '*.parquet'))

        if not parquet_files:
            return pd.DataFrame()

        dataframes = []
        for file_path in parquet_files:
            try:
                df = pd.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue

        if not dataframes:
            return pd.DataFrame()

        combined_df = pd.concat(dataframes, ignore_index=True)

        if 'TIMESTAMP' in combined_df.columns:
            combined_df = combined_df.sort_values('TIMESTAMP', ascending=False)

        return combined_df
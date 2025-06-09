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
from sqlalchemy import create_engine, inspect, text
import pymysql
from datetime import datetime, timedelta
import pytz


class BaseParquetView(APIView):
    """
    Base view for reading parquet files with authentication and authorization
    """
    permission_classes = [IsAuthenticated, HasTablePermission]
    pagination_class = CustomTransactionPagination
    folder_name = None  # Will be set by dynamically created subclasses

    def get(self, request):
        try:
            # Load all parquet files from the specified folder
            transactions_df = self.load_parquet_files()

            if transactions_df.empty:
                return Response({
                    'count': 0,
                    'next': None,
                    'previous': None,
                    'results': []
                })

            # Apply filters
            transactions_df = self.apply_filters(transactions_df, request.query_params)

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

    def load_parquet_files(self):
        """Load and combine all parquet files from the specified folder"""
        folder_path = self.get_folder_path()
        if not os.path.exists(folder_path):
            return pd.DataFrame()

        parquet_files = glob.glob(os.path.join(folder_path, '*.parquet'))

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

    def get_folder_path(self):
        """Get the folder path to read from based on the folder_name attribute"""
        data_lake_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_lake')
        if self.folder_name:
            return os.path.join(data_lake_path, self.folder_name)
        return data_lake_path

    def apply_filters(self, df, query_params):
        """Apply filters from query parameters to the DataFrame"""
        if not query_params:
            return df

        filtered_df = df.copy()

        # Payment method filter
        if 'payment_method' in query_params:
            payment_methods = query_params.getlist('payment_method')
            filtered_df = filtered_df[filtered_df['PAYMENT_METHOD'].isin(payment_methods)]

        # Country filter
        if 'country' in query_params:
            countries = query_params.getlist('country')
            filtered_df = filtered_df[filtered_df['LOCATION_COUNTRY'].isin(countries)]

        # Product category filter
        if 'product_category' in query_params:
            categories = query_params.getlist('product_category')
            filtered_df = filtered_df[filtered_df['PRODUCT_CATEGORY'].isin(categories)]

        # Status filter
        if 'status' in query_params:
            statuses = query_params.getlist('status')
            filtered_df = filtered_df[filtered_df['STATUS'].isin(statuses)]

        # Amount filters
        if 'amount_gt' in query_params:
            try:
                amount_gt = float(query_params['amount_gt'])
                filtered_df = filtered_df[filtered_df['AMOUNT_USD'] > amount_gt]
            except (ValueError, TypeError):
                pass

        if 'amount_lt' in query_params:
            try:
                amount_lt = float(query_params['amount_lt'])
                filtered_df = filtered_df[filtered_df['AMOUNT_USD'] < amount_lt]
            except (ValueError, TypeError):
                pass

        if 'amount_eq' in query_params:
            try:
                amount_eq = float(query_params['amount_eq'])
                filtered_df = filtered_df[filtered_df['AMOUNT_USD'] == amount_eq]
            except (ValueError, TypeError):
                pass

        # Customer rating filters
        if 'rating_gt' in query_params:
            try:
                rating_gt = float(query_params['rating_gt'])
                # Filter for non-null ratings first, then apply the comparison
                filtered_df = filtered_df[
                    filtered_df['CUSTOMER_RATING'].notna() &
                    (filtered_df['CUSTOMER_RATING'] > rating_gt)
                    ]
            except (ValueError, TypeError):
                pass

        if 'rating_lt' in query_params:
            try:
                rating_lt = float(query_params['rating_lt'])
                filtered_df = filtered_df[
                    filtered_df['CUSTOMER_RATING'].notna() &
                    (filtered_df['CUSTOMER_RATING'] < rating_lt)
                    ]
            except (ValueError, TypeError):
                pass

        if 'rating_eq' in query_params:
            try:
                rating_eq = float(query_params['rating_eq'])
                filtered_df = filtered_df[
                    filtered_df['CUSTOMER_RATING'].notna() &
                    (filtered_df['CUSTOMER_RATING'] == rating_eq)
                    ]
            except (ValueError, TypeError):
                pass

        return filtered_df


class BaseDatabaseTableView(APIView):
    """
    Base view for reading data from MariaDB tables
    """
    permission_classes = [IsAuthenticated, HasTablePermission]
    pagination_class = CustomTransactionPagination
    table_name = None  # Will be set by dynamically created subclasses

    def get(self, request):
        try:
            # Load data from the specified table
            table_df = self.load_table_data(request.query_params)

            if table_df.empty:
                return Response({
                    'count': 0,
                    'next': None,
                    'previous': None,
                    'results': []
                })

            # Convert DataFrame to list of dictionaries
            table_data = table_df.to_dict('records')

            # Handle NaN values
            for record in table_data:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            # Apply pagination
            paginator = self.pagination_class()
            paginated_data = paginator.paginate_queryset(table_data, request)

            # Serialize the data - using TransactionSerializer as a base,
            # but in a production app you might want to create dynamic serializers
            serializer = TransactionSerializer(paginated_data, many=True)

            return paginator.get_paginated_response(serializer.data)

        except Exception as e:
            return Response({
                'error': f'Internal server error: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def load_table_data(self, query_params=None):
        """Load filtered data from the specified database table"""
        if not self.table_name:
            return pd.DataFrame()

        try:
            # Create a connection to MariaDB
            db_connection_str = 'mysql+pymysql://tonio:efrei1234@localhost/datalake'
            db_connection = create_engine(db_connection_str)

            # Construct SQL query with filters
            query = self.build_sql_query(query_params)

            # Query the table
            df = pd.read_sql(query, db_connection)

            if 'TIMESTAMP' in df.columns and not df.empty:
                df = df.sort_values('TIMESTAMP', ascending=False)

            return df

        except Exception as e:
            print(f"Error reading from database table {self.table_name}: {e}")
            return pd.DataFrame()

    def build_sql_query(self, query_params):
        """Build SQL query with filters from query parameters"""
        query = f"SELECT * FROM {self.table_name}"

        if not query_params:
            return query

        conditions = []

        # Payment method filter
        if 'payment_method' in query_params:
            payment_methods = query_params.getlist('payment_method')
            payment_methods_str = ', '.join([f"'{method}'" for method in payment_methods])
            conditions.append(f"PAYMENT_METHOD IN ({payment_methods_str})")

        # Country filter
        if 'country' in query_params:
            countries = query_params.getlist('country')
            countries_str = ', '.join([f"'{country}'" for country in countries])
            conditions.append(f"LOCATION_COUNTRY IN ({countries_str})")

        # Product category filter
        if 'product_category' in query_params:
            categories = query_params.getlist('product_category')
            categories_str = ', '.join([f"'{category}'" for category in categories])
            conditions.append(f"PRODUCT_CATEGORY IN ({categories_str})")

        # Status filter
        if 'status' in query_params:
            statuses = query_params.getlist('status')
            statuses_str = ', '.join([f"'{status}'" for status in statuses])
            conditions.append(f"STATUS IN ({statuses_str})")

        # Amount filters
        if 'amount_gt' in query_params:
            try:
                amount_gt = float(query_params['amount_gt'])
                conditions.append(f"AMOUNT_USD > {amount_gt}")
            except (ValueError, TypeError):
                pass

        if 'amount_lt' in query_params:
            try:
                amount_lt = float(query_params['amount_lt'])
                conditions.append(f"AMOUNT_USD < {amount_lt}")
            except (ValueError, TypeError):
                pass

        if 'amount_eq' in query_params:
            try:
                amount_eq = float(query_params['amount_eq'])
                conditions.append(f"AMOUNT_USD = {amount_eq}")
            except (ValueError, TypeError):
                pass

        # Customer rating filters
        if 'rating_gt' in query_params:
            try:
                rating_gt = float(query_params['rating_gt'])
                conditions.append(f"CUSTOMER_RATING > {rating_gt} AND CUSTOMER_RATING IS NOT NULL")
            except (ValueError, TypeError):
                pass

        if 'rating_lt' in query_params:
            try:
                rating_lt = float(query_params['rating_lt'])
                conditions.append(f"CUSTOMER_RATING < {rating_lt} AND CUSTOMER_RATING IS NOT NULL")
            except (ValueError, TypeError):
                pass

        if 'rating_eq' in query_params:
            try:
                rating_eq = float(query_params['rating_eq'])
                conditions.append(f"CUSTOMER_RATING = {rating_eq} AND CUSTOMER_RATING IS NOT NULL")
            except (ValueError, TypeError):
                pass

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        return query


class DataSourcesView(APIView):
    """
    Get list of all data sources (Parquet folders and MariaDB tables)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            # Get parquet folders
            data_lake_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_lake')
            folders = []

            if os.path.exists(data_lake_path):
                folder_names = [f for f in os.listdir(data_lake_path)
                                if os.path.isdir(os.path.join(data_lake_path, f))]

                for folder in folder_names:
                    folder_path = os.path.join(data_lake_path, folder)
                    parquet_count = len(glob.glob(os.path.join(folder_path, '*.parquet')))

                    # Create URL-friendly version of folder name
                    url_path = folder.lower().replace(' ', '_')

                    folders.append({
                        'name': folder,
                        'type': 'parquet_folder',
                        'file_count': parquet_count,
                        'endpoint': f"/api/transactions/parquet/{url_path}/"
                    })

            # Get MariaDB tables
            tables = []
            try:
                # Create a connection to MariaDB
                db_connection_str = 'mysql+pymysql://tonio:efrei1234@localhost/datalake'
                engine = create_engine(db_connection_str)

                # Get all tables in the datalake schema
                inspector = inspect(engine)
                table_names = inspector.get_table_names()

                for table in table_names:
                    # Get row count for each table
                    with engine.connect() as connection:
                        result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        row_count = result.scalar()

                    # Create URL-friendly version of table name
                    url_path = table.lower().replace(' ', '_')

                    tables.append({
                        'name': table,
                        'type': 'database_table',
                        'row_count': row_count,
                        'endpoint': f"/api/transactions/db/{url_path}/"
                    })

            except Exception as e:
                print(f"Error connecting to database: {e}")

            # Get filter options
            filter_options = self.get_filter_options()

            return Response({
                'parquet_folders': folders,
                'database_tables': tables,
                'filter_options': filter_options
            })

        except Exception as e:
            return Response({
                'error': f'Internal server error: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def get_filter_options(self):
        """Get available options for filters"""
        try:
            # Try to get filter options from database for consistency
            db_connection_str = 'mysql+pymysql://tonio:efrei1234@localhost/datalake'
            engine = create_engine(db_connection_str)

            filter_options = {}

            # Get payment methods
            query = "SELECT DISTINCT PAYMENT_METHOD FROM sql_transactions_cleaned WHERE PAYMENT_METHOD IS NOT NULL"
            df = pd.read_sql(query, engine)
            filter_options['payment_methods'] = df['PAYMENT_METHOD'].tolist()

            # Get countries
            query = "SELECT DISTINCT LOCATION_COUNTRY FROM sql_transactions_cleaned WHERE LOCATION_COUNTRY IS NOT NULL"
            df = pd.read_sql(query, engine)
            filter_options['countries'] = df['LOCATION_COUNTRY'].tolist()

            # Get product categories
            query = "SELECT DISTINCT PRODUCT_CATEGORY FROM sql_transactions_cleaned WHERE PRODUCT_CATEGORY IS NOT NULL"
            df = pd.read_sql(query, engine)
            filter_options['product_categories'] = df['PRODUCT_CATEGORY'].tolist()

            # Get statuses
            query = "SELECT DISTINCT STATUS FROM sql_transactions_cleaned WHERE STATUS IS NOT NULL"
            df = pd.read_sql(query, engine)
            filter_options['statuses'] = df['STATUS'].tolist()

            # Get amount range
            query = "SELECT MIN(AMOUNT_USD) as min_amount, MAX(AMOUNT_USD) as max_amount FROM sql_transactions_cleaned"
            df = pd.read_sql(query, engine)
            filter_options['amount_range'] = {
                'min': float(df['min_amount'].iloc[0]) if not pd.isna(df['min_amount'].iloc[0]) else 0,
                'max': float(df['max_amount'].iloc[0]) if not pd.isna(df['max_amount'].iloc[0]) else 0
            }

            # Get rating range
            query = "SELECT MIN(CUSTOMER_RATING) as min_rating, MAX(CUSTOMER_RATING) as max_rating FROM sql_transactions_cleaned WHERE CUSTOMER_RATING IS NOT NULL"
            df = pd.read_sql(query, engine)
            filter_options['rating_range'] = {
                'min': int(df['min_rating'].iloc[0]) if not pd.isna(df['min_rating'].iloc[0]) else 0,
                'max': int(df['max_rating'].iloc[0]) if not pd.isna(df['max_rating'].iloc[0]) else 0
            }

            return filter_options

        except Exception as e:
            print(f"Error getting filter options: {e}")

            # Fallback to hardcoded options if database connection fails
            return {
                'payment_methods': ['credit_card', 'paypal', 'bank_transfer', 'apple_pay', 'google_pay',
                                    'cryptocurrency'],
                'countries': ['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India',
                              'China'],
                'product_categories': ['electronics', 'clothing', 'books', 'home_goods', 'food'],
                'statuses': ['completed', 'pending', 'failed', 'processing', 'cancelled'],
                'amount_range': {'min': 0, 'max': 1000},
                'rating_range': {'min': 1, 'max': 5}
            }


class MetricsBaseView(APIView):
    """
    Base class for metrics endpoints
    """
    permission_classes = [IsAuthenticated]

    def load_all_data(self):
        """Load data from all sources (both parquet files and database)"""
        # Create an empty DataFrame with the expected columns
        combined_df = pd.DataFrame()

        # Process parquet files
        data_lake_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_lake')
        if os.path.exists(data_lake_path):
            parquet_df = self.load_parquet_data(data_lake_path)
            if not parquet_df.empty and not combined_df.empty:
                # Ensure columns match before concatenating
                combined_df = pd.concat([combined_df, parquet_df], ignore_index=True)
            elif not parquet_df.empty:
                combined_df = parquet_df

        # Process database tables
        db_df = self.load_database_data()
        if not db_df.empty and not combined_df.empty:
            # Ensure columns match before concatenating
            combined_df = pd.concat([combined_df, db_df], ignore_index=True)
        elif not db_df.empty:
            combined_df = db_df

        return combined_df

    def load_parquet_data(self, data_lake_path):
        """Load data from parquet files"""
        all_parquet_data = []

        folder_names = [f for f in os.listdir(data_lake_path)
                        if os.path.isdir(os.path.join(data_lake_path, f))]

        for folder in folder_names:
            folder_path = os.path.join(data_lake_path, folder)
            parquet_files = glob.glob(os.path.join(folder_path, '*.parquet'))

            for file_path in parquet_files:
                try:
                    df = pd.read_parquet(file_path)
                    # Standardize timestamp columns
                    self.standardize_timestamps(df)
                    all_parquet_data.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    continue

        if not all_parquet_data:
            return pd.DataFrame()

        return pd.concat(all_parquet_data, ignore_index=True)

    def load_database_data(self):
        """Load data from database tables"""
        all_db_data = []

        try:
            db_connection_str = 'mysql+pymysql://tonio:efrei1234@localhost/datalake'
            engine = create_engine(db_connection_str)

            # Get all tables in the datalake schema
            inspector = inspect(engine)
            table_names = inspector.get_table_names()

            for table in table_names:
                try:
                    df = pd.read_sql(f"SELECT * FROM {table}", engine)
                    # Standardize timestamp columns
                    self.standardize_timestamps(df)
                    all_db_data.append(df)
                except Exception as e:
                    print(f"Error reading from table {table}: {e}")
                    continue
        except Exception as e:
            print(f"Error connecting to database: {e}")

        if not all_db_data:
            return pd.DataFrame()

        return pd.concat(all_db_data, ignore_index=True)

    def standardize_timestamps(self, df):
        """Standardize timestamp columns to naive datetime objects"""
        if 'TIMESTAMP' in df.columns:
            # Handle string timestamps
            if df['TIMESTAMP'].dtype == 'object':
                # Convert string timestamps to datetime objects without timezone
                df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], utc=False, errors='coerce')

            # Remove timezone info if present
            if hasattr(df['TIMESTAMP'].dtype, 'tz') and df['TIMESTAMP'].dtype.tz is not None:
                df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_localize(None)


class RecentSpendingMetricsView(MetricsBaseView):
    """
    Get money spent in the last 5 minutes
    """

    def get(self, request):
        try:
            # Get minutes parameter (default to 5 minutes)
            minutes = int(request.query_params.get('minutes', 5))

            # Load all data
            df = self.load_all_data()

            if df.empty:
                return Response({
                    'total_spent': 0,
                    'transaction_count': 0,
                    'time_window': f"Last {minutes} minutes"
                })

            # Calculate current time and time window (naive datetime)
            current_time = datetime.now()
            time_window = current_time - timedelta(minutes=minutes)

            # Filter transactions in the time window
            if 'TIMESTAMP' in df.columns:
                # Handle potential NaT values
                df = df.dropna(subset=['TIMESTAMP'])
                recent_df = df[df['TIMESTAMP'] >= time_window]
            else:
                recent_df = df  # If no timestamp column, use all data

            # Filter to include only purchase and payment transactions
            if 'TRANSACTION_TYPE' in recent_df.columns:
                spending_df = recent_df[recent_df['TRANSACTION_TYPE'].isin(['purchase', 'payment'])]
            else:
                spending_df = recent_df  # If no transaction type column, use all data

            # Calculate total spending
            if 'AMOUNT_USD' in spending_df.columns:
                total_spent = spending_df['AMOUNT_USD'].sum()
                # Handle NaN result
                if pd.isna(total_spent):
                    total_spent = 0
            else:
                total_spent = 0

            transaction_count = len(spending_df)

            return Response({
                'total_spent': round(float(total_spent), 2),
                'transaction_count': transaction_count,
                'time_window': f"Last {minutes} minutes",
                'time_window_start': time_window.isoformat(),
                'time_window_end': current_time.isoformat()
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response({
                'error': f'Error calculating recent spending: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UserSpendingMetricsView(MetricsBaseView):
    """
    Get total spent per user and transaction type
    """

    def get(self, request):
        try:
            # Load all data
            df = self.load_all_data()

            if df.empty or 'USER_ID' not in df.columns:
                return Response({'users': []})

            # Ensure necessary columns exist
            required_columns = ['USER_ID', 'TRANSACTION_TYPE', 'AMOUNT_USD']
            if not all(col in df.columns for col in required_columns):
                return Response({
                    'error': f'Required columns missing. Available columns: {df.columns.tolist()}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Drop rows with NaN in required columns
            df = df.dropna(subset=required_columns)

            # Group by user and transaction type, then sum the amounts
            user_spending = df.groupby(['USER_ID', 'TRANSACTION_TYPE'])['AMOUNT_USD'].sum().reset_index()

            # Convert to dictionary format
            result = []
            for user_id in user_spending['USER_ID'].unique():
                user_data = user_spending[user_spending['USER_ID'] == user_id]

                user_result = {
                    'user_id': user_id,
                    'spending_by_type': []
                }

                for _, row in user_data.iterrows():
                    user_result['spending_by_type'].append({
                        'transaction_type': row['TRANSACTION_TYPE'],
                        'amount': round(float(row['AMOUNT_USD']), 2)
                    })

                # Calculate total spending across all transaction types
                user_result['total_spending'] = round(float(user_data['AMOUNT_USD'].sum()), 2)

                result.append(user_result)

            # Sort by total spending (descending)
            result = sorted(result, key=lambda x: x['total_spending'], reverse=True)

            return Response({'users': result})

        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response({
                'error': f'Error calculating user spending: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class TopProductsMetricsView(MetricsBaseView):
    """
    Get the top X products bought
    """

    def get(self, request):
        try:
            # Get limit parameter (default to 10)
            limit = int(request.query_params.get('limit', 10))

            # Load all data
            df = self.load_all_data()

            if df.empty or 'PRODUCT_ID' not in df.columns:
                return Response({'products': []})

            # Ensure necessary columns exist
            required_columns = ['PRODUCT_ID', 'TRANSACTION_TYPE']
            if not all(col in df.columns for col in required_columns):
                return Response({
                    'error': f'Required columns missing. Available columns: {df.columns.tolist()}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Filter to include only purchase transactions
            purchases_df = df[df['TRANSACTION_TYPE'] == 'purchase']

            # Check if we have any purchases
            if purchases_df.empty:
                return Response({'products': [], 'limit': limit})

            # Prepare aggregation
            agg_dict = {}

            # Check if QUANTITY column exists
            if 'QUANTITY' in purchases_df.columns:
                agg_dict['QUANTITY'] = 'sum'

            # Check if AMOUNT_USD column exists
            if 'AMOUNT_USD' in purchases_df.columns:
                agg_dict['AMOUNT_USD'] = 'sum'

            # If no aggregation columns, just count occurrences
            if not agg_dict:
                product_counts = purchases_df.groupby('PRODUCT_ID').size().reset_index(name='count')
                product_counts = product_counts.sort_values('count', ascending=False)

                # Limit to top X products
                top_products = product_counts.head(limit)

                # Convert to list of dictionaries
                result = []
                for _, row in top_products.iterrows():
                    result.append({
                        'product_id': row['PRODUCT_ID'],
                        'purchase_count': int(row['count'])
                    })
            else:
                # Group by product and aggregate
                product_counts = purchases_df.groupby('PRODUCT_ID').agg(agg_dict).reset_index()

                # Determine sort column (prefer QUANTITY if available)
                sort_col = 'QUANTITY' if 'QUANTITY' in agg_dict else 'AMOUNT_USD'
                product_counts = product_counts.sort_values(sort_col, ascending=False)

                # Limit to top X products
                top_products = product_counts.head(limit)

                # Convert to list of dictionaries
                result = []
                for _, row in top_products.iterrows():
                    product_dict = {'product_id': row['PRODUCT_ID']}

                    if 'QUANTITY' in agg_dict:
                        product_dict['quantity_sold'] = int(row['QUANTITY'])

                    if 'AMOUNT_USD' in agg_dict:
                        product_dict['total_revenue'] = round(float(row['AMOUNT_USD']), 2)

                    result.append(product_dict)

            return Response({
                'products': result,
                'limit': limit
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response({
                'error': f'Error calculating top products: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

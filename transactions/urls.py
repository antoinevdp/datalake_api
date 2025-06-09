from django.urls import path
from . import views, auth_views
import os
from sqlalchemy import create_engine, inspect
import pymysql

# Get all folders in data_lake directory to create dynamic endpoints
def get_data_lake_folders():
    data_lake_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_lake')
    if os.path.exists(data_lake_path):
        return [f for f in os.listdir(data_lake_path) 
                if os.path.isdir(os.path.join(data_lake_path, f))]
    return []

# Get all tables in the MariaDB datalake schema
def get_db_tables():
    try:
        db_connection_str = 'mysql+pymysql://tonio:efrei1234@localhost/datalake'
        engine = create_engine(db_connection_str)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return []

# Dynamically create view classes for each folder
folder_views = {}
for folder in get_data_lake_folders():
    # Create a class for each folder
    folder_views[folder] = type(
        f"{folder}View",
        (views.BaseParquetView,),
        {"folder_name": folder}
    )

# Dynamically create view classes for each database table
db_table_views = {}
for table in get_db_tables():
    # Create a class for each table
    db_table_views[table] = type(
        f"{table}TableView",
        (views.BaseDatabaseTableView,),
        {"table_name": table}
    )

urlpatterns = [
    # List of available data sources
    path("", views.DataSourcesView.as_view(), name="data_sources"),
    
    # Authentication endpoints
    path("auth/login/", auth_views.CustomAuthToken.as_view(), name="auth_login"),
    
    # Permission management endpoints
    path("permissions/grant/", auth_views.GrantPermissionView.as_view(), name="grant_permission"),
    path("permissions/revoke/", auth_views.RevokePermissionView.as_view(), name="revoke_permission"),
    path("permissions/list/", auth_views.ListPermissionsView.as_view(), name="list_permissions"),
    
    # Metrics endpoints
    path("metrics/recent-spending/", views.RecentSpendingMetricsView.as_view(), name="metrics_recent_spending"),
    path("metrics/user-spending/", views.UserSpendingMetricsView.as_view(), name="metrics_user_spending"),
    path("metrics/top-products/", views.TopProductsMetricsView.as_view(), name="metrics_top_products"),
]

# Dynamically add URL patterns for each folder
for folder_name, view_class in folder_views.items():
    # Convert folder name to lowercase and replace spaces with underscores for URL paths
    url_path = folder_name.lower().replace(' ', '_')
    urlpatterns.append(
        path(f"parquet/{url_path}/", view_class.as_view(), name=f"parquet_{url_path}")
    )

# Dynamically add URL patterns for each database table
for table_name, view_class in db_table_views.items():
    # Convert table name to lowercase and replace spaces with underscores for URL paths
    url_path = table_name.lower().replace(' ', '_')
    urlpatterns.append(
        path(f"db/{url_path}/", view_class.as_view(), name=f"db_{url_path}")
    )
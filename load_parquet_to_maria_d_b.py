import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os

TOPICS_NAME = ['TRANSACTIONS_CLEANED','TEST_TOPIC_TRANSACTIONS']

for topic in TOPICS_NAME:

    # Step 1: Read one of the Parquet files to understand its structure
    parquet_files = [f for f in os.listdir(f'data_lake/{topic}/') if f.endswith('.parquet')]
    file_path = os.path.join(f'data_lake/{topic}', parquet_files[0])
    df = pd.read_parquet(file_path)

    # Print schema information to understand the structure
    print("Parquet file columns and data types:")
    print(df.dtypes)

    # Step 2: Create a connection to MariaDB
    db_connection_str = 'mysql+pymysql://tonio:efrei1234@localhost/datalake'
    db_connection = create_engine(db_connection_str)

    # Step 3: Create a table in MariaDB based on the Parquet schema
    # (pandas to_sql will create the table with appropriate types)
    table_name = f"sql_{topic.lower()}"
    df.head(0).to_sql(table_name, db_connection, if_exists='replace', index=False)



    for parquet_file in parquet_files:
        file_path = os.path.join(f'data_lake/{topic}', parquet_file)
        df = pd.read_parquet(file_path)
        df.to_sql(table_name, db_connection, if_exists='append', index=False)

    print(f"Data loaded into MariaDB table '{table_name}' successfully")
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
import mysql.connector
from mysql.connector import Error

load_dotenv()

# MUX credentials
base_url = os.getenv("baseUrl")
username = os.getenv("MUX_TOKEN_ID")
password = os.getenv("MUX_TOKEN_SECRET")

# MySQL credentials
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Set up the MySQL connection
def connect_to_mysql():
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    return connection

def table_exists(connection, table_name):
    """Check if the table exists in the database."""
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()
    cursor.close()
    return result is not None

# Create MySQL table based on DataFrame schema
def create_table_from_df(connection, table_name, df):
    cursor = connection.cursor()

    columns = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            sql_type = "INT"
        elif "float" in str(dtype):
            sql_type = "FLOAT"
        elif "datetime" in str(dtype):
            sql_type = "DATETIME"
        else:
            sql_type = "TEXT"

        columns.append(f"`{column_name}` {sql_type}")
    
    columns_sql = ", ".join(columns)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_sql});"
    
    cursor.execute(create_table_sql)
    connection.commit()
    print(f"Table `{table_name}` is ready (created or already exists).")
    cursor.close()

# Insert DataFrame into MySQL table
def insert_df_to_mysql(connection, table_name, df):
    cursor = connection.cursor()
    # Generate INSERT INTO statement
    columns = ", ".join([f"`{col}`" for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
    
    # Insert rows
    for row in df.itertuples(index=False):
        row_data = tuple(None if pd.isna(val) else val for val in row)
        cursor.execute(insert_sql, tuple(row_data))
    
    connection.commit()

    print(f"Inserted {len(df)} rows into `{table_name}` successfully.")
    cursor.close()

def main():

    # Define the time frame
    utc = pytz.UTC
    now = datetime.now(utc)
    start_date = (now - timedelta(days=1)).replace(hour=now.hour, minute=0, second=0, microsecond=0)
    end_date = now.replace(hour=now.hour, minute=0, second=0, microsecond=0)

    # Convert to epoch timestamps
    start_date = int(start_date.timestamp())
    end_date = int(end_date.timestamp())

    # start_date = 1728320400
    # end_date = 1730998799

    params = {
        'timeframe[]': [start_date, end_date],
        'page': 1,
        'limit': 25
    }

    all_data = []

    response = requests.get(base_url, auth=HTTPBasicAuth(username, password), params=params)

    if response.status_code == 200:
        json_response = response.json()
        total_row_count = json_response.get('total_row_count', 0)
        limit = params['limit']
        total_pages = (total_row_count + limit - 1) // limit
        print(f"Total rows: {total_row_count}")
        print(f"Total pages available: {total_pages}")

        for page in range(1, total_pages + 1):
            params['page'] = page 
            
            response = requests.get(base_url, auth=HTTPBasicAuth(username, password), params=params)
            
            if response.status_code == 200:
                data = response.json().get('data', [])
                all_data.extend(data)
                print(f"Page {page} fetched successfully.")
            else:
                print(f"Error on page {page}: {response.status_code}, {response.text}")
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")

    views = pd.DataFrame(all_data)
    # views.to_csv("video_views_test.csv", index=False)

    video_view_details = []

    for view_id in views['id']:
        url = f"{base_url}/{view_id}"
        
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        
        data2 = response.json().get('data', {})
            
        if data2:
            video_view_details.append(data2)
            print(f"Fetched data for VIDEO_VIEW_ID: {view_id}")

        else:
            print(f"Error fetching data for VIDEO_VIEW_ID: {view_id} - {response.status_code}: {response.text}")

    df = pd.DataFrame(video_view_details)
    print(df.head())

    drop_col = ['video_completion', 'events']
    custom_cols = [f"custom_{i}" for i in range(1, 10)]
    drop_col.extend(custom_cols)

    df = df.drop(columns=drop_col, errors='ignore')

    # df.to_csv("video_views_detail_test.csv", index=False)
    # print("Data saved successfully")

    # Load DataFrame into MySQL
    try:
        connection = connect_to_mysql()
        table_name = "video_views_details_test"
        
        if not table_exists(connection, table_name):
            create_table_from_df(connection, table_name, df)
    
        insert_df_to_mysql(connection, table_name, df)

    except Error as e:
        print(f"Error: {e}")
        
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection closed.") 

if __name__ == "__main__":
    main()
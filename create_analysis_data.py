# Dotenv loading 
from dotenv import load_dotenv

# OS traversal 
import os 

# Date wrangling 
import datetime

# Input/output stream
import io

# Iteration tracking 
from tqdm import tqdm

# Importing blob functionalities
from blobs import get_blob_names

# Dataframes
import pandas as pd 

# Importing logging 
import logging

# PSQL connection 
import psycopg2

# Array math 
import numpy as np

# Infering the current file directory 
def main():
    current_file_directory = os.path.dirname(os.path.abspath(__file__))

    # Loading the .env file from the parent directory
    load_dotenv(os.path.join(current_file_directory, ".env"))

    # PSQL credentials
    db_user = os.getenv('PSQL_USER', 'default_user')
    db_pass = os.getenv('PSQL_PASSWORD', 'default_pass')
    db_host = os.getenv('PSQL_HOST', 'localhost')
    db_name = os.getenv('PSQL_DATABASE', 'default_db')
    db_port = os.getenv('PSQL_PORT', '5432')
    
    # Connecting to psql
    try:
        conn = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port, database=db_name)
        cursor = conn.cursor()
        logging.info("Connected to PSQL")
    except:
        logging.warn("Could not connect to PSQL")
        return
    
    # Queryting the max date from the api_power_usage_analytics table 
    cursor.execute("SELECT MAX(timestamp) FROM api_power_usage_analytics")
    max_timestamp = cursor.fetchone()[0]

    # If the date is none, we will query all the data from the power_consumption table
    # and the api_power_usage tables 
    df = pd.DataFrame()
    df_api = pd.DataFrame()
    if max_timestamp is None:
        df = pd.read_sql("SELECT * FROM power_consumption", conn)
        df_api = pd.read_sql("SELECT * FROM api_power_usage", conn)
    else:
        # Queryting the data from the power_consumption table
        df = pd.read_sql(f"SELECT * FROM power_consumption WHERE timestamp > '{max_timestamp}'", conn)
        df_api = pd.read_sql(f"SELECT * FROM api_power_usage_analytics WHERE timestamp > '{max_timestamp}'", conn)

    # If the dataframes are empty, we return
    if df.empty or df_api.empty:
        return

    # Renaming the df_api columns power_usage_5_minutes_ahead to power_usage_5_minutes_ahead_forecast 
    # Renaming the df_api columns power_usage_15_minutes_ahead to power_usage_15_minutes_ahead_forecast
    # Renaming the df_api columns power_usage_60_minutes_ahead to power_usage_60_minutes_ahead_forecast
    df_api = df_api.rename(columns={
        'power_usage_5_minutes_ahead': 'power_usage_5_minutes_ahead_forecast',
        'power_usage_15_minutes_ahead': 'power_usage_15_minutes_ahead_forecast',
        'power_usage_60_minutes_ahead': 'power_usage_60_minutes_ahead_forecast'
    })

    # Dropping the columns status_code, request, created_datetime, updated_datetime
    df_api = df_api.drop(columns=['response_status_code', 'request',  'created_datetime', 'updated_datetime', 'id'])

    # Dropping the created_datetime and updated_datetime columns
    df = df.drop(columns=['created_datetime', 'updated_datetime', 'id'])

    # Merging the dataframes on timestamp
    df = df.merge(df_api, on='timestamp', how='inner')

    # Sorting by timestamp, version 
    df = df.sort_values(['timestamp', 'endpoint', 'version'])

    # Dropping the rows with None forecasts 
    df = df.dropna(subset=['power_usage_5_minutes_ahead_forecast', 'power_usage_15_minutes_ahead_forecast', 'power_usage_60_minutes_ahead_forecast'])

    # Uploading the data to psql 
    for i, row in tqdm(df.iterrows(), total=df.shape[0], desc="Uploading the data to PSQL"):
        # Extracting the row as a dictionary
        row_dict = row.to_dict()

        # Getting the current date 
        now = datetime.datetime.now()

        # Creating the query 
        query = f"""
            INSERT INTO api_power_usage_analytics (
                timestamp, 
                endpoint, 
                version, 
                power_usage_5_minutes_ahead,
                power_usage_15_minutes_ahead,
                power_usage_60_minutes_ahead,
                power_usage_5_minutes_ahead_forecast, 
                power_usage_15_minutes_ahead_forecast, 
                power_usage_60_minutes_ahead_forecast,
                created_datetime,
                updated_datetime
            ) VALUES (
                '{row_dict['timestamp']}', 
                '{row_dict['endpoint']}', 
                '{row_dict['version']}', 
                {row_dict['power_usage_5_minutes_ahead']},
                {row_dict['power_usage_15_minutes_ahead']},
                {row_dict['power_usage_60_minutes_ahead']},
                {row_dict['power_usage_5_minutes_ahead_forecast']}, 
                {row_dict['power_usage_15_minutes_ahead_forecast']}, 
                {row_dict['power_usage_60_minutes_ahead_forecast']},
                '{now}',
                '{now}'
            )
        """

        # Executing the query 
        cursor.execute(query)

    # Commiting the changes 
    conn.commit()

if __name__ == "__main__":
    main()
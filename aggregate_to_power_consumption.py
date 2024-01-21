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

# Defining the feature names 
FEATURES = [
    'power_usage', 
    'current', 
    'voltage'
]

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
    
    # Getting the newest datetime from the 'power_consumption' table
    cursor.execute("SELECT MAX(timestamp) FROM power_consumption")
    max_timestamp = cursor.fetchone()[0]

    # Defining the default query (max_timestamp - 60 minutes)
    query = f"SELECT timestamp, power_usage FROM electricity_timeseries WHERE timestamp > '{max_timestamp}'"

    # If the max_timestamp is null, we download all the 'electricity_timeseries' table
    if max_timestamp is None:
        # Creating the query
        query = f"SELECT timestamp, power_usage FROM electricity_timeseries"

    # Executing the query
    cursor.execute(query)

    # Fetching the data
    data = cursor.fetchall()

    # Creating the dataframe
    timeseries = pd.DataFrame(data, columns=["timestamp", "power_usage"])

    # Sorting the dataframe
    timeseries = timeseries.sort_values("timestamp")

    # Creating the 5, 15 and 60 minutes ahead sum power_usage features
    for minutes in [5, 15, 60]:
        # Creating the feature name
        feature_name = f"power_usage_{minutes}_minutes_ahead"

        # Creating the feature
        series = timeseries["power_usage"].rolling(minutes).sum()

        # Appending the feature to the dataframe
        timeseries[feature_name] = series.shift(-minutes)

    # Dropping the NaN values
    timeseries = timeseries.dropna()

    # Inspecting whether the dataframe is empty
    if timeseries.shape[0] == 0:
        logging.info("The dataframe is empty; Returning")
        return
    
    # Uploading the sql 
    for index, row in tqdm(timeseries.iterrows(), total=len(timeseries)):
        # Getting the current datetime 
        current_datetime = row["timestamp"]

        # Creating the values 
        values = (
            row['timestamp'],
            row['power_usage_5_minutes_ahead'],
            row['power_usage_15_minutes_ahead'],
            row['power_usage_60_minutes_ahead'],
            current_datetime,
            current_datetime,
        )

        # Creating the uplaod query 
        query = f"""
            INSERT INTO power_consumption (
                timestamp, 
                power_usage_5_minutes_ahead, 
                power_usage_15_minutes_ahead, 
                power_usage_60_minutes_ahead, 
                created_datetime, 
                updated_datetime
            ) VALUES (
                '{values[0]}', 
                '{values[1]}', 
                '{values[2]}', 
                '{values[3]}', 
                '{values[4]}', 
                '{values[5]}'
            )
        """

        # Executing the query
        cursor.execute(query)

    # Commiting the changes
    conn.commit()

if __name__ == '__main__': 
    main()
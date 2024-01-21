# Blob wrangling
from azure.storage.blob import BlobServiceClient

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

    # Extracting the connection string and the container name from the .env file 
    connection_string = os.getenv("AZURE_BLOB_CONNECTION_STRING")
    container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME")

    # Extrating the aggregated feature path 
    aggregated_feature_path = os.getenv("AZURE_ML_DATASET_PATH")

    # PSQL credentials
    db_user = os.getenv('PSQL_USER', 'default_user')
    db_pass = os.getenv('PSQL_PASSWORD', 'default_pass')
    db_host = os.getenv('PSQL_HOST', 'localhost')
    db_name = os.getenv('PSQL_DATABASE', 'default_db')
    db_port = os.getenv('PSQL_PORT', '5432')

    try:
        # Creating the blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Extracting the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Printing that the connection was successfull
        logging.info("The connection was successfull")

    except: 
        # Printing that the connection was not successfull 
        logging.warn("The connection was not successfull")

        return 
    
    # Connecting to psql
    try:
        conn = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port, database=db_name)
        cursor = conn.cursor()
        logging.info("Connected to PSQL")
    except:
        logging.warn("Could not connect to PSQL")
        return
    
    # Listing all the blobs in the container; 
    all_blobs = container_client.list_blobs(name_starts_with=aggregated_feature_path)

    # Extracting the blob names
    blob_names = get_blob_names(all_blobs)

    # Reading all the data in the blobs 
    blob_data = pd.DataFrame({})
    for blob_name in tqdm(blob_names):
        # Downloading the blob
        blob = container_client.download_blob(blob=blob_name)

        # Trying to read the blob
        try:
            blob_data = pd.concat([blob_data, pd.read_parquet(io.BytesIO(blob.readall()))])
        except:
            logging.warn(f"Could not read blob {blob_name}")
            continue

    # Grouping by year, month, day, hour, minute and getting the mean of the features
    blob_data = blob_data.groupby(['year', 'month', 'day', 'hour', 'minute'], as_index=False)[FEATURES].mean()

    # Creating the timestamp column 
    blob_data['timestamp'] = pd.to_datetime(blob_data[['year', 'month', 'day', 'hour', 'minute']])

    # dropping the year, month, day, hour and minute columns
    blob_data.drop(columns=['year', 'month', 'day', 'hour', 'minute'], inplace=True)

    # Getting the max timestamp from the database table called "electricity_timeseries"
    cursor.execute("SELECT MAX(timestamp) FROM electricity_timeseries")
    max_timestamp = cursor.fetchone()[0]
    
    # If the timestamp is not null, then we need to filter the data
    if max_timestamp:
        # Filtering the data 
        blob_data = blob_data[blob_data['timestamp'] > max_timestamp]

    # If there is no data, then we can return
    if blob_data.shape[0] == 0:
        logging.info("No new data to upload")
        return
    
    # If the data is not null, we upload row by row the data to the database
    for _, row in tqdm(blob_data.iterrows(), total=blob_data.shape[0], desc="Uploading data to PSQL"):
        # Getting the current datetime 
        now = datetime.datetime.now()

        # Creating the upload values 
        values = (
            row['timestamp'], 
            row['power_usage'], 
            row['current'], 
            row['voltage'], 
            now, 
            now
        )

        # Creating the query
        query = f"INSERT INTO electricity_timeseries (timestamp, power_usage, current, voltage, created_datetime, updated_datetime) VALUES (%s, %s, %s, %s, %s, %s)"

        # Executing the query
        cursor.execute(query, values)

    # Commiting the changes
    conn.commit()

if __name__ == '__main__': 
    main()
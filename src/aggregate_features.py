# Blob wrangling
from azure.storage.blob import BlobServiceClient
from azureml.core import Workspace, Dataset

# Azure authentification
from azureml.core.authentication import ServicePrincipalAuthentication

# Dotenv loading 
from dotenv import load_dotenv

# OS traversal 
import os 
import shutil

# Date wrangling 
import datetime

# Avro reading 
import fastavro

# Arg parsing 
import argparse

# Typehinting 
from typing import Union

# Input/output stream
import io

# Iteration tracking 
from tqdm import tqdm

# From the constant file importing the seconds in a day
from src.constants import SECONDS_IN_DAY, DAYS_IN_YEAR

# Numpy functionalities
import numpy as np

# Importing blob functionalities
from src.blobs import get_blob_names, get_delta_blobs

# Dataframes
import pandas as pd 

# Tempdir 
import tempfile

def extract_features(record: dict) -> dict:
    """
    Creates the features used for the aggregation
    
    Arguments
    ---------

    record: dict
        Dictionary containing the record
    """
    # Getting the Body from the record 
    if "Body" not in record.keys():
        return {}
    
    # Extracting the body
    body = record["Body"]

    # Converting the body that is in bytes to a dictionary 
    body = eval(body.decode("utf-8"))

    # Converting the timestamp to a datetime object
    timestamp = datetime.datetime.strptime(body["timestamp"], "%Y-%m-%d %H:%M:%S.%f")

    # Extracting the features
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day

    # Calculating which day of the year it is 
    day_of_year = timestamp.timetuple().tm_yday

    # Calculating what the seconds of the day it is 
    second_of_day = (timestamp - datetime.datetime(year, month, day)).total_seconds()

    # Getting the sin and cos signals for the day of the year and the second of the day
    sin_day_of_year = np.sin(2 * np.pi * day_of_year / DAYS_IN_YEAR)
    cos_day_of_year = np.cos(2 * np.pi * day_of_year / DAYS_IN_YEAR)
    sin_second_of_day = np.sin(2 * np.pi * second_of_day / SECONDS_IN_DAY)
    cos_second_of_day = np.cos(2 * np.pi * second_of_day / SECONDS_IN_DAY)

    # Creating the features dictionary
    features = {
        "sin_day_of_year": sin_day_of_year,
        "cos_day_of_year": cos_day_of_year,
        "sin_second_of_day": sin_second_of_day,
        "cos_second_of_day": cos_second_of_day
    }

    # Appending the power_usage, voltage and current features
    for key in ["power_usage", "voltage", "current"]:
        features[key] = body[key]

    # Returning the features
    return features

# Defining the function to aggregate the features 
def main(delta_hours: Union[int, None]) -> None: 
    """
    Function that reads the raw streaming data and aggregates it 
    
    Arguments
    ---------
    delta_hours: int
        The number of hours to look back in time to aggregate the features
    """
    # Infering the current file directory 
    current_file_directory = os.path.dirname(os.path.abspath(__file__))

    # Loading the .env file from the parent directory
    load_dotenv(os.path.join(current_file_directory, "..", ".env"))

    # Extracting the connection string and the container name from the .env file 
    connection_string = os.getenv("AZURE_BLOB_CONNECTION_STRING")
    container_name = os.getenv("AZURE_BLOB_CONTAINER_NAME")

    # Extrating the aggregated feature path 
    aggregated_feature_path = os.getenv("AZURE_ML_DATASET_PATH")

    # Extracting the azure ml workspace credentials
    azure_ml_workspace_name = os.getenv("AZURE_ML_WORKSPACE_NAME")
    azure_ml_workspace_tenant_id = os.getenv("AZURE_ML_WORKSPACE_TENANT_ID")
    azure_ml_workspace_secret_id = os.getenv("AZURE_ML_WORKSPACE_SECRET_ID")
    azure_ml_workspace_secret_value = os.getenv("AZURE_ML_WORKSPACE_SECRET_VALUE")

    # Creating the blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Extracting the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Listing all the blobs in the container; The blobs should be created in the past 24 hours
    blobs = container_client.list_blobs(name_starts_with="flexitricity/")

    # Extracting the blob names
    blob_names = get_blob_names(blobs)

    # Extracting the delta blobs
    delta_blob_names = get_delta_blobs(blob_names, delta_hours)

    # Creating an empty list to store the aggregated features
    aggregated_features = []

    # Iterating over the delta blob names and extracting the features
    for blob_name in tqdm(delta_blob_names, desc="Extracting the features"):
        # Extracting the blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Downloading the blob
        blob = blob_client.download_blob().readall()

        # Creating a fastavro reader
        avro_reader = fastavro.reader(io.BytesIO(blob))

        # Iterating over the records in the blob
        for record in avro_reader:
            # Extracting the features
            features = extract_features(record)

            # Appending the features to the list
            aggregated_features.append(features)

    # Converting to a parquet file
    aggregated_features = pd.DataFrame(aggregated_features)

    # Listing all the blobs with the name aggregated_feature_path
    blobs = container_client.list_blobs(name_starts_with=f"{aggregated_feature_path}/")
    blob_names = get_blob_names(blobs)

    # Creating the name for the blob for upload 
    feature_blob_name = f"{aggregated_feature_path}/aggregated_features_{len(blob_names) + 1}.parquet"

    # Creating a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Saving the dataframe to a parquet file
        aggregated_features.to_parquet(os.path.join(temp_dir, "aggregated_features.parquet"))

        # Uploading the parquet file to the blob storage
        container_client.upload_blob(name=feature_blob_name, data=open(os.path.join(temp_dir, "aggregated_features.parquet"), "rb"))


if __name__ == '__main__': 
    # Creating the argument parser
    parser = argparse.ArgumentParser(description="Aggregate the features from the raw streaming data")

    # Adding the arguments to the parser
    parser.add_argument("--delta_hours", type=Union[int, None], help="The number of hours to look back in time to aggregate the features", default=None)

    # Parsing the arguments
    args = parser.parse_args()

    # Extracting the delta hours
    delta_hours = args.delta_hours

    # Calling the main function
    main(delta_hours=delta_hours)
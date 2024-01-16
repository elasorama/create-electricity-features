# Typehinting 
from typing import Union

# Datetime 
import datetime

# Getting all the names for the blobs 
def get_blob_names(blobs) -> list:
    """
    Function that extracts the blob names from a list of blobs
    """
    # Creating an empty list to store the blob names 
    blob_names = []

    # Iterating over the blobs and extracting the blob names 
    for blob in blobs:
        blob_names.append(blob.name)

    # Returning the blob names 
    return blob_names

def get_delta_blobs(blob_names: list, delta_hours: Union[int, None]) -> list:
    """
    Only leaves the names of the blobs that are within the delta hours; 

    The blob names are in the form: 
    <str>/<str>/<partition>/year/month/day/hour/minute/second.avro

    Arguments
    ---------
    blob_names: list
        List of blob names
    delta_hours: int
        The number of hours to look back in time to aggregate the features
    """
    if delta_hours is None: 
        # Returning the blob names
        return blob_names
    
    # Extracting the current date
    current_date = datetime.datetime.now()

    # Creating an empty list to store the blob names
    delta_blob_names = []

    # Iterating over the blob names and extracting the ones that are within the delta hours
    for blob_name in blob_names:
        # Creating the date for the blob 
        blob_date = blob_name.split('/')
        year = int(blob_date[3])
        month = int(blob_date[4])
        day = int(blob_date[5])
        hour = int(blob_date[6])
        minute = int(blob_date[7])
        second = int(blob_date[8].split(".")[0])
        blob_date = datetime.datetime(year, month, day, hour, minute, second)

        # Calculating the difference in hours
        delta = current_date - blob_date

        # Checking if the difference is within the delta hours
        if delta <= datetime.timedelta(hours=delta_hours):
            # Appending the blob name to the list
            delta_blob_names.append(blob_name)

    # Returning the delta blob names
    return delta_blob_names
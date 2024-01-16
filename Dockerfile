# Python 3.11 image
FROM python:3.11 

# Adding the .env file, requirements.txt, aggregage_features.py and blobs.py files to container
WORKDIR /app
COPY requirements.txt /app
COPY .env /app
COPY aggregate_features.py /app
COPY blobs.py /app

# Installing the requirements
RUN pip install -r requirements.txt

# Running the command aggregate_features 
CMD ["python", "-m", "aggregate_features"]
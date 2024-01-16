# Create electricity features

Project that creates data generated from the https://github.com/elasorama/mock-electricity-data repo. 

# Virtual environment 

All the packages are in the requirements.txt file. To create a virtual environment with all the packages, run the command: 

```
python3.11 -m venv electricity-features-env
```

To activate the virtual environment, run the command: 

```
# Bash
source electricity-features-env/bin/activate

# Powershell
.\electricity-features-env\Scripts\activate.ps1
```

# Aggregating the features 

The features are aggregated using the `aggregate_features.py` script. The script has the following arguments: 

* delta_hours - The number of hours to aggregate the data. Default: None

To run the feature aggregation, run the command: 

```
pythona -m aggregate_features --delta_hours 24
```

# Container 

To build the container, run the command: 

```
docker build -t electricity-features .
```

To run the container, run the command: 

```
docker run electricity-features
```
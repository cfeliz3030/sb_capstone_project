
# Capstone Project: Data Pipeline Cloud
This ETL pipeline collects NYC taxi trip record data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page. These records are then processed using PySpark and saved on the cloud.

## Built With
Python 3.7\
AWS EMR Cluster

## Prerequisites
* Sign up for an AWS account, and setup a S3 bucket with input/output folders. Also setup a EMR Spark Cluster and bootstrap above shell script to install dependecies.
https://aws.amazon.com
* Sign up for an AppToken to retrieve data. You will need your username, password, and apptoken.
https://dev.socrata.com/foundry/data.cityofnewyork.us/h9gi-nx95
* Sign up for an account with Open Weather in order to retrieve weather data. You will need a API key.
https://openweathermap.org/api
* pyspark
``` pip install pyspark```
* pyarrow: 
```pip install pyarrow```
* requests
```pip install requests```
* sodapy
```pip install sodapy```
* uszipcode
```pip install uszipcode```
* boto3
```pip install boto3```
* meteostat
```pip install meteostat```
* Python 3.7+


## Usage
* Run nyc_weather etl spark job. Please include command line arguments (start_year,end_year, input,output). See example below\
```spark-submit --py-files s3://taxi-data-pipeline/taxi_pipeline_scripts/weather_etl_config.py s3://taxi-data-pipeline/taxi_pipeline_scripts/weather_etl.py 2017 2020 s3://taxi-data-pipeline/taxi-raw-data s3://taxi-data-pipeline/taxi-processed-data```
* Run nyc_collisions etl spark job. Please include command line arguments (input,output). See example below\
``` spark-submit --py-files s3://taxi-data-pipeline/taxi_pipeline_scripts/collisions_utils.py,s3://taxi-data-pipeline/taxi_pipeline_scripts/collisions_collection.py,s3://taxi-data-pipeline/taxi_pipeline_scripts/collisions_processing.py s3://taxi-data-pipeline/taxi_pipeline_scripts/collisions_main.py s3://taxi-data-pipeline/taxi-raw-data s3://taxi-data-pipeline/taxi-processed-data```
* Run nyc_taxi etl spark job. Please include command line arguments (start_year,end_year, input,output). See example below\
``` spark-submit --py-files s3a://taxi-data-pipeline/taxi_pipeline_scripts/config.py,s3a://taxi-data-pipeline/taxi_pipeline_scripts/taxi_utils.py s3a://taxi-data-pipeline/taxi_pipeline_scripts/taxi-data-pipeline.py 2017 2019 s3://taxi-data-pipeline/taxi-raw-data s3://taxi-data-pipeline/taxi-processed-data```

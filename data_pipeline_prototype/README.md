# Capstone Project: Data Pipeline Prototype
This ETL pipeline collects NYC taxi trip record data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page. These records are then processed using PySpark and saved locally. For future iterations I will incorporate the use of a cloud service.

## Built With
Python 3.7

## Prerequisites
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
* Python 3.7+

## Installation
* Clone the repo
``` git clone https://github.com/cfeliz3030/sb_capstone_project.git```

## Usage
* Open config.py file and enter the desired filepath for saving all the files. After, input the credentials needed for connecting to Socrata(apptoken,username,password) and OpenWeather(API key). Lastly, enter the range of years that you would like to download taxi data for. Taxi data is available from 2015-2020. Run 'config.py'.\
``` python3 config.py ```
* Run 'data_collection.py' to create functions needed for collecting and saving multiple data sources.\
``` python3 data_collection.py```
* Run 'data_processing.py' to create functions which process multiple data sources.\
``` python3 data_processing.py ```
* Lastly, we can run 'main.py' to run the data pipeline.\
``` python3 main.py```

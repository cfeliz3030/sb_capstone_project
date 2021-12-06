import glob
import logging
from datetime import datetime
import requests
from config import user_filepath
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import os
import time
import pandas as pd
from sodapy import Socrata


logger = logging.getLogger(__name__)
file = logging.FileHandler(f'{user_filepath}/data_collection.log', mode='a')
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
file.setFormatter(special_format)
logger.addHandler(file)


def taxi_files_list():
    filepath = user_filepath
    f = glob.glob(filepath + '/taxi_data_20[0-9][0-9].csv')
    return f


def taxi_data_mappings():
    """ Collects files needed for taxi data mappings."""
    save_path = user_filepath

    df = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')
    df.to_csv(save_path + '/taxi_zones.csv', index=False)

    df_2 = pd.read_csv('https://data.beta.nyc/dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/7caac650-d082'
                       '-4aea-9f9b-3681d568e8a5/download/nyc_zip_borough_neighborhoods_pop.csv')
    df_2.to_csv(save_path + '/zip_data_2.csv', index=False)

    df_3 = pd.read_html('https://worldpopulationreview.com/zips/new-york')
    df_3 = df_3[0]
    df_3.to_csv(save_path + '/zip_data_1.csv', index=False)


def taxi_data_collect_year(year):
    """
    Collect taxi data for specified year from NY TLC
    Years available 2015-2020
    ex.https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-04.csv
    :param year: int
    :return:
    """
    start = time.time()
    month = 0
    save_path = user_filepath
    file_name = f'taxi_data_{year}'
    complete_name = os.path.join(save_path, file_name + ".csv")
    logger.info('Collecting NYC Taxi Data')
    while year:
        month += 1
        if month < 10:
            str_month = str(month).zfill(2)
        else:
            str_month = str(month)
        logger.info(f'Downloading {str_month} {year}')
        url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{str_month}.csv'
        r = requests.get(url, stream=True)

        with open(complete_name, 'ab') as f:
            for chunk in r.iter_content(10000):
                f.write(chunk)
        logger.info(f'Finished {str_month} {year}')
        if month == 12:
            break
    end = time.time()
    logger.info(f'{year} File Download Complete, execution time in minutes: {(end - start) // 60}')


def collect_vehicle_collisions(apptoken, username, password):
    """ Collects vehicle collision data using Socrata API."""
    start = time.time()
    logger.info('Collecting Vehicle Collisions Data')
    client = Socrata('data.cityofnewyork.us',
                     app_token=apptoken,
                     username=username,
                     password=password)

    logger.info('Connecting to Socrata API')
    results = client.get("h9gi-nx95", limit=2000000, content_type='csv')

    save_path = user_filepath
    file_name = 'vehicle_collisions.csv'
    complete_name = os.path.join(save_path, file_name)

    logger.info('Writing data to file')
    with open(complete_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(results)

    logger.info('Finished collecting new collision data.')
    end = time.time()
    logger.info("Collisions data complete,Execution time in minutes: %s ", (end - start) // 60)


def collect_borough_weather(api_key):
    """ Collects weather data using open weather API."""
    logger.info('Collecting borough weather Data')
    list_of_boroughs = ['Queens', 'Manhattan', 'Bronx', 'Staten Island', 'Brooklyn']

    df_current = pd.DataFrame(
        columns=['Time', 'Location', 'Temp', 'Feels like', 'Main', 'Description', 'Visibility', 'Wind Speed'])
    logger.info('Connecting to weather data API')
    for b in list_of_boroughs:
        link = f'https://api.openweathermap.org/data/2.5/weather?q={b}&units=imperial&APPID={api_key}'
        r = requests.get(link)
        weather = r.json()
        weather_dict = {'Location': weather['name'],
                        'Temp': weather['main']['temp'],
                        'Feels like': weather['main']['feels_like'],
                        'Main': weather['weather'][0]['main'],
                        'Description': weather['weather'][0]['description'],
                        'Visibility': weather['visibility'],
                        'Wind Speed': weather['wind']['speed'],
                        'Time': datetime.fromtimestamp(weather['dt'])}
        df_current = df_current.append(weather_dict, ignore_index=True)
    logger.info('Writing weather data to file')
    table = pa.Table.from_pandas(df_current)
    pq.write_to_dataset(
        table,
        root_path=user_filepath + '/weather_data_boroughs.parquet',
        partition_cols=['Location'])
    logger.info('Finished collecting new weather data.')


def collect_traffic_sensor():
    """ Collects live data feed from NYC traffic sensors."""
    logger.info('Collecting traffic sensor Data')
    url = 'http://207.251.86.229/nyc-links-cams/LinkSpeedQuery.txt'
    df = pd.read_csv(url, sep='\t')
    logger.info('Writing traffic sensor data to file')
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=user_filepath + '/traffic_sensor_feed.parquet',
        partition_cols=['Borough'])
    logger.info('Finished collecting new traffic sensor data.')

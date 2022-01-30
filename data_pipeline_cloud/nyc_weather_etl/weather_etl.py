import logging
import sys
from datetime import datetime
from weather_etl_config import user_filepath, output_filepath, start_year,end_year
import pandas as pd
from meteostat import Point, Hourly, units

logger = logging.getLogger(__name__)
streamHandler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
streamHandler.setFormatter(special_format)
logger.addHandler(streamHandler)


def collect_nyc_weather(start, end):
    """ Collects hourly historical weather for NYC"""
    logger.info('Collecting NYC historical weather Data')
    # Create Point for NYC, NY
    nyc = Point(40.730610, -73.935242)
    start = datetime(start, 1, 1)
    end = datetime(end, 12, 31, 23, 59)

    data = Hourly(nyc, start, end)
    data = data.convert(units.imperial)
    data = data.fetch()
    logger.info('Writing weather data to file')
    data.to_parquet(user_filepath + '/weather_data_nyc.parquet')
    logger.info('Finished collecting weather data.')


def process_nyc_weather():
    """ Processes weather data and outputs parquet file."""
    logger.info('Processing weather data.')
    filepath = user_filepath + '/weather_data_nyc.parquet/'
    df = pd.read_parquet(filepath)
    df.drop(columns=['tsun', 'coco'], inplace=True)
    logger.info('Creating new file for processed data.')
    df.to_parquet(output_filepath + '/processed_weather_data_nyc.parquet')
    logger.info('Finished processing weather data.')


collect_nyc_weather(start_year, end_year)

process_nyc_weather()

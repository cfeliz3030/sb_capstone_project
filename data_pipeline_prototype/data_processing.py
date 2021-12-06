from datetime import date, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pyspark.sql.functions import udf, concat_ws, when, hour
from pyspark.sql.types import TimestampType
from uszipcode import SearchEngine
from config import taxi_loc_id_schema, taxi_lat_lon_schema, collision_schema, name, user_filepath
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
file = logging.FileHandler(f'{user_filepath}/data_processing.log', mode='a')
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
file.setFormatter(special_format)
logger.addHandler(file)


def spark_session(session_name):
    """ Creates spark session object"""
    spark = SparkSession.builder.appName(str(session_name)).getOrCreate()
    return spark


def process_weather_data():
    """ Processes weather data and outputs parquet file."""
    logger.info('Processing weather data.')
    filepath = user_filepath + '/weather_data_boroughs.parquet'
    df = pd.read_parquet(filepath)
    bronx_dict = {'Bronx County': 'Bronx'}
    df['Location'] = df['Location'].apply(lambda x: bronx_dict[x] if x in bronx_dict.keys() else x)
    # keep weather from last 24 hrs?
    logger.info('Creating new file for processed data.')
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=user_filepath + '/processed_weather_data_boroughs.parquet',
        partition_cols=['Location'])
    logger.info('Finished processing weather data.')


def process_sensor_data():
    """ Processes sensor data and outputs parquet file."""
    logger.info('Processing sensor data.')
    filepath = user_filepath + '/traffic_sensor_feed.parquet'
    df = pd.read_parquet(filepath)
    df['DataAsOf'] = pd.to_datetime(df['DataAsOf'])
    df.drop(columns=['Status', 'linkId', 'Transcom_id', 'Owner', 'EncodedPolyLine', 'EncodedPolyLineLvls'],
            inplace=True)
    date_filter = date.today() - timedelta(1)  # 1 day buffer
    date_filter = pd.to_datetime(date_filter)
    df = df[df['DataAsOf'] > date_filter]
    logger.info('Creating new file for processed data.')
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=user_filepath + '/processed_traffic_sensor_feed.parquet',
        partition_cols=['Borough'])
    logger.info('Finished processing sensor data.')


def process_collision_data():
    """ Processes vehicle collisions data and outputs csv file."""
    logger.info('Processing collisions data.')
    spark = spark_session(name)
    df = spark.read.option('header', 'true').csv(
        f'{user_filepath}/vehicle_collisions.csv', schema=collision_schema)
    # df = df.limit(5000)
    df = df.dropna(subset=['crash_date'])
    # combine crash date + crash time columns
    df = df.withColumn('crash_date_time', concat_ws(' ', 'crash_date', 'crash_time'))
    df = df.withColumn('crash_date_time', df.crash_date_time.cast(TimestampType()))
    # create rush hour flag columns,rush hour 8-9am or 3-7pm
    df = df.withColumn(
        'rush_hour',
        when((hour(df.crash_date_time) >= 8) & (hour(df.crash_date_time) < 10), 1).when(
            (hour(df.crash_date_time) >= 15) & (hour(df.crash_date_time) < 20), 1).otherwise(0))
    logger.info('Creating new file for processed data.')
    df.coalesce(1).write.option("header", True).csv(f'{user_filepath}/processed_vehicle_collisions')
    logger.info('Finished processing collisions data.')


def get_zipcode(lat, lon):
    """ This function intakes a set of (lat,long) coordinates and returns the zipcode location."""
    search = SearchEngine(simple_zipcode=True)
    result = search.by_coordinates(lat=lat, lng=lon, returns=1)
    try:
        result = result[0].zipcode
    except:
        result = 0
    return result


def get_zipcode_mappings():
    """ Retrieves zipcode files and outputs as dictionary."""
    logger.info('Retrieving zipcode mappings.')
    save_path = user_filepath
    # load zip code mapping #1
    zip_data = pd.read_csv(save_path + '/zip_data_1.csv')
    zip_data = zip_data[zip_data['County'].isin(['Queens', 'Kings', 'Bronx', 'New York', 'Richmond'])]

    zip_data['County'] = zip_data['County'].map({'Queens': 'Queens', 'Kings': 'Brooklyn', 'Bronx': 'Bronx',
                                                 'New York': 'Manhattan', 'Richmond': 'Staten Island'})

    zipcode_dic = dict(zip(zip_data['Zip Code'], zip_data['County']))
    zipcode_dic[0] = 'Unknown'
    # load zip code mapping #2
    zip_data_2 = pd.read_csv(save_path + '/zip_data_2.csv')
    zipcode_dic_2 = dict(zip(zip_data_2['zip'], zip_data_2['borough']))
    zipcode_dic.update(zipcode_dic_2)
    # master dictionary with zip code to borough key:value pairs
    return zipcode_dic


def get_taxi_zones():
    """ Retrieves taxi zone file and outputs as dictionary."""
    logger.info('Retrieving taxi zones.')
    save_path = user_filepath
    location_data_map = pd.read_csv(save_path + '/taxi_zones.csv')
    location_dic = dict(zip(location_data_map['LocationID'], location_data_map['Borough']))
    return location_dic


def check_header(filename):
    """ Returns first/header row of file."""
    with open(filename) as f:
        first = f.readlines(1)
        first = ' '.join(first).split(',')
    return first


def process_taxi_data(data_path):
    """ Processes taxi data and outputs csv file."""
    logger.info(f'Processing taxi data {data_path[-8:-4]}.')
    spark = spark_session(name)
    header = check_header(data_path)

    if 'pickup_longitude' in header:
        zipcode_dic = get_zipcode_mappings()
        df = spark.read.option('header', 'true').csv(data_path, schema=taxi_lat_lon_schema)
        # df = df.limit(5000)
        udf_get_zipcode = udf(get_zipcode)
        udf_zipcode_map = udf(lambda x: zipcode_dic[int(x)] if int(x) in list(zipcode_dic.keys()) else 'Unknown')

        df = df.withColumn('PULocationID', udf_get_zipcode('pickup_latitude', 'pickup_longitude'))
        df = df.withColumn('DOLocationID', udf_get_zipcode('dropoff_latitude', 'dropoff_longitude'))

        df = df.withColumn('PULocationID', udf_zipcode_map('PULocationID'))
        df = df.withColumn('DOLocationID', udf_zipcode_map('DOLocationID'))

        df = df.drop('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude')

    else:
        location_dic = get_taxi_zones()
        df = spark.read.option('header', 'true').csv(data_path, schema=taxi_loc_id_schema)
        # df = df.limit(5000)
        location_map_udf = udf(lambda x: location_dic[x])

        df = df.withColumn('PULocationID', location_map_udf('PULocationID'))
        df = df.withColumn('DOLocationID', location_map_udf('DOLocationID'))

    # collected from source data dictionary, mapping payment types
    payment_dict = {1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}
    udf_payment_map = udf(lambda x: payment_dict[x])
    df = df.withColumn('payment_type', udf_payment_map('payment_type'))

    rate_dict = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare',
                 6: 'Group ride'}
    udf_rate_map = udf(lambda x: rate_dict[x])
    df = df.withColumn('RatecodeID', udf_rate_map('RatecodeID'))

    logger.info('Creating new file for processed data.')
    df.coalesce(1).write.option("header", True).csv(user_filepath + f'/processed_taxi_data/processed_taxi_{data_path[-8:]}')

    logger.info(f'Finished processing taxi data {data_path[-8:-4]}.')

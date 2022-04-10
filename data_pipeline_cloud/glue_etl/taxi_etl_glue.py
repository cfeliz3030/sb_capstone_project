import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField, DateType, TimestampType
from itertools import chain
import boto3
import requests
import io
import botocore
from pyspark.sql.functions import udf, create_map, lit
from uszipcode import SearchEngine
import os
import time
import pandas as pd
import s3fs
import boto3.s3.transfer as s3transfer
from boto3.s3.transfer import TransferConfig
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','user_filepath','output_filepath','start_year','end_year'])
user_filepath=args['user_filepath']
user_output_filepath=args['output_filepath']
start_year=int(args['start_year'])
end_year=int(args['end_year'])

logger = logging.getLogger(__name__)
streamHandler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
streamHandler.setFormatter(special_format)
logger.addHandler(streamHandler)

taxi_lat_lon_schema = StructType(
    [StructField("VendorID", IntegerType(), True),
     StructField("tpep_pickup_datetime", TimestampType(), True),
     StructField("tpep_dropoff_datetime", TimestampType(), True),
     StructField("passenger_count", IntegerType(), True),
     StructField("trip_distance", FloatType(), True),
     StructField("pickup_longitude", FloatType(), True),
     StructField("pickup_latitude", FloatType(), True),
     StructField("RatecodeID", IntegerType(), True),
     StructField("store_and_fwd_flag", StringType(), True),
     StructField("dropoff_longitude", FloatType(), True),
     StructField("dropoff_latitude", FloatType(), True),
     StructField("payment_type", IntegerType(), True),
     StructField("fare_amount", FloatType(), True),
     StructField("extra", FloatType(), True),
     StructField("mta_tax", FloatType(), True),
     StructField("tip_amount", FloatType(), True),
     StructField("tolls_amount", FloatType(), True),
     StructField("improvement_surcharge", FloatType(), True),
     StructField("total_amount", FloatType(), True)]
)

taxi_loc_id_schema = StructType(
    [StructField("VendorID", IntegerType(), True),
     StructField("tpep_pickup_datetime", TimestampType(), True),
     StructField("tpep_dropoff_datetime", TimestampType(), True),
     StructField("passenger_count", IntegerType(), True),
     StructField("trip_distance", FloatType(), True),
     StructField("RatecodeID", IntegerType(), True),
     StructField("store_and_fwd_flag", StringType(), True),
     StructField("PULocationID", IntegerType(), True),
     StructField("DOLocationID", IntegerType(), True),
     StructField("payment_type", IntegerType(), True),
     StructField("fare_amount", FloatType(), True),
     StructField("extra", FloatType(), True),
     StructField("mta_tax", FloatType(), True),
     StructField("tip_amount", FloatType(), True),
     StructField("tolls_amount", FloatType(), True),
     StructField("improvement_surcharge", FloatType(), True),
     StructField("total_amount", FloatType(), True)]
)

def taxi_files_list():
    logger.info(f'Checking for available taxi datasets in {user_filepath} ')
    s3 = s3fs.S3FileSystem(anon=False)
    f = s3.glob(user_filepath + '/taxi_data_[0-9][0-9]-20[0-9][0-9].csv')
    return f


# def upload_log_to_s3():
#     s3 = boto3.client("s3")
#     s3.put_object(Body=log_stringio.getvalue(), Bucket='taxi-data-pipeline', Key='taxi-raw-data/taxi_logs')


def taxi_data_mappings():
    """ Collects files needed for taxi data mappings."""
    logger.info('Collecting taxi data mappings.')
    save_path = user_filepath

    df = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')
    df.to_csv(save_path + '/taxi_zones.csv', index=False)

    df_2 = pd.read_csv('https://data.beta.nyc/dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/7caac650-d082'
                       '-4aea-9f9b-3681d568e8a5/download/nyc_zip_borough_neighborhoods_pop.csv')
    df_2.to_csv(save_path + '/zip_data_2.csv', index=False)

    df_3 = pd.read_html('https://worldpopulationreview.com/zips/new-york')
    df_3 = df_3[0]
    df_3.to_csv(save_path + '/zip_data_1.csv', index=False)
    logger.info('Finished collecting taxi data mappings.')


def taxi_data_collect_year(year,filepath):
    """
    Collect taxi data for specified year from NY TLC
    Years available 2015-2020
    ex.https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-04.csv
    :param year: int
    :return:
    """
    start = time.time()
    s3_bucket = filepath.split('/')[2]
    s3 = boto3.client('s3')
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    month = 0
    save_path = filepath.split('/')[-1]

    logger.info('Collecting NYC Taxi Data')
    while year:
        month += 1
        if month < 10:
            str_month = str(month).zfill(2)
        else:
            str_month = str(month)
        logger.info(f'Downloading {str_month} {year}')

        file_name = f'taxi_data_{str_month}-{year}'
        complete_name = os.path.join(save_path, file_name + ".csv")

        url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{str_month}.csv'
        s = requests.Session()
        r = s.get(url, stream=True)

        bytesIO = io.BytesIO(bytes(r.content))
        with bytesIO as data:
            # s3f.upload(data, s3_bucket, complete_name)

            s3.upload_fileobj(data, s3_bucket, complete_name,
                              Config=config)
        logger.info(f'Finished {str_month} {year}')
        if month == 12:
            break
    end = time.time()
    logger.info(f'{year} File Download Complete, execution time in minutes: {(end - start) // 60}')


def get_zipcode(lat, lon):
    """ This function intakes a set of (lat,long) coordinates and returns the zipcode location."""
    search = SearchEngine()
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
    zipcode_mapping = create_map([lit(x) for x in chain(*zipcode_dic.items())])
    return zipcode_mapping


def get_taxi_zones():
    """ Retrieves taxi zone file and outputs as dictionary."""
    logger.info('Retrieving taxi zones.')
    save_path = user_filepath
    location_data_map = pd.read_csv(save_path + '/taxi_zones.csv')
    location_dic = dict(zip(location_data_map['LocationID'], location_data_map['Borough']))
    location_mapping = create_map([lit(x) for x in chain(*location_dic.items())])
    return location_mapping


def check_header(filename):
    """ Returns first/header row of file."""

    s3 = boto3.resource('s3')
    content = s3.Object(filename.split('/', 1)[0], filename.split('/', 1)[-1]).get()['Body'].read(300)
    header = content.decode('UTF-8', 'ignore')
    x = header.split('\n')
    x = ''.join(list(x[0]))
    return x.split(',')


def process_taxi_data(data_path, spark_session):
    """ Processes taxi data and outputs csv file."""

    logger.info(f'Processing taxi data {data_path[-11:-4]}.')

    spark = spark_session
    spark.conf.set("spark.sql.adaptive.enabled", True)
    header = check_header(data_path)
    data_path = 's3a://' + data_path

    if 'pickup_longitude' in header:
        zipcode_mappings = get_zipcode_mappings()
        df = spark.read.option('header', 'true').csv(data_path, schema=taxi_lat_lon_schema)
        # df = df.limit(500)
        udf_get_zipcode = udf(get_zipcode)

        df = df.withColumn('PULocationID', udf_get_zipcode('pickup_latitude', 'pickup_longitude'))
        df = df.withColumn('DOLocationID', udf_get_zipcode('dropoff_latitude', 'dropoff_longitude'))

        df = df.withColumn('PULocationID', zipcode_mappings[df['PULocationID']])
        df = df.withColumn('DOLocationID', zipcode_mappings[df['DOLocationID']])

        df = df.na.fill({'PULocationID': 'Unknown', 'DOLocationID': 'Unknown'})

        df = df.drop('pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude')

    else:
        location_mapping = get_taxi_zones()
        df = spark.read.option('header', 'true').csv(data_path, schema=taxi_loc_id_schema)
        # df = df.limit(500)

        df = df.withColumn('PULocationID', location_mapping[df['PULocationID']])
        df = df.withColumn('DOLocationID', location_mapping[df['DOLocationID']])

    # collected from source data dictionary, mapping payment types
    payment_dict = {1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}
    payment_mapping = create_map([lit(x) for x in chain(*payment_dict.items())])

    df = df.withColumn('payment_type', payment_mapping[df['payment_type']])

    rate_dict = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare',
                 6: 'Group ride'}
    rate_mapping = create_map([lit(x) for x in chain(*rate_dict.items())])

    df = df.withColumn('RatecodeID', rate_mapping[df['RatecodeID']])

    logger.info('Creating new file for processed data.')

    df.write.mode("overwrite").option("header", True).parquet(
        user_output_filepath + f'/processed_taxi_{data_path[-11:-4]}')

    logger.info(f'Finished processing taxi data {data_path[-11:-4]}.')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.adaptive.enabled", True)

f = taxi_files_list()

if len(f) == 0:
    for y in range(start_year, end_year + 1):
        taxi_data_collect_year(y, user_filepath)

taxi_data_mappings()

f = taxi_files_list()
for file in f:
    process_taxi_data(file, spark)
    
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
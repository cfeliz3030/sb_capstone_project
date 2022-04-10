import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import os
from io import BytesIO
import boto3
import requests
import time
import logging
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField, DateType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import concat_ws, when, hour

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','user_filepath','output_filepath'])
user_filepath=args['user_filepath']
output_filepath=args['output_filepath']

collision_schema = StructType(
    [StructField("crash_date", DateType(), True),
     StructField("crash_time", StringType(), True),
     StructField("borough", StringType(), True),
     StructField("zip_code", IntegerType(), True),
     StructField("latitude", FloatType(), True),
     StructField("longitude", FloatType(), True),
     StructField("location", StringType(), True),
     StructField("on_street_name", StringType(), True),
     StructField("off_street_name", StringType(), True),
     StructField("cross_street_name", StringType(), True),
     StructField("number_of_persons_injured", IntegerType(), True),
     StructField("number_of_persons_killed", IntegerType(), True),
     StructField("number_of_pedestrians_injured", IntegerType(), True),
     StructField("number_of_pedestrians_killed", IntegerType(), True),
     StructField("number_of_cyclist_injured", IntegerType(), True),
     StructField("number_of_cyclist_killed", IntegerType(), True),
     StructField("number_of_motorist_injured", IntegerType(), True),
     StructField("number_of_motorist_killed", IntegerType(), True),
     StructField("contributing_factor_vehicle_1", StringType(), True),
     StructField("contributing_factor_vehicle_2", StringType(), True),
     StructField("contributing_factor_vehicle_3", StringType(), True),
     StructField("contributing_factor_vehicle_4", StringType(), True),
     StructField("contributing_factor_vehicle_5", StringType(), True),
     StructField("collision_id", IntegerType(), True),
     StructField("vehicle_type_code1", StringType(), True),
     StructField("vehicle_type_code2", StringType(), True),
     StructField("vehicle_type_code_3", StringType(), True),
     StructField("vehicle_type_code_4", StringType(), True),
     StructField("vehicle_type_code_5", StringType(), True)]
)



logger = logging.getLogger(__name__)
streamHandler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
streamHandler.setFormatter(special_format)
logger.addHandler(streamHandler)


def collect_vehicle_collisions():
    """ Collects vehicle collision data using Socrata API."""
    start = time.time()
    logger.info('Collecting Vehicle Collisions Data')

    url = 'https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$limit=2000000&$order=crash_date'
    r = requests.get(url)

    save_path = user_filepath.split('/')[-1]
    file_name = 'vehicle_collisions.csv'
    complete_name = os.path.join(save_path, file_name)

    logger.info('Writing data to file')

    s3_bucket = user_filepath.split('/')[2]
    s3 = boto3.client('s3')

    bytesIO = BytesIO(bytes(r.content))
    with bytesIO as data:
        s3.upload_fileobj(data, s3_bucket, complete_name)

    logger.info('Finished collecting new collision data.')
    end = time.time()
    logger.info("Collisions data complete,Execution time in minutes: %s ", (end - start) // 60)

def process_collision_data(spark_object):
    """ Processes vehicle collisions data and outputs csv file."""
    logger.info('Processing collisions data.')
    spark = spark_object
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
    # df.coalesce(1).write.mode("overwrite").option("header", True).csv(f'{user_filepath}/processed_vehicle_collisions')
    df.write.mode("overwrite").option("header", True).parquet(f'{output_filepath}/processed_vehicle_collisions')
    logger.info('Finished processing collisions data.')

collect_vehicle_collisions()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

process_collision_data(spark)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
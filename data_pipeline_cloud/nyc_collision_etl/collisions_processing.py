import logging
import sys
from pyspark.sql.types import TimestampType
from collisions_utils import user_filepath, output_filepath, collision_schema
from pyspark.sql.functions import concat_ws, when, hour


logger = logging.getLogger(__name__)
# file = logging.FileHandler(f'{user_filepath}/data_collection.log', mode='a')
streamHandler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
streamHandler.setFormatter(special_format)
logger.addHandler(streamHandler)


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
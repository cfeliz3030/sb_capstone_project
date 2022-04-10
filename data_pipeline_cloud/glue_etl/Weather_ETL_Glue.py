import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from meteostat import Point, Hourly, units
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','user_filepath','output_filepath','start_year','end_year'])
user_filepath=args['user_filepath']
output_filepath=args['output_filepath']
start_year=int(args['start_year'])
end_year=int(args['end_year'])

def collect_nyc_weather(start, end):
    """ Collects hourly historical weather for NYC"""

    # Create Point for NYC, NY
    nyc = Point(40.730610, -73.935242)
    start = datetime(start, 1, 1)
    end = datetime(end, 12, 31, 23, 59)

    data = Hourly(nyc, start, end)
    data = data.convert(units.imperial)
    data = data.fetch()
    data.to_parquet(user_filepath + '/weather_data_nyc.parquet')



def process_nyc_weather():
    """ Processes weather data and outputs parquet file."""
    filepath = user_filepath + '/weather_data_nyc.parquet/'
    df = pd.read_parquet(filepath)
    df.drop(columns=['tsun', 'coco'], inplace=True)
    df.to_parquet(output_filepath + '/processed_weather_data_nyc.parquet')



collect_nyc_weather(start_year, end_year)

process_nyc_weather()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
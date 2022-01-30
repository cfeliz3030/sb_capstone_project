from pyspark.sql import SparkSession
import sys
from config import start_year, end_year, user_filepath
from taxi_utils import taxi_files_list, taxi_data_mappings, taxi_data_collect_year, process_taxi_data

# download taxi files if not in s3 bucket
spark = SparkSession.builder.appName('taxi_pipeline').getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", True)

f = taxi_files_list()
if len(f) == 0:
    for y in range(start_year, end_year + 1):
        taxi_data_collect_year(y, user_filepath)

taxi_data_mappings()

f = taxi_files_list()
for file in f:
    process_taxi_data(file, spark)

# upload_log_to_s3()

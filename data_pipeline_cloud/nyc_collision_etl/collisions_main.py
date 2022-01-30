from collisions_collection import collect_vehicle_collisions
from collisions_processing import process_collision_data
from pyspark.sql import SparkSession
import sys

if len(sys.argv) != 3:
    raise Exception('Missing arguments, 2 needed (inputpath,outputpath.')

collect_vehicle_collisions()

spark = SparkSession.builder.appName('collisions_pipeline').getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled",True)

process_collision_data(spark)

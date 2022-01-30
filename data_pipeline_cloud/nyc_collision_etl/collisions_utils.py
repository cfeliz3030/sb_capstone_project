import sys
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField, DateType

# file input/output paths
user_filepath = sys.argv[1]
output_filepath = sys.argv[2]

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

from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField, DateType, TimestampType

# set filepath for storing files
user_filepath = "Enter file path as string."

# taxi data available from 2015-2020, input range below as int
start_year = int
end_year = int

# Socrata API creds
apptoken = 'Enter AppToken'
username = 'Enter Username'
password = 'Enter Password'

# weather API creds
api_key = "Enter API Key"

# pyspark session and schemas
name = 'Enter Spark Session name as string'

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



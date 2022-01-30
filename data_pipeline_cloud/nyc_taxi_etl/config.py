from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField, DateType, TimestampType
import sys

if len(sys.argv) != 5:
    raise Exception('Missing argument 4 needed (start_year,end_year,inputpath,outputpath')

start_year = int(float(sys.argv[1]))
end_year = int(float(sys.argv[2]))
user_filepath = sys.argv[3]
user_output_filepath = sys.argv[4]


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

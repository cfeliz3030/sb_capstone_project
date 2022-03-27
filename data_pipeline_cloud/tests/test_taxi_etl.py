import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark import Row
from pyspark.sql.functions import *
from itertools import chain
from taxi_etl import get_taxi_zones, map_location_values

class TestTaxi(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    def test_data(self):

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
        StructField("tolls_amount", IntegerType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("congestion_surcharge", FloatType(), True)])

        taxi_rides = Row('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
       'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra',
       'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
       'total_amount', 'congestion_surcharge')

        taxi_ride_1 = taxi_rides(1,datetime.strptime('2020-03-01 00:51:35', '%Y-%m-%d %H:%M:%S'),datetime.strptime('2020-03-01 01:00:17', '%Y-%m-%d %H:%M:%S'),1,1.0,1,'N',234,114,1,7.0,3.0,0.5,1.3,0,0.3,12.1,2.5)
        taxi_ride_2 = taxi_rides(1,datetime.strptime('2020-03-01 00:13:42', '%Y-%m-%d %H:%M:%S'),datetime.strptime('2020-03-01 00:23:00', '%Y-%m-%d %H:%M:%S'),4,1.1,1,'N',148,211,1,7.5,3.0,0.5,2.0,0,0.3,13.3,2.5)
        taxi_ride_3 = taxi_rides(1,datetime.strptime('2020-03-01 00:01:25', '%Y-%m-%d %H:%M:%S'),datetime.strptime('2020-03-01 00:08:01', '%Y-%m-%d %H:%M:%S'),2,1.0,1,'N',230,164,1,6.5,3.0,0.5,0.0,0,0.3,10.3,2.5)
        taxi_ride_4 = taxi_rides(1,datetime.strptime('2020-03-01 00:21:14', '%Y-%m-%d %H:%M:%S'),datetime.strptime('2020-03-01 00:30:47', '%Y-%m-%d %H:%M:%S'),1,2.5,1,'N',230,236,1,10.0,3.0,0.5,2.07,0,0.3,15.87,2.5)
        taxi_ride_5 = taxi_rides(1,datetime.strptime('2020-03-01 00:56:56', '%Y-%m-%d %H:%M:%S'),datetime.strptime('2020-03-01 01:13:47', '%Y-%m-%d %H:%M:%S'),0,3.5,1,'N',263,48,1,14.5,3.0,0.5,3.65,0,0.3,21.95,2.5)

        taxi_sample = [taxi_ride_1, taxi_ride_2, taxi_ride_3, taxi_ride_4,taxi_ride_5]
        taxi_sample_df = self.spark.createDataFrame(taxi_sample,schema=taxi_loc_id_schema)

        return taxi_sample_df

    def test_df_shape(self):
        df = self.test_data()
        self.assertEqual(len(df.columns), 18)
        self.assertGreater(df.count(), 1)

    def test_mapping_file_download(self):
        zones = get_taxi_zones()
        self.assertIsNotNone(zones)

    def test_mapping_loc(self):
        df = map_location_values(self.test_data())
        self.assertEqual(dict(df.dtypes)['PULocationID'],'string')
        self.assertEqual(dict(df.dtypes)['DOLocationID'],'string')
    
    def test_mapping_payment(self):
        df = self.test_data()
        payment_dict = {1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}
        payment_mapping = create_map([lit(x) for x in chain(*payment_dict.items())])
        
        df = df.withColumn('payment_type', payment_mapping[df['payment_type']])
        self.assertEqual(dict(df.dtypes)['payment_type'],'string')

    def test_mapping_rate_id(self):
        df = self.test_data()
        rate_dict = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare',
                 6: 'Group ride'}
        rate_mapping = create_map([lit(x) for x in chain(*rate_dict.items())])

        df = df.withColumn('RatecodeID', rate_mapping[df['RatecodeID']])
        self.assertEqual(dict(df.dtypes)['RatecodeID'],'string')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
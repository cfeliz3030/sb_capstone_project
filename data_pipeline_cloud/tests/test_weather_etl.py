
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark import Row
from pyspark.sql.functions import *
from weather_etl import remove_columns


class TestWeather(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    def test_data(self):

        weather_schema = StructType(
        [
        StructField("time", TimestampType(), True),
        StructField("temp", FloatType(), True),
        StructField("wpgt", FloatType(), True),
        StructField("pres", FloatType(), True),
        StructField("tsun", FloatType(), True),
        StructField("wspd", FloatType(), True),
        StructField("dwpt", FloatType(), True),
        StructField("rhum", FloatType(), True),
        StructField("prcp", FloatType(), True),
        StructField("coco", FloatType(), True),
        StructField("snow", FloatType(), True),
        StructField("wdir", FloatType(), True)])

        weather = Row('time','temp','dwpt','rhum','prcp','snow','wdir','wspd','wpgt','pres','tsun','coco')

        weather_1 = weather(datetime.strptime('2017-01-04 02:00:00', '%Y-%m-%d %H:%M:%S'),44.6,42.6,93.0,0.012,None,30.0,7.0,None,1029.5,None,7.0)
        weather_2 = weather(datetime.strptime('2017-01-04 10:00:00', '%Y-%m-%d %H:%M:%S'),42.1,39.9,92.0,0.0,None,260.0,9.2,None,1029.5,None,5.0)
        weather_3 = weather(datetime.strptime('2017-01-03 21:00:00', '%Y-%m-%d %H:%M:%S'),44.1,42.1,93.0,0.059,None,30.0,18.3,None,1029.6,None,7.0)
        weather_4 = weather(datetime.strptime('2017-01-03 15:00:00', '%Y-%m-%d %H:%M:%S'),42.1,39.9,92.0,0.012,None,40.0,20.8,None,1030.0,None,7.0)
        weather_5 = weather(datetime.strptime('2017-01-03 05:00:00', '%Y-%m-%d %H:%M:%S'),39.9,39.2,97.0,0.047,None,40.0,16.1,None,1030.3,None,7.0)

        weather_sample = [weather_1,weather_2,weather_3,weather_4,weather_5]
        weather_sample_df = self.spark.createDataFrame(weather_sample, schema=weather_schema)

        return weather_sample_df

    def test_df_shape(self):
        df = self.test_data()
        self.assertEquals(len(df.columns), 12)
        self.assertGreater(df.count(), 1)

    def test_remove_columns(self):
        df = remove_columns(self.test_data())
        self.assertEquals(len(df.columns), 10)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
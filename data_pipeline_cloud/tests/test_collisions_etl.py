import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark import Row
from datetime import date
import pyspark.sql.functions as f
from collisions_etl import add_columns


class TestCollisions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

    def test_data(self):

        collision_schema = StructType(
        [StructField("collision_id", IntegerType(), True),
        StructField("crash_date", DateType(), True),
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
        StructField("vehicle_type_code1", StringType(), True),
        StructField("vehicle_type_code2", StringType(), True),
        StructField("vehicle_type_code_3", StringType(), True),
        StructField("vehicle_type_code_4", StringType(), True),
        StructField("vehicle_type_code_5", StringType(), True)])

        collisions = Row('collision_id','crash_date', 'crash_time', 'borough', 'zip_code', 'latitude',
        'longitude', 'location', 'on_street_name', 'off_street_name',
        'cross_street_name', 'number_of_persons_injured',
        'number_of_persons_killed', 'number_of_pedestrians_injured',
        'number_of_pedestrians_killed', 'number_of_cyclist_injured',
        'number_of_cyclist_killed', 'number_of_motorist_injured',
        'number_of_motorist_killed', 'contributing_factor_vehicle_1',
        'contributing_factor_vehicle_2', 'contributing_factor_vehicle_3',
        'contributing_factor_vehicle_4', 'contributing_factor_vehicle_5',
        'collision_id', 'vehicle_type_code1', 'vehicle_type_code2',
        'vehicle_type_code_3', 'vehicle_type_code_4', 'vehicle_type_code_5')

        collision_1 = collisions(1,datetime.strptime('2012-07-01', '%Y-%m-%d'),'9:40','MANHATTAN',10002,40.7140687,-73.9975038,"(40.7140687, -73.9975038)",'DIVISION STREET','CATHERINE STREET', None,0,0,0,0,0,0,0,0,'Driver Inexperience','Driver Inexperience',None,None,None,5294,'PASSENGER VEHICLE','LIVERY VEHICLE',None,None)
        collision_2 = collisions(2,datetime.strptime('2012-07-01', '%Y-%m-%d'),'18:10','BROOKLYN',11229,40.6055989,-73.9595748,"(40.6055989, -73.9595748)",'AVENUE R','EAST 13 STREET',None,0,0,0,0,0,0,0,0,'Lost Consciousness','Unspecified',None,None,None,116262,'SPORT UTILITY / STATION WAGON','PASSENGER VEHICLE',None,None)
        collision_3 = collisions(3,datetime.strptime('2012-07-01','%Y-%m-%d'),'00:37','MANHATTAN',10017,40.7559235,-73.9748888,"(40.7559235, -73.9748888)",'PARK AVENUE ','EAST 48 STREET',None,1,0,0,0,0,0,1,0,'Unspecified','Unspecified',None,None,None,37633,'TAXI','PASSENGER VEHICLE',None,None)
        collision_4 = collisions(4,datetime.strptime('2012-07-01','%Y-%m-%d'),'18:40','BRONX',10454,40.8016648,-73.9132226,"(40.8016648, -73.9132226)",'WILLOW AVENUE','EAST 134 STREET',None,0,0,0,0,0,0,0,0,'Illness',None,None,None,None,72590,'SPORT UTILITY / STATION WAGON',None,None,None)
        collision_5 = collisions(5,datetime.strptime('2012-07-01','%Y-%m-%d'),'4:50','MANHATTAN',10009,40.7235061,-73.9792808,"(40.7235061, -73.9792808)",'EAST 6 STREET ','AVENUE C',None,1,0,1,0,0,0,0,0,'Unspecified',None,None,None,None,13866,'PASSENGER VEHICLE',None,None,None)


        coll_sample = [collision_1,collision_2,collision_3,collision_4,collision_5]
        coll_sample_df = self.spark.createDataFrame(coll_sample,schema=collision_schema)
        
        return coll_sample_df

    def test_df_shape(self):
        df = self.test_data()
        self.assertEqual(len(df.columns), 29)
        self.assertGreater(df.count(), 1)

    def test_dates(self):
        df = self.test_data()
        today = date.today()
        latest_date = df.select(f.max('crash_date').alias('latest')).first()['latest']
        earliest_date = df.select(f.min('crash_date').alias('earliest')).first()['earliest']
        earliest_data_collection_date = datetime.strptime('2011-12-31','%Y-%m-%d').date()
        
        self.assertTrue(latest_date < today)
        self.assertTrue(earliest_date > earliest_data_collection_date)

    def test_add_columns(self):
        df = add_columns(self.test_data())
        self.assertEqual(len(df.columns), 31)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
if __name__ == '__main__':
    unittest.main()
from pyspark.sql.functions import *
from itertools import chain
import pandas as pd

def get_taxi_zones():
    """ Retrieves taxi zone file and outputs as dictionary."""
    location_data_map = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')
    location_dic = dict(zip(location_data_map['LocationID'], location_data_map['Borough']))
    location_mapping = create_map([lit(x) for x in chain(*location_dic.items())])
    return location_mapping

def map_location_values(df):
    location_mapping = get_taxi_zones()
    df = df.withColumn('PULocationID', location_mapping[df['PULocationID']])
    df = df.withColumn('DOLocationID', location_mapping[df['DOLocationID']])
    return df

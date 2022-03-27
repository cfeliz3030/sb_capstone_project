from pyspark.sql.types import *
import pyspark.sql.functions as f

def add_columns(df):
    df = df.withColumn('crash_date_time', f.concat_ws(' ', 'crash_date', 'crash_time'))
    df = df.withColumn('crash_date_time', df.crash_date_time.cast(TimestampType()))

    df = df.withColumn(
    'rush_hour',
    f.when((f.hour(df.crash_date_time) >= 8) & (f.hour(df.crash_date_time) < 10), 1).when(
        (f.hour(df.crash_date_time) >= 15) & (f.hour(df.crash_date_time) < 20), 1).otherwise(0))
    
    return df
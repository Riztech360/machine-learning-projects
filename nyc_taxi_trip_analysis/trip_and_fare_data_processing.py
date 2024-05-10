import os
import re
import shutil
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, lit, date_trunc, 
                                  date_format, minute, count, expr)
from pyspark.sql.types import (StructField, StructType, StringType, IntegerType, 
                              FloatType, DecimalType, TimestampNTZType)

# defines path to be read from or saved to
def path_definer(folder_name, sub_folder_name, file_name=None, month=None, asterisk=False, upto_dir=False):
    path = 'Hello Kitty'

    if file_name is not None:
        path =  base_path + folder_name + '/' + sub_folder_name + '/' + file_name + '/'
    
    elif month is not None:
        path =  base_path + folder_name + '/' + sub_folder_name + '/' + f'/month={month}/'
        if asterisk:
            path = path + '*'
        if upto_dir:
            path = path.removesuffix('/')
    
    else:
        path =  base_path + folder_name + '/' + sub_folder_name + '/'
        if upto_dir:
            path = path.removesuffix('/')

    return path

def csv_reader_and_partition(folder_name, file_number): #read csv file and partitions it
    file_name = folder_name + '_' + str(file_number) + '.csv'
    path_to_read = path_definer('original_dataset', folder_name, file_name)
    path_to_save = path_definer('partitioned_dataset', folder_name, month=file_number)
    schema = 'Hello Kitty'
    drop_unnecessary_columns_list = []

    # schema structure for trip data
    if folder_name == 'trip_data':
        schema = StructType([
            StructField('medallion',StringType(),True),
            StructField('hack_license',StringType(),True),
            StructField('vendor_id',StringType(),True),
            StructField('rate_code',IntegerType(),True),
            StructField('store_and_fwd_flag',StringType(),True),
            StructField('pickup_datetime',TimestampNTZType(),True),
            StructField('dropoff_datetime',TimestampNTZType(),True),
            StructField('passenger_count',IntegerType(),True),
            StructField('trip_time_in_secs',IntegerType(),True),
            StructField('trip_distance',FloatType(),True),
            StructField('pickup_longitude',DecimalType(9,6),True),
            StructField('pickup_latitude',DecimalType(9,6),True),
            StructField('dropoff_longitude',DecimalType(9,6),True),
            StructField('dropoff_latitude',DecimalType(9,6),True)
        ])

        drop_unnecessary_columns_list = ['rate_code','store_and_fwd_flag','dropoff_datetime','passenger_count',
                                         'dropoff_latitude','dropoff_longitude']

    # schema structure for trip fare data
    else:
        schema = StructType([
            StructField('medallion',StringType(),True),
            StructField('hack_license',StringType(),True),
            StructField('vendor_id',StringType(),True),
            StructField('pickup_datetime',TimestampNTZType(),True),
            StructField('payment_type',StringType(),True),
            StructField('fare_amount',FloatType(),True),
            StructField('surcharge',FloatType(),True),
            StructField('mta_tax',FloatType(),True),
            StructField('tip_amount',FloatType(),True),
            StructField('tolls_amount',FloatType(),True),
            StructField('total_amount',FloatType(),True)
        ])

        drop_unnecessary_columns_list = ['payment_type','surcharge','mta_tax','tolls_amount']

    #read csv file and partition it by month
    spark.read\
    .option("delimiter",",")\
    .option("header", "true")\
    .schema(schema)\
    .csv(path_to_read)\
    .drop(*drop_unnecessary_columns_list)\
    .write.format('parquet').mode('overwrite').save(path_to_save)

    return None

# tracks the rows removed and standardize pickup_datatime
def track_and_standardize(folder_name, month):
    path_to_read =  path_definer('partitioned_dataset', folder_name, month=month, asterisk=True)
    path_to_save_null_tracker =  path_definer('processed_dataset', 'removed_rows_track')
    path_to_save_standardize  =  path_definer('processed_dataset', folder_name, month=month)

    df = spark.read.parquet(path_to_read)

    if folder_name == 'trip_data': 
        df = df.withColumn('to_drop',when(col('medallion').isNull(),True)
                                    .when(col('hack_license').isNull(),True)
                                    .when(col('vendor_id').isNull(),True)
                                    .when(col('pickup_datetime').isNull(),True)
                                    .when(col('trip_time_in_secs').isNull(),True)
                                    .when(col('trip_distance').isNull(),True)
                                    .when(col('pickup_longitude').isNull(),True)
                                    .when(col('pickup_latitude').isNull(),True)
                                    .otherwise(False))
    else:
        df = df.withColumn('to_drop',when(col('medallion').isNull(),True)
                                    .when(col('hack_license').isNull(),True)
                                    .when(col('vendor_id').isNull(),True)
                                    .when(col('pickup_datetime').isNull(),True)
                                    .when(col('fare_amount').isNull(),True)
                                    .when(col('tip_amount').isNull(),True)
                                    .when(col('total_amount').isNull(),True)
                                    .otherwise(False))\
    
    # tracking the rows dropped
    df.agg(
            count(col('medallion')).alias('total_rows'),
            count(when(col('to_drop')==True,col('medallion'))).alias('rows_removed')
        )\
        .withColumn('removed_pct',when(col('rows_removed') == 0,0.0)
                                .otherwise((col('rows_removed') / col('total_rows')) * 100.0))\
        .withColumn('file',lit(f'{folder_name}'))\
        .withColumn('file_number',lit(f'{month}'))\
        .select('file','file_number','total_rows','rows_removed','removed_pct')\
        .write.format('parquet').mode('append').save(path_to_save_null_tracker)
    
    #only trip data pickup_dateime needs to standardize
    if folder_name == 'trip_data':
        df.withColumn('minute',minute(col('pickup_datetime')))\
          .filter(~(col('to_drop')==True))\
          .select("*",date_format(date_trunc('hour','pickup_datetime'),'HH:mm').alias("pickup_time_trunced_hour"))\
          .withColumn('pickup_time',when(col('minute').between(0,7),col('pickup_time_trunced_hour'))
                                        .when(col('minute').between(8,20), col('pickup_time_trunced_hour') + expr('INTERVAL 15 MINUTES'))
                                        .when(col('minute').between(21,35),col('pickup_time_trunced_hour') + expr('INTERVAL 30 MINUTES'))
                                        .when(col('minute').between(36,50),col('pickup_time_trunced_hour') + expr('INTERVAL 45 MINUTES'))
                                        .otherwise(col('pickup_time_trunced_hour') + expr('INTERVAL 1 HOURS')))\
          .withColumn('pickup_time',date_format(col('pickup_time'),'HH:mm'))\
          .drop('minute','pickup_time_trunced_hour','to_drop')\
          .write.format('parquet').mode('overwrite').save(path_to_save_standardize)

    # for trip fare data no need to standardize just save
    else:
        df.filter(~(col('to_drop')==True))\
          .write.format('parquet').mode('overwrite').save(path_to_save_standardize)
    
    return None

# joins trip_data and trip_fare_data for that month
def trip_join_trip_fare(month, df1, df2):
    path_to_save = path_definer('processed_dataset','trip_and_trip_fare_merge', month=month)
    join_conditions = [df1.medallion == df2.medallion, 
                       df1.hack_license == df2.hack_license, 
                       df1.vendor_id == df2.vendor_id, 
                       df1.pickup_datetime == df2.pickup_datetime]

    df1.join(df2,join_conditions,'inner')\
       .select(df1["*"],'fare_amount','tip_amount','total_amount')\
       .write.format('parquet').mode('overwrite').save(path_to_save)
       
    return None

def main():
    # will delete this directory because changes get appended so to avoid duplication 
    if os.path.isdir(path_definer('processed_dataset', 'removed_rows_track', upto_dir=True)):
        shutil.rmtree(path_definer('processed_dataset', 'removed_rows_track', upto_dir=True))

    # process trip and trip fare separately then save in their respective processed folder
    for file_number in range(file_start_number, file_end_number): # represents month
        for folder_name in folder_names_list:
            csv_reader_and_partition(folder_name, file_number)   # reading csv file of trip and their fare and save it by month = file_number

        # process trip and trip fare data for that month or file_number:
        track_and_standardize(folder_names_list[0], month=file_number)  # track null columns dropped and standardize pickup time for trip
        track_and_standardize(folder_names_list[1], month=file_number)  # track null columns dropped and standardize pickup time for trip fare

        # define paths for trip and trip fare processed data
        trip_processed_path = path_definer('processed_dataset', folder_names_list[0], month=file_number, asterisk=True)  # define a path for trip processed data
        trip_fare_processed_path = path_definer('processed_dataset', folder_names_list[1], month=file_number, asterisk=True) # define a path for trip fare processed data

        # use defined paths to read trip and trip fare processed data
        trip_processed_df = spark.read.parquet(trip_processed_path) 
        trip_fare_processed_df = spark.read.parquet(trip_fare_processed_path)

        # once read trip and trip fare processed data is read then join them
        trip_join_trip_fare(file_number, trip_processed_df, trip_fare_processed_df)
        
        # removes un-wanted month's trip and trip fare data and the original file to save space 
        for i in range(0,2):
            os.remove(path_definer('original_dataset', folder_names_list[i], file_name=f'{folder_names_list[i]}_{file_number}.csv'))
            shutil.rmtree(path_definer('partitioned_dataset', folder_names_list[i], month=file_number, upto_dir=True))
            shutil.rmtree(path_definer('processed_dataset', folder_names_list[i], month=file_number, upto_dir=True))

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("sparky")\
        .getOrCreate()

    # parameters #
    base_path =  'C:/your/path/to/repo' + '/machine-learning-projects/nyc_taxi_trip_analysis/'
    folder_names_list = ['trip_data','trip_fare']
    file_start_number = 1
    file_end_number = max([int(re.search("(\d+)", file).group(1)) for file in os.listdir(path_definer('original_dataset', folder_names_list[0], upto_dir=True))]) + 1
    print(file_end_number)

    #main()
import os
import shutil
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, lit, year, month, minute, 
                                   date_trunc, date_format, count, expr)
from pyspark.sql.types import (StructField, StructType, StringType, IntegerType, 
                              FloatType, DecimalType, TimestampNTZType)


def csv_reader_and_partition(base_path, folder_name, file_number):
    path_to_read = base_path + 'original_dataset/'    + folder_name + '/' + folder_name + '_' + str(file_number) + '.csv'
    path_to_save = base_path + 'partitioned_dataset/' + folder_name + '/'
    schema = 'Hello Kitty'
    drop_unnecessary_columns_list = []

    # schema structure for trips data
    if folder_name == 'trips_data':
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

    # schema structure for fares data
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
    
    #read csv file and partition it by year and month
    spark.read\
    .option("delimiter",",")\
    .option("header", "true")\
    .schema(schema)\
    .csv(path_to_read)\
    .withColumn('year',year(col('pickup_datetime')))\
    .withColumn('month',month(col('pickup_datetime')))\
    .drop(*drop_unnecessary_columns_list)\
    .write.partitionBy('year','month').format('parquet').mode('overwrite').save(path_to_save)

    return None

def track_and_standardize(base_path, folder_name, year, month):
    path_to_read =  base_path + 'partitioned_dataset/' + folder_name + f'year={year}/month={month}/*' 
    path_to_save =  base_path + 'processed_dataset/'   + folder_name + f'year={year}/month={month}/'
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
        .write.format('parquet').mode('append').save(base_path + 'processed_datatset/removed_rows_track/')
    
    #only trip data pickup_dateime needs to standardize
    if folder_name == 'trips_data':
        df.withColumn('minute',minute(col('pickup_datetime')))\
          .filter(~(col('to_drop')==True))\
          .select("*",date_format(date_trunc('hour','pickup_datetime'),'HH:mm').alias("pickup_time_trunced_hour"))\
          .withColumn('pickup_time',when(col('minute').between(0,15),col('pickup_time_trunced_hour') + expr('INTERVAL 15 MINUTES'))
                                        .when(col('minute').between(16,30),col('pickup_time_trunced_hour') + expr('INTERVAL 30 MINUTES'))
                                        .when(col('minute').between(31,45),col('pickup_time_trunced_hour') + expr('INTERVAL 45 MINUTES'))
                                        .otherwise(col('pickup_time_trunced_hour') + expr('INTERVAL 1 HOURS')))\
          .withColumn('pickup_time',date_format(col('pickup_time'),'HH:mm'))\
          .drop('minute','pickup_time_trunced_hour')\
          .write.partitionBy('year','month').format('parquet').mode('overwrite').save(path_to_save)

    # for fare data no need to standardize just save
    else:
        df.filter(~(col('to_drop')==True))\
          .write.partitionBy('year','month').format('parquet').mode('overwrite').save(path_to_save)
    
    return None

def trips_join_fares(base_path, year, month, df1, df2):
    path_to_save = base_path + 'processed_dataset/' + f'trip_fare_merge/year={year}/month={month}/'
    join_conditions = [df1.medallion == df2.medallion, 
                       df1.hack_license == df2.hack_license, 
                       df1.vendor_id == df2.vendor_id, 
                       df1.pickup_datetime == df2.pickup_datetime]

    df1.join(df2,join_conditions,'inner')\
       .select(df1["*"],'fare_amount','tip_amount','total_amount')\
       .write.format('parquet').mode('overwrite').save(path_to_save)
       
    return None

def main():
    # parameters #
    base_path =  'C:/Users/rizvi/Downloads' + '/Machine-Learning-Projects/nyc_taxi_trips_and_fares_analysis/'
    folder_names_list = ['trip_data','fare_data']
    file_start_number = 1
    file_end_number   = 2
    file_year_start   = 2013
    file_year_end     = 2014

    # will delete this directory because changes get appended to avoid duplication 
    if os.path.isdir(base_path + 'removed_rows_track'):
        shutil.rmtree(base_path + 'removed_rows_track')

    # process trips and fares separately then save in their respective processed folder
    for file_number in range(file_start_number,file_end_number): # represents month
        for folder_name in folder_names_list:
            csv_reader_and_partition(base_path, folder_name, file_number) # reading csv file of trips and their fares then partition file by year and month 
                 
        for year in range(file_year_start,file_year_end): # join trips and fares data 
            track_and_standardize(base_path, folder_names_list[0], year, file_number)  # track null columns dropped and standardize pickup time for trip
            track_and_standardize(base_path, folder_names_list[1], year, file_number)  # track null columns dropped and standardize pickup time for fare

            trips_processed_df = spark.read.parquet(base_path + 'processed_dataset/' + folder_names_list[0] + f'/year={year}/month={file_number}/*')
            fares_processed_df = spark.read.parquet(base_path + 'processed_dataset/' + folder_names_list[1] + f'/year={year}/month={file_number}/*')
            trips_join_fares(base_path, year, file_number, trips_processed_df, fares_processed_df) #join trips data to fares data

            #removes un-wanted data (a month from trip and fare of that year)
            for i in range(0,2):
                shutil.rmtree(base_path + 'partitioned_dataset/' + folder_names_list[i] + f'/year={year}/month={file_number}')
                shutil.rmtree(base_path + 'processed_dataset/'   + folder_names_list[i] + f'/year={year}/month={file_number}')

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("sparky")\
        .getOrCreate()

    main()
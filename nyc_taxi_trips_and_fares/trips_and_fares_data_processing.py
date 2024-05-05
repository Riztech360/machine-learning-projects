import os
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, lit, year, month, minute, 
                                   date_trunc, date_format, count, expr)
from pyspark.sql.types import (StructField, StructType, StringType, IntegerType, 
                              FloatType, DecimalType, TimestampNTZType)


def csv_reader(base_path, folder_name, file_number):
    path = base_path + folder_name + '/' + folder_name + '_' + str(file_number) + '.csv'
    schema = 'Hello Kitty'

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
    
    #read csv file
    df_original = spark.read\
    .option("delimiter",",")\
    .option("header", "true")\
    .schema(schema)\
    .csv(path)\
    .limit(20)

    return df_original

def drop_nulls_and_track(base_path, folder_name, file_number, df):
    unnecessary_trips_columns_list = ['rate_code','store_and_fwd_flag','dropoff_datetime','passenger_count',
                                      'dropoff_latitude','dropoff_longitude']
    
    unnecessary_fares_columns_list = ['payment_type','surcharge','mta_tax','tolls_amount']

    if folder_name == 'trips_data': 
        df = df.withColumn('to_drop',when(col('medallion').isNull(),True)
                                    .when(col('hack_license').isNull(),True)
                                    .when(col('vendor_id').isNull(),True)
                                    .when(col('pickup_datetime').isNull(),True)
                                    .when(col('trip_time_in_secs').isNull(),True)
                                    .when(col('trip_distance').isNull(),True)
                                    .when(col('pickup_longitude').isNull(),True)
                                    .when(col('pickup_latitude').isNull(),True)
                                    .otherwise(False))\
                                    .drop(*unnecessary_trips_columns_list)
    else:
        df = df.withColumn('to_drop',when(col('medallion').isNull(),True)
                                    .when(col('hack_license').isNull(),True)
                                    .when(col('vendor_id').isNull(),True)
                                    .when(col('pickup_datetime').isNull(),True)
                                    .when(col('fare_amount').isNull(),True)
                                    .when(col('tip_amount').isNull(),True)
                                    .when(col('total_amount').isNull(),True)
                                    .otherwise(False))\
                                    .drop(*unnecessary_fares_columns_list)
    
    # tracking the rows dropped
    df.agg(
            count(col('medallion')).alias('total_rows'),
            count(when(col('to_drop')==True,col('medallion'))).alias('rows_removed')
        )\
        .withColumn('removed_pct',when(col('rows_removed') == 0,0.0)
                                .otherwise((col('rows_removed') / col('total_rows')) * 100.0))\
        .withColumn('file',lit(f'{folder_name}'))\
        .withColumn('file_number',lit(f'{file_number}'))\
        .select('file','file_number','total_rows','rows_removed','removed_pct')\
        .write.format('parquet').mode('append').save(base_path + 'removed_rows_track/')
    
    return df

def standardize_and_partition(base_path, folder_name, df):
    path = base_path + folder_name + '_processed/'

    # to be used for partitioning files and keep only non-null values
    df = df.withColumn('year',year(col('pickup_datetime')))\
           .withColumn('month',month(col('pickup_datetime')))\
           .filter(~(col('to_drop')==True))\
           .drop('to_drop')

    #only trips data pickup_dateime needs to standardize
    if folder_name == 'trips_data':
        df.withColumn('minute',minute(col('pickup_datetime')))\
          .select("*",date_format(date_trunc('hour','pickup_datetime'),'HH:mm').alias("pickup_time_trunced_hour"))\
          .withColumn('pickup_time',when(col('minute').between(0,15),col('pickup_time_trunced_hour') + expr('INTERVAL 15 MINUTES'))
                                        .when(col('minute').between(16,30),col('pickup_time_trunced_hour') + expr('INTERVAL 30 MINUTES'))
                                        .when(col('minute').between(31,45),col('pickup_time_trunced_hour') + expr('INTERVAL 45 MINUTES'))
                                        .otherwise(col('pickup_time_trunced_hour') + expr('INTERVAL 1 HOURS')))\
          .withColumn('pickup_time',date_format(col('pickup_time'),'HH:mm'))\
          .drop('minute','pickup_time_trunced_hour')\
          .write.partitionBy('year','month').format('parquet').mode('overwrite').save(path)

    # for fares data no need to standardize just partition files
    else:
        df.write.partitionBy('year','month').format('parquet').mode('overwrite').save(path)

    return None

def trips_join_fares(base_path, year, month, df1, df2):
    path = base_path + f'trips_fares_processed/year={year}/month={month}/'
    join_conditions = [df1.medallion == df2.medallion, df1.hack_license == df2.hack_license, 
                       df1.vendor_id == df2.vendor_id, df1.pickup_datetime == df2.pickup_datetime]

    df1.join(df2,join_conditions,'inner')\
       .select(df1["*"],'fare_amount','tip_amount','total_amount')\
       .write.format('parquet').mode('overwrite').save(path)
       
    return None

def main():
    #### parameters ### 
    base_path = 'your/path/here/' + '/Machine-Learning-Projects/nyc_taxi_trips_and_fares/dataset_files/'
    folder_names_list = ['trips_data','fares_data']
    file_start_number = 1
    file_end_number   = 2
    file_year_start   = 2013
    file_year_end     = 2014

    # process trips and fares separately then save in their respective processed folder
    for file_number in range(file_start_number,file_end_number):
        for folder_name in folder_names_list:
            df_original = csv_reader(base_path, folder_name, file_number)                  # reading csv file of trips and their fares
            df_0 = drop_nulls_and_track(base_path, folder_name, file_number, df_original)  # to drop unnecessary columns and label null columns for kept columns
            standardize_and_partition(base_path, folder_name, df_0)                        # readable pickup time based on conditions so easier to read and evaluate

        for year in range(file_year_start,file_year_end): # join trips and fares data 
            trips_processed_df = spark.read.parquet(base_path + folder_names_list[0] + f'_processed/year={year}/month={file_number}/*')
            fares_processed_df = spark.read.parquet(base_path + folder_names_list[1] + f'_processed/year={year}/month={file_number}/*')
            trips_join_fares(base_path, year, file_number, trips_processed_df, fares_processed_df) #join trips data to fares data

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master("local")\
        .appName("sparky")\
        .getOrCreate()

    main()
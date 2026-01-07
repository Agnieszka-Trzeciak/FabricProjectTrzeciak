# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6ac7e717-8382-4db2-b80a-4ed9824d3231",
# META       "default_lakehouse_name": "Main",
# META       "default_lakehouse_workspace_id": "55f60bfe-17cc-4050-80f8-325980210c70",
# META       "known_lakehouses": [
# META         {
# META           "id": "6ac7e717-8382-4db2-b80a-4ed9824d3231"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,abs,to_date, to_timestamp_ntz
import os

ColorDict = {'green':'l','yellow':'t'}

file_path = f'/lakehouse/default/Files/{color}_tripdata_{Year}-{Month}.parquet'
if os.path.exists(file_path):
    df = spark.read.parquet(f'Files/{color}_tripdata_{Year}-{Month}.parquet').dropDuplicates()
    df = df .na.fill(0)\
            .drop('VendorID','passenger_count','RatecodeID','store_and_fwd_flag','payment_type','fare_amount','extra','tip_amount','mta_tax','tolls_amount','improvement_surcharge')\
            .withColumnRenamed(ColorDict[color]+'pep_pickup_datetime','pickup_datetime') \
            .withColumnRenamed(ColorDict[color]+'pep_dropoff_datetime','dropoff_datetime') \
            .withColumn('pickup_date', to_date(col('pickup_datetime'))) \
            .withColumn("PULocationID",col('PULocationID').cast('int'))\
            .withColumn("DOLocationID",col('DOLocationID').cast('int'))
    if int(Year)>=2025:
        df = df .withColumn('Total',
                        abs(col('total_amount')+col('congestion_surcharge')+col('Airport_fee')+col('cbd_congestion_fee')) if color=='yellow'
                        else abs(col('total_amount')+col('congestion_surcharge')+col('cbd_congestion_fee')))\
                .drop('total_amount','congestion_surcharge','cbd_congestion_fee','cbd_congestion_fee')
    else:
        df = df .withColumn('Total',
                        abs(col('total_amount')+col('congestion_surcharge')+col('Airport_fee')) if color=='yellow'
                        else abs(col('total_amount')+col('congestion_surcharge')))\
                .drop('total_amount','congestion_surcharge')
    if color =='green':
        df = df.drop('ehail_fee','trip_type')
    else:
        df = df.drop('Airport_fee')

    df.write.mode('append').saveAsTable('TLC_rides_silver')
    print(f'Added {color}, {Year}-{Month}')
else:
    raise(Exception(f'Failed to find {color}, {Year}-{Month}'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

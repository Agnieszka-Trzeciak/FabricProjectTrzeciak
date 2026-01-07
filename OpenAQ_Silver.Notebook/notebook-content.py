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

import re
import os
from pyspark.sql.functions import col,lit,to_date,split
from pyspark.sql.types import TimestampType

Year_esc = re.escape(Year)
Month_esc = re.escape(Month)

pattern = rf'\w+_{Year_esc}_{Month_esc}_sensordata_\d+_\d+\.json'
files = os.listdir('/lakehouse/default/Files/.')
for file in files:
    if re.match(pattern,file):
        df = spark.read.option("multiline", "true").json('Files/'+file)
        df = df .withColumn('Borough',lit(file[:file.find('_')]))\
                .withColumn('Sensor_ID',lit(file[list(re.finditer('_',file))[1].start()+1:list(re.finditer('_',file))[2].start()]))\
                .withColumn('Start_Date',to_date(col('coverage.datetimeFrom.local')))\
                .withColumn('Observed_Interval_Duration', split(col('coverage.observedInterval'), ":")[0].cast("int"))\
                .select(col('Borough'),
                        col('Sensor_ID'),
                        col('parameter.id').alias('Parameter_ID'),
                        col('parameter.name').alias('Parameter_Name'),
                        col('parameter.units').alias('Parameter_Units'),
                        col('coverage.datetimeFrom.local').alias('Start_Datetime'),
                        col('coverage.datetimeTo.local').alias('End_Datetime'),
                        col('Start_Date'),
                        col('coverage.expectedInterval').alias('Expected_Interval'),
                        col('coverage.observedInterval').alias('Observed_Interval'),
                        col('Observed_Interval_Duration'),
                        col('value').alias('Value'),
                        col('summary.min').alias('Minimum'),
                        col('summary.max').alias('Maximum'),
                        col('summary.median').alias('Median'),
                        col('summary.avg').alias('Mean'),
                        col('summary.sd').alias('Std'),
                        col('summary.q75').alias('Q75'),
                        col('summary.q25').alias('Q25')
                     )\
                    .withColumn('Start_Datetime',col('Start_Datetime').cast(TimestampType()))\
                    .withColumn('End_Datetime',col('End_Datetime').cast(TimestampType()))
        df.write.mode('append').saveAsTable('Air_quality_silver')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

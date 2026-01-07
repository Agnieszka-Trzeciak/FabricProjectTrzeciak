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

import pandas as pd
from datetime import date
import numpy as np
from pyspark.sql.functions import to_date,col

url = 'https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata'

df = pd.read_csv(url)
df = df[df['TIME_PERIOD']>='2008-12-01'][['TIME_PERIOD','OBS_VALUE']]
df.set_index('TIME_PERIOD',inplace=True)
idx = pd.date_range('2008-12-01', date.today())
df.index = pd.DatetimeIndex(df.index)
df = df.reindex(idx, fill_value=np.nan)
df.ffill(inplace = True)
df.bfill(inplace = True)
df['TIME_PERIOD'] = df.index
spark_df = spark.createDataFrame(df)
spark_df = spark_df.withColumn('TIME_PERIOD',to_date(col('TIME_PERIOD')))
spark_df.write.format("delta").mode("overwrite").saveAsTable("Exchange_Rates")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

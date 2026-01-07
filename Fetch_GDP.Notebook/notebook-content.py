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
import requests as rq
import json
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

url = 'https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json'
data = rq.get(url).json()

df = pd.DataFrame.from_dict(data[1]).drop(['indicator','country','unit','obs_status','decimal'],axis=1)
spark_df = spark.createDataFrame(df)
spark_df = spark_df.withColumn('date',col('date').cast(IntegerType()))
spark_df.write.format("delta").mode("overwrite").saveAsTable("GDP")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

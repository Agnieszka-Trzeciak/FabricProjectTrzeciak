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

d = {'Borough':['Manhattan','Brooklyn','Queens','Bronx','Staten Island'],
    'Latitude':[40.7831,40.6782,40.7282,40.8448,40.5795],
    'Longitude':[-73.9712,-73.9442,-73.7949,-73.8648,-74.1502]}
df = pd.DataFrame(data = d)
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite").saveAsTable("Borough_Lookup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Main.dbo.exchange_rates ORDER BY TIME_PERIOD DESC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Main.dbo.gdp ORDER BY date DESC LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

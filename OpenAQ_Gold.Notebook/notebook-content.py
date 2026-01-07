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

import pyspark.sql.functions as F

Year = '2026'
Month = '01'

df_silver = spark.sql(f"SELECT * FROM Main.dbo.air_quality_silver WHERE Start_Date>='{Year}-{Month}-01'")
df_gold = df_silver     .select(F.col('Borough'),
                            F.col('Parameter_Name'),
                            F.col('Parameter_Units'),
                            F.col('Start_Date'),
                            F.col('Minimum'),
                            F.col('Mean'),
                            F.col('Maximum'),
                            F.col('Observed_Interval_Duration'))\
                        .groupBy(['Parameter_Units','Parameter_Name','Start_Date','Borough']).agg(
                            (F.sum(F.col('Mean') * F.col('Observed_Interval_Duration')) / F.sum('Observed_Interval_Duration')).alias('Average'),
                            F.max(F.col('Maximum')).alias('Maximum'),
                            F.min(F.col('Minimum')).alias('Minimum'))\
                        .withColumn('Maximum',F.greatest(F.lit(0), F.col('Maximum')))\
                        .withColumn('Average',F.greatest(F.lit(0), F.col('Average')))\
                        .withColumn('Minimum',F.greatest(F.lit(0), F.col('Minimum')))\
                        .dropDuplicates()
df_gold = df_gold   .withColumn("Borough", 
                            F.when(F.col("Borough") == "StatenIsland", "Staten Island")
                            .otherwise(F.col("Borough")))\
                    .withColumn('ID',F.concat(F.col('Start_Date'),F.lit(' '),F.col('Borough')))\
                    .withColumn('Full_Borough_Name', F.concat(F.col('Borough'),F.lit(' & New York, USA')))
df_gold.write.mode('append').saveAsTable('Air_quality_gold')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

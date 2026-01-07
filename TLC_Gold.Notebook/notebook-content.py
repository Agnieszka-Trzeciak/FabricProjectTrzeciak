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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "14a1518b-ccf9-4971-9719-2a1eeb1e558a",
# META       "known_warehouses": [
# META         {
# META           "id": "14a1518b-ccf9-4971-9719-2a1eeb1e558a",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import year,col,concat,lit

df = spark.sql('''
SELECT TLC.pickup_date,Lookup.Borough, SUM(Total) AS Total_Revenue, COUNT(Total) AS Number_of_Trips, AVG(Total) as Average_Fare
FROM Main.dbo.tlc_rides_silver as TLC
JOIN Main.dbo.taxi_zone_lookup AS Lookup
ON TLC.PULocationID = Lookup.LocationID
WHERE   
    Lookup.Borough != 'EWR' AND Lookup.Borough != 'Unknown' AND Lookup.Borough != 'N/A' AND 
    pickup_date > '2010-01-01' AND pickup_date <= CURRENT_DATE
GROUP BY TLC.pickup_date,Lookup.Borough
''')
df = df .withColumn('Year',year(col('pickup_date')))\
        .withColumn('ID',concat(col('pickup_date'),lit(' '), col('Borough')))\
        .dropDuplicates()
df.write.mode('overwrite').saveAsTable('TLC_rides_gold')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

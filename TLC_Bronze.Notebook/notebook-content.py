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

import requests as rq
from io import BytesIO

url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{Year}-{Month}.parquet'
file_path = f'/lakehouse/default/Files/{color}_tripdata_{Year}-{Month}.parquet'

response = rq.get(url, stream=True)

if response.status_code == 200:
    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f'Data successfully saved to {color}_tripdata_{Year}-{Month}.parquet')
else:
    raise(Exception(f'Failed to access data for {color},{Year}-{Month}. Status code: {response.status_code}'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

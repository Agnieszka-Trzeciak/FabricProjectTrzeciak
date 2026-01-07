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
# META     "environment": {
# META       "environmentId": "3a4b770d-6f47-aff0-49d4-14efd8a9f4da",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
import geopandas
import geodatasets
import json
import calendar
import time

headers = {
    "X-API-Key": '760bee674e817aff28809feb3bf537029357124e7ebe44b75f0448b4897a58fc',
    "accept": "application/json"
}

def find_locations_in_boroughs():
    Results = pd.DataFrame()
    path_nybb = geodatasets.get_path("nybb")
    boros = geopandas.GeoDataFrame.from_file(path_nybb).to_crs("EPSG:4326")
    url = f"https://api.openaq.org/v3/locations"

    boroughs = {'Staten Island':0,'Queens':1,'Brooklyn':2,'Manhattan':3,'Bronx':4}
    for borough in boroughs:
        params = {
            "bbox": str(boros[boros['BoroName']==borough]['geometry'].values[0].bounds)[1:-1].replace(" ",""),
            "mobile": False,
            "limit": 100}

        response = requests.get(url, headers=headers,params=params)

        if response.status_code == 200:
            data = response.json()['results']
        else:
            raise(Exception(f'Failed to find locations for {borough}, response code: {response.status_code}'))
        df = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
        df['borough'] = borough
        gdf = geopandas.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df['coordinates.longitude'], df['coordinates.latitude']), crs="EPSG:4326")
        gdf['isWithin'] = (boros['geometry'][boroughs[borough]]).contains(gdf['geometry'])
        Results = pd.concat([Results,df[gdf['isWithin']==True][['id','name','sensors','borough']]])
    return Results

def save_sensor_data_for_borough(locations,borough,start_date,end_date):
    file_path = f'/lakehouse/default/Files/{borough.replace(" ","")}_airqualitydata_{start_date[:7]}.json'
    params = {
        "date_from": start_date,
        "date_to": end_date,
        "limit": 1000}
    with open(file_path, "w") as f:
        for location in locations[locations['borough']==borough].iterrows():
            for sensor in location[1]['sensors']:
                url = f"https://api.openaq.org/v3/sensors/{sensor['id']}/days"
                response = requests.get(url, headers=headers,params=params)
                if response.status_code == 200:
                    data = response.json()['results']
                    if data!= []:
                        json.dump(data,f,indent=4)
                time.sleep(20)
    with open(file_path, 'r') as f:
        data = f.read()
        data = data.replace('\n][', ',')
    with open(file_path, 'w') as f:
        f.write(data)
    print(f'Saved data to {borough.replace(" ","")}_airqualitydata_{start_date[:7]}.json')

Year = '2025'
Month = '12'

data = find_locations_in_boroughs()

if len(Month)<2:
    Month = '0'+Month
start_date = Year+'-'+Month+'-'+'01'
end_date = Year+'-'+Month+'-'+str(calendar.monthrange(int(Year), int(Month))[1])

for borough in ['Staten Island','Queens','Brooklyn','Manhattan','Bronx']:
    save_sensor_data_for_borough(data,borough,start_date,end_date)
    print("")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

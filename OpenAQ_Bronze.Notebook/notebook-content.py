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
import time
import datetime
import calendar

headers = {
    "X-API-Key": 'f74ee7eacd45ab9cfe427e479641b3829f7805110cb51f4e47493926279d505e',
    
    "accept": "application/json"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
            raise(response.status_code)
        df = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
        df['borough'] = borough
        gdf = geopandas.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df['coordinates.longitude'], df['coordinates.latitude']), crs="EPSG:4326")
        gdf['isWithin'] = (boros['geometry'][boroughs[borough]]).contains(gdf['geometry'])
        Results = pd.concat([Results,df[gdf['isWithin']==True][['id','name','sensors','borough']]])
    return Results

Locations = find_locations_in_boroughs()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Download_Historical_Data_for_Sensor(sensor_id,borough):
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
    params = {'limit':1000,
                "date_from": '2009-01-01',
                "date_to": '2025-12-31'}
    response = requests.get(url, headers=headers,params=params)
    if response.status_code == 200:
        data = response.json()
        page = 1
        if data['results']!= []:
            with open(f'/lakehouse/default/Files/{borough.replace(" ","")}_sensordata_{sensor_id}_1.json','w') as f:
                json.dump(data['results'],f,indent=4)
            while data['meta']['found']==">1000":
                params['page']=page
                response = requests.get(url, headers=headers,params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data=="{'detail': 'Too many requests'}":
                        time.sleep(60)
                        response = requests.get(url, headers=headers,params=params)
                        data = response.json()
                    else:
                        time.sleep(10)
                    with open(f'/lakehouse/default/Files/{borough.replace(" ","")}_sensordata_{sensor_id}_{page}.json','w') as f:
                        json.dump(data['results'],f,indent=4)
                    page+=1
        return page
    else:
        return response.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Download_Monthly_Data_for_Sensor(sensor_id,borough,Year,Month):
    if len(Month)<2:
        Month = '0'+Month
    start_date = Year+'-'+Month+'-'+'01'
    end_date = Year+'-'+Month+'-'+str(calendar.monthrange(int(Year), int(Month))[1])

    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
    params = {'limit':1000,
                "date_from": start_date,
                "date_to": end_date}
    response = requests.get(url, headers=headers,params=params)
    if response.status_code == 200:
        data = response.json()
        page = 1
        if data['results']!= []:
            with open(f'/lakehouse/default/Files/{borough.replace(" ","")}_{Year}_{Month}_sensordata_{sensor_id}_1.json','w') as f:
                json.dump(data['results'],f,indent=4)
            while data['meta']['found']==">1000":
                params['page']=page
                response = requests.get(url, headers=headers,params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data=="{'detail': 'Too many requests'}":
                        time.sleep(60)
                        response = requests.get(url, headers=headers,params=params)
                        data = response.json()
                    else:
                        time.sleep(10)
                    with open(f'/lakehouse/default/Files/{borough.replace(" ","")}_{Year}_{Month}_sensordata_{sensor_id}_{page}.json','w') as f:
                        json.dump(data['results'],f,indent=4)
                    page+=1
        return page
    else:
        return response.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Get_Historical_Data(locations):
    for location in locations.iterrows():
        for sensor in location[1]['sensors']:
            RTRN = Download_Historical_Data_for_Sensor(sensor['id'],location[1]['borough'])
            if RTRN<100:
                print(f'{datetime.datetime.now().time()}: Handled sensor {sensor["id"]} from {location[1]["borough"]} after {RTRN} pages.')
            else:
                print(f'{datetime.datetime.now().time()}: Error on {sensor["id"]} from {location[1]["borough"]}, code {RTRN}')
            time.sleep(60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Get_Monthly_Data(locations,Year,Month):
    for location in locations.iterrows():
        for sensor in location[1]['sensors']:
            RTRN = Download_Monthly_Data_for_Sensor(sensor['id'],location[1]['borough'],Year,Month)
            if RTRN<100:
                print(f'{datetime.datetime.now().time()}: Handled sensor {sensor["id"]} from {location[1]["borough"]} after {RTRN} pages.')
            else:
                print(f'{datetime.datetime.now().time()}: Error on {sensor["id"]} from {location[1]["borough"]}, code {RTRN}')
            time.sleep(30)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Get_Monthly_Data(Locations,Year,Month)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

import requests
import json
import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect, types
from datetime import date


# DB credentials
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
ENDPOINT2 = os.environ['ENDPOINT2']
DB_NAME2 = os.environ['DB_NAME2']
USERNAME2 = os.environ['USERNAME2']
PASSWORD2 = os.environ['PASSWORD2']

# DB table name
table_name = 'weather_hourly'

# connection string for sql alchemy engine
conn_string = f'postgresql://{USERNAME}:{PASSWORD}@{ENDPOINT}/{DB_NAME}'
conn_string2 = f'postgresql://{USERNAME2}:{PASSWORD2}@{ENDPOINT2}/{DB_NAME2}'



def lambda_handler(event, context):
    print('Starting process')

    ## Extract
    db = create_engine(conn_string)
    dbconnect = db.connect()

    df = pd.read_sql_table('weather_hourly', dbconnect)

    ## Transform
    df = df[['Datum','Standort','Parameter','Wert']]
    df['Standort'] = df['Standort'].str.replace('Zch_','',regex=True)
    df['Wert'] = df['Wert'].astype(float)
    df['Datum'] = pd.to_datetime(df['Datum'],utc=True) # convert to datetime and utc
    df['Datetime'] = df['Datum'].dt.tz_convert('Europe/Berlin') # convert local time (daylightsaving)
    df['Datetime'] = df['Datetime'].dt.tz_localize(None) # remove timezone info
    del df['Datum']
    
    df = df[df['Datetime'].dt.strftime('%Y') >= '2013'] #only from 2013 upwards relevant
    
    parameter_keep = ["RainDur","T"]
    df = df[df['Parameter'].isin(parameter_keep)]
    df.reset_index(drop=True)
    
    df_temperature = df[df["Parameter"]=="T"]
    df_rain = df[df["Parameter"]=="RainDur"]

    df_temperature = df_temperature.rename(columns = {'Wert':'Temperature'})
    del df_temperature['Parameter']

    df_rain = df_rain.rename(columns = {'Wert':'Rainduration'})
    del df_rain['Parameter']
    
    df_wide = pd.merge(df_temperature,
                       df_rain,
                       how='left',
                       left_on=['Standort','Datetime'],
                       right_on = ['Standort','Datetime'])
    
    df_wide = df_wide[["Datetime", "Standort", "Temperature","Rainduration"]]
    df_wide = df_wide.sort_values("Datetime").reset_index(drop=True)
    

    ## Load
    db2 = create_engine(conn_string2)
    #dbconnect2 = db2.connect()

    df_wide.to_sql('weather_hourly_stage', db2, if_exists='replace',index=False, method='multi',chunksize=5000)

    
    return {
        'statusCode': 200,
        'body': json.dumps('Finished Process!')
    }
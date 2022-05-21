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

    #Extract
    db = create_engine(conn_string)
    dbconnect = db.connect()

    df = pd.read_sql_table('weather_hourly', dbconnect)

    #Transform
    df = df[['Datum','Standort','Parameter','Einheit','Wert']]

    parameter_keep = ["RainDur","T"]
    df = df[df['Parameter'].isin(parameter_keep)]

    df['Standort'] = df['Standort'].str.replace('Zch_','',regex=True)

    df['Wert'] = df['Wert'].astype(float)
    df['Datum'] = pd.to_datetime(df['Datum'],utc=False)

    df = df[df["Datum"].dt.strftime('%Y') >= '2012'] #only from 2013 upwards relevant


    #Load
    db2 = create_engine(conn_string2)
    #dbconnect2 = db2.connect()

    df.to_sql('weather_hourly_stage', db2, if_exists='replace',index=False, method='multi',chunksize=5000)

    
    return {
        'statusCode': 200,
        'body': json.dumps('Finished Process!')
    }


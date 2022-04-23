# dependencies
import pandas as pd
import psycopg2 as pg
from datetime import date
from sqlalchemy import create_engine
import requests
import json
import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


#############################################################################
# Extract / Transform
#############################################################################


def fetchUrlList():
    """
    We want to fetch the current list of all csv files from the opendata.swiss API.
    We are interested mainly in the differnt url of the csv files to batchdownload them.
    We save this metadata of the differnt files in a table in our database for quality checks.
    Important: The credetials of the database must be configured in Airflow!
    """

    # Specify the package you are interested in from opendataswiss
    package = 'taglich-aktualisierte-meteodaten-seit-1992'

    base_url = 'https://opendata.swiss/api/3/action/package_show?id='# Base url for package information. This is always the same.
    package_information_url = base_url + package # Construct the url for the package of interest
    package_information = requests.get(package_information_url) # Make the HTTP request
    package_dict = json.loads(package_information.content) # Use the json module to load CKAN's response into a dictionary
    package_dict = package_dict['result']   # we only need the 'result' part from the dictionary

    # Get the relevant metadata for the data from the dictionary
    df_sources = pd.DataFrame({'year':[],'filename':[], 'url': [],'format': []})

    for ele in package_dict['resources']:
        to_append = [ele['url'][-8:-4],ele['url'][-25:-4], ele['url'],ele['format']]
        df_sources.loc[len(df_sources)] = to_append
    
    df_sources =  df_sources[df_sources['format']=="CSV"] #only keep csv files in the dataframe
    df_sources['year'] = df_sources['year'].astype(int) # convert year to int



    
    # attempt the connection to postgres
    try:
        pg_hook = PostgresHook(postgres_conn_id="trunks", schema="trunks")
        dbconnect = pg_hook.get_conn()

    except Exception as error:
        print("Error: Could not get credentials for DB from Airflow")
        print(error)

    try: 
        cur = dbconnect.cursor()
        
    except pg.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    dbconnect.set_session(autocommit=True)
    conn_string = pg_hook.get_uri()
    db = create_engine(conn_string)
    dbconnect = db.connect()

    # write df to database
    df_sources.to_sql('weather_daily_urls', con= dbconnect, if_exists='replace',
          index=False)
    dbconnect = pg.connect(conn_string)
    dbconnect.autocommit = True
    cur.close()
    dbconnect.close()

def fetchAllCsv():
    """
    We use the url list from the last part and batchdownload all csv files from opendata.swiss.
    We store the in a single dataframe and load them in our database. Becuase the datasize is low, we
    dont do a deltaload here. The new load replace the Table in the Database completle.
    """

    # Specify the package you are interested in from opendataswiss
    package = 'taglich-aktualisierte-meteodaten-seit-1992'

    base_url = 'https://opendata.swiss/api/3/action/package_show?id='# Base url for package information. This is always the same.
    package_information_url = base_url + package # Construct the url for the package of interest
    package_information = requests.get(package_information_url) # Make the HTTP request
    package_dict = json.loads(package_information.content) # Use the json module to load CKAN's response into a dictionary
    package_dict = package_dict['result']   # we only need the 'result' part from the dictionary

    # Get the relevant metadata for the data from the dictionary
    df_sources = pd.DataFrame({'package_id': [], 'url': [],'uri': [],'format': []})

    for ele in package_dict['resources']:
        to_append = [ele['package_id'], ele['url'],ele['uri'],ele['format']]
        df_sources.loc[len(df_sources)] = to_append
    
    df_sources =  df_sources[df_sources['format']=="CSV"] #only keep csv files in the dataframe

    # merging csv files into one single dataframe
    df = pd.concat(
        map(pd.read_csv, list(df_sources["url"])), ignore_index=False)

    # attempt the connection to postgres
    try:
        pg_hook = PostgresHook(postgres_conn_id="trunks", schema="trunks")
        dbconnect = pg_hook.get_conn()

    except Exception as error:
        print("Error: Could not get credentials for DB from Airflow")
        print(error)

    try: 
        cur = dbconnect.cursor()
        
    except pg.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    dbconnect.set_session(autocommit=True)
    conn_string = pg_hook.get_uri()
    db = create_engine(conn_string)
    dbconnect = db.connect()

    # write df to db
    df.to_sql('weather_daily', con= dbconnect, if_exists='replace',
          index=False)
    dbconnect = pg.connect(conn_string
                        )
    dbconnect.autocommit = True
    cur.close()
    dbconnect.close()


dag = DAG(
        'weather_daily',
        schedule_interval='@daily',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1))



fetchUrlList = PythonOperator(
    task_id="fetchUrlList",
    python_callable=fetchUrlList,
    dag=dag
 )

fetchAllCsv = PythonOperator(
    task_id="fetchAllCsv",
    python_callable=fetchAllCsv,
    dag=dag
 )


fetchUrlList >> fetchAllCsv
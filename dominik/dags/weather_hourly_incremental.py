# dependencies
import pandas as pd
import psycopg2 as pg
from datetime import date
from sqlalchemy import create_engine, text, inspect
import requests
import json
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def fetchUrlList():
    """
    We want to fetch the current list of all csv files from the opendata.swiss API.
    We are interested mainly in the differnt url of the csv files to batchdownload them.
    We save this metadata of the differnt files in a table in our database for quality checks.
    Important: The credetials of the database must be configured in Airflow!
    """

    # Specify the package you are interested in from opendataswiss
    package = 'stundlich-aktualisierte-meteodaten-seit-1992'

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
    df_sources = df_sources.sort_values(by=['year'],ascending=False)

    
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
    df_sources.to_sql('weather_hourly_urls', con= dbconnect, if_exists='replace',
          index=False)
    dbconnect = pg.connect(conn_string)
    dbconnect.autocommit = True
    cur.close()
    dbconnect.close()

def checkUrlList():
    """
    In this step we chech the before fetched list of urls to batchdownload all csv files in the next step.
    We make a very simple check if the fetched urls are valid.
    """

    # DB table name
    table_name_urls = 'weather_hourly_urls'


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

    
    statement = text(f'SELECT "url" FROM {table_name_urls} ORDER BY "year" DESC LIMIT 1')
    url_start = dbconnect.execute(statement).fetchone()[0][0:5]

    if 'https' in url_start:
        print('Check successful...ready to fetch data')
    else:
        raise ValueError('Check unsuccessful...ulrs seems to be corrupted')
    
    cur.close()
    dbconnect.close()

def fetchWeatherData():
    """
    In this function we now take the urls list from the database and batchdownload the data from the differnt endpoint of the opendata API.
    The funktion cheks if the table already exists. if the table exists it checks the last record in the database and then start to load the missing data upon the newest available data.
    If the table is not created yet it will sreate a new one and will load the data in a inital load. To avoid loading to mutch data at one we can limit the load per load with the
    variable "years_to_save_per_execution", which is set in the default setting to 5.
    """

    print('Starting process')

    # DB table names
    table_name = 'weather_hourly'
    table_name_urls = 'weather_hourly_urls'

    # determines how many years in one execution are saved in the db
    years_to_save_per_execution = 5

    # dtypes for columns
    dtypes = {"Standort": 'string', "Parameter": 'string', "Intervall": 'string', "Einheit": 'string',
          "Wert": 'string', "Status": 'string'}

    # columns to include in database
    usecols = [0 ,1 ,2, 3, 4, 5, 6]


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


    # load checked table with urls from database
    df_sources = pd.read_sql_table(table_name_urls, dbconnect)


    if inspect(db).has_table(table_name):
        print('Start to insert data for new years')
        # get current year
        current_year = date.today().year
        # get last year that is in table
        with db.connect() as conn:
            statement = text(f'SELECT "Datum" FROM {table_name} ORDER BY "Datum" DESC LIMIT 1')
            last_year = int(conn.execute(statement).fetchone()[0][0:4])
        # if last year in table is current year, drop previous data of this year and load in current data for this year
        if last_year == current_year:
            df_sources_current_year = df_sources[df_sources['year'] == current_year]
            df = pd.concat(map(lambda file: pd.read_csv(file, dtype=dtypes, usecols=usecols),
                               list(df_sources_current_year["url"])), ignore_index=True)
            if len(df) == 0:
                raise Exception('New data is missing. Previous data will not be deleted.')

            # Drop all rows for this year
            with db.connect().execution_options(autocommit=True) as conn:
                statement = text(f"DELETE FROM {table_name} WHERE \"Datum\" LIKE '%{current_year}%'")
                conn.execute(statement)
            # write new data to db
            with db.connect().execution_options(autocommit=True) as conn:
                df.to_sql(table_name, con=conn, if_exists='append', index=False)

        # else load in the missing data from previous years
        else:
            current_year = last_year + years_to_save_per_execution
            df_sources_years_to_insert = df_sources[(df_sources['year'] <= current_year) & (df_sources['year'] > last_year)]
            df = pd.concat(map(lambda file: pd.read_csv(file, dtype=dtypes, usecols=usecols),
                               list(df_sources_years_to_insert["url"])), ignore_index=True)
            with db.connect().execution_options(autocommit=True) as conn:
                df.to_sql(table_name, con=conn, if_exists='append', index=False)

        # quality check: check if data for all years is saved in db
        with db.connect() as conn:
            statement = text(f'SELECT "Datum" FROM {table_name} ORDER BY "Datum" DESC LIMIT 1')
            last_year = int(conn.execute(statement).fetchone()[0][0:4])

        if last_year != current_year:
            raise Exception(
                'Quality check failed: Data for certain years is missing. Insertion process is not correct.')

        print('Finished process of inserting data')

    # initial load of the first years to save in db
    else:
        print('Initial Load of data')
        df_sources_first_two_years = df_sources[df_sources['year'] <= (2012 + years_to_save_per_execution)]
        df = pd.concat(map(lambda file: pd.read_csv(file, dtype=dtypes, usecols=usecols), list(df_sources_first_two_years["url"])), ignore_index=True)
        with db.connect().execution_options(autocommit=True) as conn:
            df.to_sql(table_name, con=conn, if_exists='fail', index=False)
        print('Finished process of inserting data')
    return {
        'statusCode': 200,
        'body': json.dumps('Finished Process!')
    }


dag = DAG(
        'weather_hourly_incremental',
        schedule_interval='@daily',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1))


fetchUrlList = PythonOperator(
    task_id="fetchUrlList",
    python_callable=fetchUrlList,
    dag=dag
 )

checkUrlList = PythonOperator(
    task_id="checkUrlList",
    python_callable=checkUrlList,
    dag=dag
 )

fetchWeatherData = PythonOperator(
    task_id="fetchWeatherData",
    python_callable=fetchWeatherData,
    dag=dag
 )



fetchUrlList >> checkUrlList >> fetchWeatherData
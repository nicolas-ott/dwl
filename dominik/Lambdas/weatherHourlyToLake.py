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

# DB table name
table_name = 'weather_hourly'

# connection string for sql alchemy engine
conn_string = f'postgresql://{USERNAME}:{PASSWORD}@{ENDPOINT}/{DB_NAME}'

# Package list of the Swiss open data portal
base_url = 'https://opendata.swiss/api/3/action/package_show?id='
# packages = 'https://opendata.swiss/api/3/action/package_list'
package = 'stundlich-aktualisierte-meteodaten-seit-1992'


# columns to include in database
usecols = [0 ,1 ,2, 3, 4, 5, 6]

# determines how many years in one execution are saved in the db
years_to_save_per_execution = 2


def lambda_handler(event, context):
    print('Starting process')

    # Make the HTTP request
    package_information = requests.get(base_url + package)

    # Use the json module to load CKAN's response into a dictionary
    package_dict = json.loads(package_information.content)

    # Check the contents of the response.
    assert package_dict['success'] is True  # again make sure if response is OK
    package_dict = package_dict['result']  # we only need the 'result' part from the dictionary
    # pprint.pprint(package_dict)  # pretty print the package information to screen

    # Get the relevant metadata for the data from the dictionary
    df_sources = pd.DataFrame({'year': [], 'filename': [], 'package_id': [], 'url': [], 'uri': [], 'format': []})

    for ele in package_dict['resources']:
        to_append = [ele['url'][-8:-4], ele['url'][-25:-4], ele['package_id'], ele['url'], ele['uri'], ele['format']]
        df_sources.loc[len(df_sources)] = to_append

    df_sources = df_sources[df_sources['format'] == "CSV"]  # only keep csv files in the dataframe
    df_sources['year'] = df_sources['year'].astype(int)  # convert year to int

    # create sql alchemy engine
    db = create_engine(conn_string)

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
            df = pd.concat(map(lambda file: pd.read_csv(file, usecols=usecols),
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
            df_sources_years_to_insert = df_sources[
                (df_sources['year'] <= current_year) & (df_sources['year'] > last_year)]
            df = pd.concat(map(lambda file: pd.read_csv(file, usecols=usecols),
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
        df = pd.concat(map(lambda file: pd.read_csv(file, usecols=usecols),
                           list(df_sources_first_two_years["url"])), ignore_index=True)
        with db.connect().execution_options(autocommit=True) as conn:
            df.to_sql(table_name, con=conn, if_exists='fail', index=False)
        print('Finished process of inserting data')
    return {
        'statusCode': 200,
        'body': json.dumps('Finished Process!')
    }


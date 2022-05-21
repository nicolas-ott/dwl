import os
import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect, types

# DB credentials
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

ENDPOINT_2 = os.environ['ENDPOINT_2']
DB_NAME_2 = os.environ['DB_NAME_2']
USERNAME_2 = os.environ['USERNAME_2']
PASSWORD_2 = os.environ['PASSWORD_2']

# DB table name
table_name = 'miv_hourly'
table_name_2 = 'miv_hourly_stage'

# connection string for sql alchemy engine
conn_string = f'postgresql://{USERNAME}:{PASSWORD}@{ENDPOINT}/{DB_NAME}'
conn_string_2 = f'postgresql://{USERNAME_2}:{PASSWORD_2}@{ENDPOINT_2}/{DB_NAME_2}'

# dtypes for columns to load in db
dtypes_db = {
             "StrassenName": types.String(),"EKoord": types.Float(precision=2, asdecimal=True),
             "NKoord": types.Float(precision=2, asdecimal=True),
             "MessungDatZeit": types.DateTime(timezone=False), "AnzFahrzeuge": types.INTEGER(),
             "AnzFahrzeugeStatus": types.String()}

def lambda_handler(event, context):
    print('Starting process')

    # Extract and transform
    # sql only select certain columns and sum up values for both direction for each timestamp
    # Exclude not distinctly assignable values to timestamp due to
    # format of timestamp during time shift by HAVING count(*) < 3

    db = create_engine(conn_string)
    sql = 'SELECT "MessungDatZeit", MAX("Achse") as "Strassenname", ' \
          'MAX("EKoord") as "EKoord", MAX("NKoord") as "NKoord", ' \
          'SUM("AnzFahrzeuge")as "AnzFahrzeuge", ' \
          'MAX("AnzFahrzeugeStatus") as "AnzFahrzeugeStatus" ' \
          'FROM miv_hourly ' \
          'GROUP BY "MessungDatZeit", "ZSID" HAVING count(*) < 3;'
    with db.connect() as conn:
        df = pd.read_sql(sql=sql, con=conn)

    # Load
    db2 = create_engine(conn_string_2)
    with db2.connect().execution_options(autocommit=True) as conn:
        df.to_sql(table_name_2, con=conn, if_exists='replace', index=False, dtype=dtypes_db)

    print('Finished process of inserting data')
    return {
        'statusCode': 200,
        'body': json.dumps('Finished Process!')
    }

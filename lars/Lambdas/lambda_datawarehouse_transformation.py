import os
import json
from sqlalchemy import create_engine, text

# DB credentials
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

# DB table name
table_name = 'datawarehouse_final'

# connection string for sql alchemy engine
conn_string = f'postgresql://{USERNAME}:{PASSWORD}@{ENDPOINT}/{DB_NAME}'

def lambda_handler(event, context):
    print('Starting process')

    db = create_engine(conn_string)
    # Alter column vacation_flag from vacations table from int to boolean
    with db.connect().execution_options(autocommit=True) as conn:
        statement = text(
            f'ALTER TABLE vacation_daily_stage ALTER "vacation_flag" TYPE bool USING "vacation_flag"::text::boolean;'
        )
        conn.execute(statement)
 
    # Create final datawarehouse table if not exists
    with db.connect().execution_options(autocommit=True) as conn:
        statement = text(
            f'CREATE TABLE IF NOT EXISTS datawarehouse_final (DatZeit timestamp, '
            f'Datum varchar, Jahr varchar, Monat varchar, Woche varchar, Zeit varchar, '
            f'Wochentag varchar, Standort varchar, Longitude float(4), Latitude float(4), '
            f'Temperatur float(4), Regendauer float(4), AnzFahrzeuge int, AnzFahrzeugeStatus varchar, '
            f'Oelpreis float(4), Newslink text, Ferien boolean);'
        )
        conn.execute(statement)

    # TRUNCATE TABLE Before inserting new data
    with db.connect().execution_options(autocommit=True) as conn:
        statement = text(
            f'TRUNCATE TABLE datawarehouse_final;'
        )
        conn.execute(statement)

    # INSERT New data in final table. Join the needed data from the different stage tables
    with db.connect().execution_options(autocommit=True) as conn:
        statement = text(
            f'''INSERT INTO datawarehouse_final 
            SELECT miv_hourly_stage."MessungDatZeit",
            to_char(miv_hourly_stage."MessungDatZeit",'DD:MM:YYYY'),
            to_char(miv_hourly_stage."MessungDatZeit",'YYYY'),
            to_char(miv_hourly_stage."MessungDatZeit",'Mon'),
            to_char(miv_hourly_stage."MessungDatZeit",'WW'),
            to_char(miv_hourly_stage."MessungDatZeit",'HH24:MI:SS'),
            to_char(miv_hourly_stage."MessungDatZeit",'Dy'),
            miv_hourly_stage."Strassenname",
            miv_hourly_stage."NKoord",
            miv_hourly_stage."EKoord",
            weather_hourly_stage."Temperature", 
            weather_hourly_stage."Rainduration",
            miv_hourly_stage."AnzFahrzeuge",
            miv_hourly_stage."AnzFahrzeugeStatus",
            oilprice_daily_stage."close", 
            oilprice_daily_stage."news_link", 
            CAST(vacation_daily_stage."vacation_flag" AS BOOLEAN)
            FROM miv_hourly_stage
            LEFT JOIN weather_hourly_stage ON miv_hourly_stage."MessungDatZeit" = weather_hourly_stage."Datetime" AND 
            miv_hourly_stage."Strassenname" = weather_hourly_stage."Standort"
            LEFT JOIN oilprice_daily_stage ON DATE(miv_hourly_stage."MessungDatZeit") = oilprice_daily_stage."date"
            LEFT JOIN vacation_daily_stage ON DATE(miv_hourly_stage."MessungDatZeit") = vacation_daily_stage."date";'''
        )
        conn.execute(statement)

    print('Finished process of inserting data')
    
    return {
        'statusCode': 200,
        'body': json.dumps('process finished')
    }


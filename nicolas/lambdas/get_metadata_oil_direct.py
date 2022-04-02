# Import required packages
import os
import pandas as pd
import yfinance as yf
import json
import psycopg2
from sqlalchemy import create_engine
from datetime import date

# DB configuration
config = {
    'host': os.environ['host'],
    'port': os.environ['port'],
    'dbname': os.environ['dbname'],
    'user': os.environ['user'],
    'password': os.environ['password']
}

# Configure cnx_string for sqlalchemy
cnx_str = f'postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["dbname"]}'

# Set ticker for required instrument "Brent Crude Oil"
brent = yf.Ticker("BZ=F")


def lambda_handler(event, context):
    # Get metadata and convert it into csv format
    dict_metadata = brent.info
    df_metadata = pd.DataFrame(dict_metadata, index=[0]).T.reset_index()
    df_metadata.rename(columns={'index': 'key', 0: 'value'}, inplace=True)
    df_metadata

    # Establish connection to database 'lakehouse'
    try:
        conn = psycopg2.connect(
            dbname=config['dbname'],
            user=config['user'],
            host=config['host'],
            password=config['password'],
            port=config['port']
        )

    except psycopg2.Error as e:
        print("Error: Could not make the connection to the postgres database")
        print(e)

    # Create cursor
    try:
        cursor = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get the cursor to the database")
        print(e)

    # Set auto commit feature
    conn.set_session(autocommit=True)

    # Create engine
    engine = create_engine(cnx_str)

    # Create table 'metadata_oil'
    sql = """
        CREATE TABLE IF NOT EXISTS metadata_oil (
            key text,
            value text
            )
    """
    cursor.execute(sql)

    # Insert values into table using sqlalchemy
    df_metadata.to_sql('metadata_oil', engine, if_exists='replace', index=False)

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Metadata from {date.today()} successfully loaded into RDS database')
    }

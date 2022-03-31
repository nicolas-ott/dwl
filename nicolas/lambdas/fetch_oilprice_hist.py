import os
import json
import yfinance as yf
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

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


def lambda_handler(event, context):
    # Set ticker for required instrument "Brent Crude Oil"
    brent = yf.Ticker("BZ=F")

    # Fetch data from Yahoo Finance API
    hist = brent.history(period="max")

    # Remove columns 'Dividends' and 'Stock Splits'
    hist.drop(['Dividends', 'Stock Splits'], inplace=True, axis=1)

    # Remove last row
    hist.drop(hist.tail(1).index, inplace=True)

    # Put the date index as column and convert it to date format
    hist = hist.reset_index(level=0)
    hist['Date'] = pd.to_datetime(hist['Date']).dt.date

    # Rename columns
    hist.rename(columns={'Date': 'date',
                         'Open': 'open',
                         'High': 'high',
                         'Low': 'low',
                         'Close': 'close',
                         'Volume': 'volume'},
                inplace=True)

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

    # # Create engine
    engine = create_engine(cnx_str)

    # Create table oilprice
    sql = """
        CREATE TABLE IF NOT EXISTS oilprice (
            date DATE,
            open float,
            high float,
            low float,
            close float,
            volume INT
            )
    """
    cursor.execute(sql)

    # Insert values into table using sqlalchemy
    hist.to_sql('oilprice', engine, if_exists='replace', index=False)

    # Count inserted values
    sql = '''
    SELECT COUNT(*) cnt
    FROM oilprice;
    '''

    cursor.execute(sql)
    count = str(cursor.fetchone()[0])

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'{count} records loaded successfully into table oilprice')
    }

import os
import json
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
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

    # Get latest date of table oilprice
    sql = '''
        SELECT date
        FROM oilprice
        ORDER BY date DESC
        LIMIT 1;
        '''
    cursor.execute(sql)
    last_date = str(cursor.fetchone()[0] + timedelta(days=1))

    # Get oil price data dynamically
    data = yf.download("BZ=F", start=last_date, end=str(date.today()))

    # Remove column 'Close'
    data.drop(['Close'], inplace=True, axis=1)

    # Put the date index as column and convert it to date format
    data = data.reset_index(level=0)
    data['Date'] = pd.to_datetime(data['Date']).dt.date

    # Rename columns
    data.rename(columns={'Date': 'date',
                         'Open': 'open',
                         'High': 'high',
                         'Low': 'low',
                         'Adj Close': 'close',
                         'Volume': 'volume'},
                inplace=True)

    # Insert values into table using sqlalchemy
    data.to_sql('oilprice', engine, if_exists='append', index=False)

    # Check the most recently inputted values
    sql = '''
        SELECT *
        FROM oilprice
        ORDER BY date DESC
        LIMIT 1;
        '''
    cursor.execute(sql)
    last_record = str(cursor.fetchone()[0])

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f' Records until {last_record} loaded successfully into table oilprice')
    }

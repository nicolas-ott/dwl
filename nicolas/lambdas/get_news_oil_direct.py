# Import required packages
import os
import pandas as pd
import yfinance as yf
import json
from pprint import pprint
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
    # Get news and convert it into JSON
    news = json.dumps(brent.news)

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

    # Create table news_oil using the JSONB data type
    sql = """
        CREATE TABLE IF NOT EXISTS news_oil (
          ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
          news jsonb NOT NULL
          );
    """
    cursor.execute(sql)

    # Upload news into db
    sql = """
        INSERT INTO news_oil (news)
        VALUES (%s);
    """
    cursor.execute(sql, [news])

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'news from {date.today()} succesfully loaded into database')
    }

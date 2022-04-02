# Import required packages
import os
import pandas as pd
import yfinance as yf
import json
import boto3
from boto3 import client
import psycopg2
from sqlalchemy import create_engine
from datetime import date

# Set aws credentials
ID = os.environ['ID']
KEY = os.environ['KEY']
TOKEN = os.environ['TOKEN']

# Set bucket
bucket = 'lakehousebucket'

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

# Set file name
file = f'news_oil/{date.today()}_news.json'

# Set local path
local_path = '/tmp/' + file.split('/')[-1]


# Create lambda handler function
def lambda_handler(event, context):
    # Get metadata and convert it into JSON
    news = json.dumps(brent.news)

    # Creating boto3 resource
    s3 = boto3.resource('s3',
                        aws_access_key_id=ID,
                        aws_secret_access_key=KEY,
                        aws_session_token=TOKEN)

    # Upload metadata to s3 bucket 'lakehousebucket'
    object = s3.Object(bucket, file)
    result = object.put(Body=news)

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

    # Downloading the JSON file from S3 and executing a COPY statement to get data into tabele news_oil on RDS
    s3.meta.client.download_file(bucket, file, local_path)

    f = open(local_path, "r")
    cursor.copy_expert('COPY news_oil (news) FROM STDIN;', f)

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'news from {date.today()} succesfully saved in S3 bucket and loaded into database')
    }


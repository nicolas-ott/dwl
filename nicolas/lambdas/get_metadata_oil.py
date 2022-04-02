# Import required packages
import os
import pandas as pd
import yfinance as yf
import json
from pprint import pprint
import boto3
from boto3 import client
import psycopg2
from sqlalchemy import create_engine
from datetime import date
from io import StringIO

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

# Set file name and path
file = f'{date.today()}_metadata.csv'
path = f'metadata_oil/{file}'


def lambda_handler(event, context):
    # Get metadata and convert it into csv format
    dict_metadata = brent.info
    df_metadata = pd.DataFrame(dict_metadata, index=[0]).T.reset_index()
    csv_buffer = StringIO()
    df_metadata.to_csv(csv_buffer, header=False, index=False)

    # Creating boto3 resource
    s3 = boto3.resource('s3',
                        aws_access_key_id=ID,
                        aws_secret_access_key=KEY,
                        aws_session_token=TOKEN)

    # Upload metadata to s3 bucket 'lakehousebucket'
    object = s3.Object(bucket, path)
    result = object.put(Body=csv_buffer.getvalue())

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

    # Downloading the csv file from S3 and executing a table import statement to get data into table metadata_oil on RDS
    sql = """
        SELECT aws_s3.table_import_from_s3(
            'metadata_oil',
            '',
            '(FORMAT CSV, DELIMITER '','', HEADER false)',
            %s,
            %s,
            'us-east-1',
            %s,
            %s,
            %s
            );
    """
    cursor.execute(sql, (bucket, path, ID, KEY, TOKEN))

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Metadata successfully saved in S3 bucket and loaded into RDS database')
    }

# Import required packages
import json
import os
import numpy as np
import pandas as pd
import boto3
from boto3 import client
import psycopg2
from sqlalchemy import create_engine

# Set aws credentials
ID = os.environ['ID']
KEY = os.environ['KEY']
TOKEN = os.environ['TOKEN']

# Set bucket
bucket = os.environ['bucket']

# Set file path
file = os.environ['file']

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
    # Creating boto3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=ID,
        aws_secret_access_key=KEY,
        aws_session_token=TOKEN,
    )

    # Get data from S3 bucket
    response = s3_client.get_object(Bucket=bucket, Key=file)

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"))
        print(df)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")

    # Quality check
    df.loc[df.end_date < df.start_date]

    # Adjust date at index 4
    df.loc[4]['end_date'] = '2019-01-05 00:00:00'

    # Expand df with column 'date' and 'vacation_flag'
    df['date'] = df.apply(lambda x: pd.date_range(start=x['start_date'], end=x['end_date']), axis=1)
    df = df.explode('date').reset_index(drop=True)
    df['vacation_flag'] = 1

    # Remove, rename and reorder columns
    df.drop(columns=['start_date', 'end_date', 'created_date'], inplace=True)
    df.rename(columns={'summary': 'vacation_text'}, inplace=True)
    df = df.reindex(columns=['date', 'vacation_flag', 'vacation_text'])

    # Drop duplicate values, extend data frame with missing days, fill vacation_flag and vacation_text column
    df.drop_duplicates(subset=['date'], keep='first', inplace=True)
    df = df.set_index('date').asfreq('D', fill_value=0).reset_index()
    df.vacation_text.replace(0, np.nan, inplace=True)

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

    # Create table 'vacation_daily_stage'
    sql = """
        CREATE TABLE IF NOT EXISTS vacation_daily_stage (
            date DATE,
            vacation_flag INT,
            vacation_text text
            )
    """
    cursor.execute(sql)

    # Insert values into table using sqlalchemy
    df.to_sql('vacation_daily_stage', cnx_str, index=False, if_exists='replace')

    # Count inserted values
    sql = '''
    SELECT COUNT(*) cnt
    FROM vacation_daily_stage;
    '''

    cursor.execute(sql)
    count = str(cursor.fetchone()[0])

    # Close connection
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'{count} records loaded successfully into table vacations')
    }

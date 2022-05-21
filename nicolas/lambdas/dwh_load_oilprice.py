import os
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# DB configuration
db1_config = {
    'host': os.environ['db1_host'],
    'port': os.environ['db1_port'],
    'user': os.environ['db1_user'],
    'password': os.environ['db1_password'],
    'dbname': os.environ['db1_dbname'],
}

db2_config = {
    'host': os.environ['db2_host'],
    'port': os.environ['db2_port'],
    'user': os.environ['db2_user'],
    'password': os.environ['db2_password'],
    'dbname': os.environ['db2_dbname'],
}

# Configure cnx_string for sqlalchemy
db1 = f'postgresql://{db1_config["user"]}:{db1_config["password"]}@{db1_config["host"]}/{db1_config["dbname"]}'
db2 = f'postgresql://{db2_config["user"]}:{db2_config["password"]}@{db2_config["host"]}/{db2_config["dbname"]}'


def lambda_handler(event, context):
    # Establish connection to database 'datawarehouse'
    try:
        dwh_conn = psycopg2.connect(
            dbname=db2_config['dbname'],
            user=db2_config['user'],
            host=db2_config['host'],
            password=db2_config['password'],
            port=db2_config['port']
        )

    except psycopg2.Error as e:
        print("Error: Could not make the connection to database 'datawarehouse'")
        print(e)

    # Create cursor
    try:
        dwh_cursor = dwh_conn.cursor()

    except psycopg2.Error as e:
        print("Error: Could not get the cursor to the database 'datawarehouse'")
        print(e)

    # Set auto commit feature
    dwh_conn.set_session(autocommit=True)

    # Create engines
    db1_engine = create_engine(db1)
    db2_engine = create_engine(db2)

    # Create table 'oilprice_daily_stage' in DWH
    sql = """
        CREATE TABLE IF NOT EXISTS oilprice_daily_stage (
            date DATE,
            close float,
            news_link text
            )
    """
    dwh_cursor.execute(sql)

    # Query for price data
    sql1 = '''
         SELECT date, close
         FROM oilprice
         ORDER BY date ASC;
         '''

    # Query for news data
    sql2 = '''
        SELECT ingested_at, news
        FROM news_oil
        ORDER BY ingested_at ASC;
        '''

    df_oilprice = pd.read_sql(sql1, db1)
    df_news = pd.read_sql(sql2, db1)

    # Transform news data
    df_news.loc[:, 'news_link'] = df_news.news.map(lambda x: x[0]['link'])
    df_news['date'] = pd.to_datetime(df_news['ingested_at']).dt.date
    df_news.drop(columns=['ingested_at', 'news'], inplace=True)

    # Merge into single dataframe
    df = pd.merge(df_oilprice, df_news, on='date', how='left')

    # Extend data frame with missing trading days
    # Forward fill closing price and news column
    date_range = pd.date_range(start=df.date.min(), end=df.date.max())
    df = df.set_index('date').reindex(date_range).fillna(method='ffill').rename_axis('date').reset_index()

    # Insert values into table 'oilprice_daily_stage' using sqlalchemy
    df.to_sql('oilprice_daily_stage', db2, index=False, if_exists='replace')

    # Count inserted values
    sql = '''
        SELECT COUNT(*) cnt
        FROM oilprice_daily_stage;
        '''
    count = pd.read_sql(sql, db2).values[0][0]

    # Close connection
    dwh_cursor.close()
    dwh_conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'{count} records loaded successfully into table oilprice (DWH)')
    }

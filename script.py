# import csv
import os
import requests
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()


# "source pythonenv/bin/activate" -> to activate the virtual terminal

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "STOCK_DATA")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "TICKERS")

LIMIT = 1000

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

def create_table_if_not_exists(conn):
    """Create the tickers table if it doesn't exist"""
    cursor = conn.cursor()
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        ticker VARCHAR(50),
        name VARCHAR(500),
        market VARCHAR(50),
        locale VARCHAR(10),
        primary_exchange VARCHAR(50),
        type VARCHAR(50),
        active BOOLEAN,
        currency_name VARCHAR(10),
        cik VARCHAR(20),
        composite_figi VARCHAR(50),
        share_class_figi VARCHAR(50),
        last_updated_utc TIMESTAMP_NTZ,
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    
    cursor.execute(create_table_sql)
    cursor.close()
    print(f"Table {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} is ready")

def insert_tickers_to_snowflake(conn, tickers):
    """Insert ticker data into Snowflake table"""
    if not tickers:
        print("No tickers to insert")
        return
    
    cursor = conn.cursor()
    
    # Prepare the insert statement
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
    (ticker, name, market, locale, primary_exchange, type, active, currency_name, 
     cik, composite_figi, share_class_figi, last_updated_utc)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Prepare data rows
    rows = []
    for ticker in tickers:
        rows.append((
            ticker.get('ticker'),
            ticker.get('name'),
            ticker.get('market'),
            ticker.get('locale'),
            ticker.get('primary_exchange'),
            ticker.get('type'),
            ticker.get('active'),
            ticker.get('currency_name'),
            ticker.get('cik'),
            ticker.get('composite_figi'),
            ticker.get('share_class_figi'),
            ticker.get('last_updated_utc')
        ))
    
    # Execute batch insert
    cursor.executemany(insert_sql, rows)
    cursor.close()
    print(f"Inserted {len(tickers)} tickers into Snowflake table")

def run_stock_ticker_job():
    url = (
        "https://api.massive.com/v3/reference/tickers"
        f"?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    )
    response = requests.get(url)
    # print(response.json())
    tickers = []
    data = response.json()

    for item in data["results"]:
        tickers.append(item)

    while 'next_url' in data:
        print("requesting next page", data['next_url'])
        response = requests.get(data['next_url'] + f"&apiKey={POLYGON_API_KEY}")
        data = response.json()
        print(data)
        for item in data['results']:
            tickers.append(item)

    # Connect to Snowflake and insert data
    conn = None
    try:
        conn = get_snowflake_connection()
        create_table_if_not_exists(conn)
        insert_tickers_to_snowflake(conn, tickers)
        print(f"Successfully processed {len(tickers)} tickers to Snowflake")
    except Exception as e:
        print(f"Error inserting data to Snowflake: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_stock_ticker_job()
import requests
import os
import csv
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    HAS_SNOWFLAKE = True
except Exception:
    HAS_SNOWFLAKE = False
load_dotenv() # loads values from .env into environment

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
DS = '2025-12-06'  # date stamp for data load

DS = datetime.now().strftime('%Y-%m-%d')
url = f'https://api.massive.com/v3/reference/tickers?market=indices&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'

response = requests.get(url) # sending request to the url
tickers = []
 
# Parse JSON safely (API may return an error object)
try:
    data = response.json()
except ValueError:
    print('Initial response not JSON; aborting. Response text:')
    print(response.text)
    data = {}

# The API normally returns a dict with keys: 'results', 'status', 'request_id', 'count', 'next_url'.
# Only proceed when 'results' is present; otherwise log and continue to write whatever we've collected.
if 'results' in data:
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)
else:
    print("'results' not found in initial response:", data)

print(f'Initial length: {len(tickers)}')

# We can adjust the LIMIT, but don't set it to larger numbers like 10000, as this can lead to error
# Let's set this to 1000 for now. But there are obviously more than 1000 stocks listed in the market. 
# The URL is paginated, meaning, when we send GET request to the original URL, we only get the first 'LIMIT' number of stocks (here it is 1000)
# The remaining stocks are located in another URL. This URL is stored as 'next_url' key in data.

# So, as long as there is a key 'next_url' in the response, we must continue the above process. 
# A while loop is the best way to achieve this.
# We check data contains 'next_url', if it does, we send request to the 'next_url' (we must authenticate with API key)

while 'next_url' in data:
    print('---------- Requesting next page ----------')
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}') # authenticating with API key
    try:
        data = response.json()
    except ValueError:
        print('Next page response not JSON; stopping. Response text:')
        print(response.text)
        break

    # If API returns an error (rate limit, etc.) it may not include 'results'
    if 'results' not in data:
        print("No 'results' in response; stopping. Response:", data)
        break

    for ticker in data['results']:
        ticker['ds'] = DS  # adding date stamp field
        tickers.append(ticker) # append them to the list ticker too

example_ticker = {'ticker': 'I:DJIEZM', 
 'name': 'Dow Jones Islamic Market Euro Mid-Cap Index', 
 'market': 'indices', 
 'locale': 'us', 
 'active': True, 
 'source_feed': 'CMEMarketDataPlatformDowJones',
 'ds': '2025-12-06'  # adding date stamp field
 }

# Write tickers to CSV with the same schema as example_ticker
fieldnames = ['ticker', 'name', 'market', 'locale', 'active', 'source_feed']
# Prepare rows normalized to the expected schema
rows = []
for t in tickers:
    rows.append({k: t.get(k) for k in fieldnames})

# If Snowflake credentials are provided and connector is installed, load to Snowflake
SNOW_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOW_USER = os.getenv('SNOWFLAKE_USER')
SNOW_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOW_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOW_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOW_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOW_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOW_TABLE = os.getenv('SNOWFLAKE_TABLE', 'TICKERS')

def write_csv(rows):
    try:
        with open('tickers.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f'Wrote {len(rows)} rows to tickers.csv')
    except Exception as e:
        print('Failed to write CSV:', e)

if HAS_SNOWFLAKE and SNOW_ACCOUNT and SNOW_USER and SNOW_PASSWORD:
    try:
        # Convert rows to DataFrame
        df = pd.DataFrame(rows)
        df.columns = [c.upper() for c in df.columns]

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOW_USER,
            password=SNOW_PASSWORD,
            account=SNOW_ACCOUNT,
            warehouse=SNOW_WAREHOUSE,
            database=SNOW_DATABASE,
            schema=SNOW_SCHEMA,
            role=SNOW_ROLE,
        )

        # Ensure table exists (simple DDL - adjust types as needed)
        create_ddl = f'''
        create table if not exists {SNOW_SCHEMA}.{SNOW_TABLE} (
            ticker varchar,
            name varchar,
            market varchar,
            locale varchar,
            active boolean,
            source_feed varchar,
            ds varchar
        )'''
        try:
            cur = conn.cursor()
            # create schema if needed
            if SNOW_DATABASE and SNOW_SCHEMA:
                cur.execute(f'create database if not exists {SNOW_DATABASE}')
                cur.execute(f'use database {SNOW_DATABASE}')
                cur.execute(f'create schema if not exists {SNOW_SCHEMA}')
                cur.execute(f'use schema {SNOW_SCHEMA}')
            cur.execute(create_ddl)
        finally:
            try:
                cur.close()
            except Exception:
                pass

        # Use write_pandas to load DataFrame to Snowflake
        success, nchunks, nrows, _ = write_pandas(conn, df, SNOW_TABLE)
        if success:
            print(f'Successfully loaded {nrows} rows into {SNOW_DATABASE}.{SNOW_SCHEMA}.{SNOW_TABLE}')
        else:
            print('write_pandas reported failure; falling back to CSV')
            write_csv(rows)

        conn.close()
    except Exception as e:
        print('Snowflake load failed, falling back to CSV. Error:', e)
        write_csv(rows)
else:
    if not HAS_SNOWFLAKE:
        print('Snowflake connector not available; writing CSV instead.')
    else:
        print('Snowflake credentials not set; writing CSV instead.')
    write_csv(rows)

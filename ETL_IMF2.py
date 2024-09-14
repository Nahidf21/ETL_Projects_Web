# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

# Extract function
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    # Find all tables
    tables = data.find_all('tbody')
    
    # Extract rows from the target table
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

# Transform function
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    
    # Clean and convert GDP values to float
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    
    # Convert from millions to billions
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]
    
    # Update the DataFrame
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df

# Load to CSV function
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

# Load to database function
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Query function
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Log progress function
def log_progress(message): 
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Standard timestamp format
    now = datetime.now()  # Get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt", "a") as f: 
        f.write(f"{timestamp} : {message}\n")


# Example usage of the functions:

# 1. Extract data
table_attributes = ["Country", "GDP_USD_millions"]
df = extract(url, table_attributes)
log_progress("Data extracted successfully")

# 2. Transform data
df_transformed = transform(df)
log_progress("Data transformed successfully")

# 3. Load data to CSV
csv_path = "./gdp_data.csv"
load_to_csv(df_transformed, csv_path)
log_progress(f"Data loaded to {csv_path}")

# 4. Load data to SQLite database
conn = sqlite3.connect('gdp_data.db')
table_name = 'Country_GDP'
load_to_db(df_transformed, conn, table_name)
log_progress(f"Data loaded to SQLite database in table {table_name}")

# 5. Querying the database
query = "SELECT * FROM Country_GDP LIMIT 5"
run_query(query, conn)
log_progress("Query executed successfully")

# Close the SQLite connection
conn.close()
log_progress("SQLite connection closed")

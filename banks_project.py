# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
from datetime import datetime 

url = r'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'

# Log progress function
def log_progress(message): 
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Standard timestamp format
    now = datetime.now()  # Get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt", "a") as f: 
        f.write(f"{timestamp} : {message}\n")


def extract(url):
    page = requests.get(url).text
    soup = BeautifulSoup(page,'html')

    table = soup.find_all('tbody')
    dic ={'Bank Name':[],'Market CAP (US$ billion)':[]}
    table_body = table[0].find_all('tr')
    for t_b in table_body[1:11]:
        for i in range(0,3):
            t=t_b.find_all('td')[i]
            if i==1:
                dic['Bank Name'].append(t.text.replace("/n","").strip())
            if i==2:
                dic['Market CAP (US$ billion)'].append(float(t.text.replace("/n","").strip()))

    return pd.DataFrame(dic)      

def transform(extract_df):
    extract_df["MC_GBP_Billion"] = round(extract_df['Market CAP (US$ billion)'] * 0.8,2)
    extract_df["MC_EUR_Billion"] = round(extract_df['Market CAP (US$ billion)'] * 0.93,2)
    extract_df["MC_INR_Billion"] = round(extract_df['Market CAP (US$ billion)'] * 82.95,2)

    return extract_df

def load_csv(transform_df,output_path):
    transform_df.to_csv(output_path, index=False)

def load_sqlBD(transform_df,sql_connection,table_name):
    transform_df.to_sql(table_name, sql_connection, if_exists= 'replace', index= False)
    

def run_query(query_statement, sql_connection):
    dbsql =pd.read_sql(query_statement,sql_connection)
    print(dbsql)


output_path = "Largest_banks_data"

sql_connection = sqlite3.connect('Banks.db')
table_name = "Largest_banks"
query_statement = "select * from Largest_banks"
log_progress("Data extraction begin")
extract_df =extract(url)
log_progress("Data transformetion begin")
transform_df = transform(extract_df)
log_progress("CSV load begin")
load_csv(transform_df,output_path)
log_progress("SQL load begin")
load_sqlBD(transform_df,sql_connection,table_name)
log_progress(f"{query_statement}")
run_query(query_statement, sql_connection)
sql_connection.close
log_progress(f"SQL Connection close")

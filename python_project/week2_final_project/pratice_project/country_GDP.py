import glob 
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np 
import requests

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_name = "GDP_of_country"
table_attributes = ["Country","IMF","World_Bank"]
csv_path = "GDP_of_country.csv"
log_path = "GDP_of_country.txt"
db_name = "GDP_of_country.db"

# Task 1: Extracting Information
def extract(url, table_attributes):
    # Create empty dataframe
    dataframe = pd.DataFrame(columns=table_attributes)
    
    # Get info from web
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # Get table
    table = data.find_all('tbody')
    
    # Get rows
    rows = table[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                dataDict = {"Country": col[0].a.contents[0],
                            "IMF": col[2].contents[0],
                            "World_Bank": col[4].contents[0]}
                df1 = pd.DataFrame(dataDict, index=[0])
                dataframe = pd.concat([dataframe, df1], ignore_index=True)
    return dataframe

# Task 2: Transforming Information
def transform(dataframe):
    # Transform to list string
    GDP_list = dataframe["IMF"].tolist()
    
    # Transform to float
    for i, GDPs in enumerate(GDP_list):
        GDP_list[i] = float("".join(GDPs.split(',')))
    
    # Transform to billions
    for i, GDPs in enumerate(GDP_list):
        GDP_list[i] = np.round(GDPs/1000, 2)
        
    dataframe["IMF"] = GDP_list
    # Rename columns
    dataframe = dataframe.rename(columns={"IMF": "IMF_billions"})
    return dataframe

# Task 3: Loading Information
# Load to csv file
def load(dataframe, csv_path):
    dataframe.to_csv(csv_path, index=False)

def load_to_database(dataframe, table_name, sql_connection):
    dataframe.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Task 4: Query the database table
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Task 5: Logging progress
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_path, 'a') as f:
        f.write(timestamp + ":" + message +'\n')

# Task 6: Function call
log_progress("Initiating ETL progress")

dataframe = extract(url, table_attributes)

log_progress("Data extraction complete. Initiating Transforms")

dataframe = transform(dataframe)

log_progress("Data transformation complete. Initiating Loading to csv")

load(dataframe, csv_path)

log_progress("Data loading complete. Initiating SQL connection")
sql_connection = sqlite3.connect(db_name)

log_progress("SQL connection complete. Initiating Loading to database")

load_to_database(dataframe, table_name, sql_connection)

query_statement = f"SELECT * FROM {table_name} WHERE IMF_billions >= 100"
run_query(query_statement, sql_connection)

log_progress("ETL Job Ended")

sql_connection.close()

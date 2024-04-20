import requests
import pandas as pd 
import numpy as np 
import glob 
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3 

#Initialization of known entities
url = "	https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
db_name = "Banks.db"
table_name = "Largest_banks"
Log_path = "code_log.txt"
csv_path = "Largest_banks_data.csv"
table_attributes_after_extraction = ["Name","MC_USD_Billion"]
#table_attributes_final = ["Name","MC_USD_Billion","MC_GBP_Billion","MC_EUR_Billion","MC_INR_Billion"]

#create sql_connection
sql_connection = sqlite3.connect(db_name)

# function write log
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(Log_path,"a") as f: 
        f.write(timestamp + ':' + message + '\n')

#extract in4 from website
def extract(url,table_attributes):
    #create name of attributes
    dataframe = pd.DataFrame(columns= table_attributes)

    #get infor from web
    htmp_page = requests.get(url).text
    data = BeautifulSoup(htmp_page, 'html.parser')

    table = data.find_all("tbody")
    rows = table[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0 :
            data_dict = {"Name": col[1].find_all("a")[1]["title"],
                         "MC_USD_Billion":col[2].contents[0]}
            df = pd.DataFrame(data_dict,index=[0])
            dataframe = pd.concat([dataframe,df], ignore_index=True)
    return dataframe

def transform(dataframe):
    #create dataframe
    exchange_rates = pd.read_csv("exchange_rate.csv")

    #convert to a dic with "Currency" as Key and "Rate" as values
    exchange_rates = exchange_rates.set_index("Currency")
    exchange_rates = exchange_rates.to_dict()["Rate"]

    #convert MC_USD_Billions to float
    dataframe["MC_USD_Billion"] = dataframe["MC_USD_Billion"].astype(float)
    
    #add columns and round off 2 decimals
    dataframe["MC_GBP_Billion"] = np.round(dataframe["MC_USD_Billion"] * exchange_rates["GBP"], 2)
    dataframe["MC_EUR_Billion"] = np.round(dataframe["MC_USD_Billion"] * exchange_rates["EUR"], 2)
    dataframe["MC_INR_Billion"] = np.round(dataframe["MC_USD_Billion"] * exchange_rates["INR"], 2)

    return dataframe

#load_to_csv
def load_to_csv(dataframe,csv_path):
    dataframe.to_csv(csv_path)

#def load_to_database
def load_to_database(dataframe,sql_connection,table_name):
    dataframe.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement,sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress("Init ETL")

dataframe = extract(url,table_attributes_after_extraction)

log_progress("done ETL. Init transform")

dataframe = transform(dataframe)
print(dataframe)

log_progress("done transform. Init loading to csv file")

load_to_csv(dataframe,csv_path)

log_progress("done loading to csv file. Init loading to database")

load_to_database(dataframe,sql_connection,table_name)

log_progress("done loading to database. Excute loading")
#Call run_query() 
# 1. Print the contents of the entire table
query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

log_progress("Process Completed")

sql_connection.close()

log_progress("Connection closed")



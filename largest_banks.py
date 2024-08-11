import pandas as pd 
import numpy as np
from bs4 import BeautifulSoup
import requests
from datetime import datetime 
import sqlite3

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
ingested_data_cols = ['Name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
xrate_path = 'exchange_rate.csv'
conn = sqlite3.connect(db_name)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as log:
        log.write(timestamp + ': ' + message + '\n')

def extract(url,table_attributes):
    r = requests.get(url).text
    soup = BeautifulSoup(r,'html.parser')
    #initialize empty df
    df = pd.DataFrame(columns=table_attributes)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name":bank_name,
                         "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1],ignore_index=True)

    return df

def transform(df,xrate_path):
    #obtain the exchange rates, add 3 new columns to the df with the multiplication to create a new df
    exchange_rates = pd.read_csv(xrate_path)
    exchange_rates = exchange_rates.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rates['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rates['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rates['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df,path):
    df.to_csv(path, index = False)


def load_to_db(conn, df, tbl):
    df.to_sql(tbl,conn,if_exists ='replace', index=False)
    
def run_queries(conn,sql):
    print(sql,':\n',pd.read_sql(sql, conn),'\n')


extracted_data = extract(url,['Name','MC_USD_Billion'])
log_progress('Finished data extraction')
transformed_data = transform(extracted_data,xrate_path)
log_progress('Finished data transformation')
load_to_csv(transformed_data,output_path)
log_progress('Finished data loading to csv')
load_to_db(conn,transformed_data,table_name)
log_progress('Finished connecting do db')
queries = ['SELECT * FROM Largest_banks','SELECT AVG(MC_GBP_Billion) FROM Largest_banks','SELECT Name from Largest_banks LIMIT 5']
for query in queries:
    run_queries(conn,query)
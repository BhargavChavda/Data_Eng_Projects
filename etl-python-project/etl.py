import json
import pandas as pd
from datetime import datetime
import requests
import sqlite3
from bs4 import BeautifulSoup as bs

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
attr = ['Bank_Name','Market_Cap_USD','Market_Cap_EUR','Market_Cap_GBP','Market_Cap_INR']
csvpath = '/home/bc/Desktop/Data_Eng_Projects/etl-python-project/transformed.csv' 

def log_progress(message):
    time = datetime.now()
    with open('/home/bc/Desktop/Data_Eng_Projects/etl-python-project/code_log.txt','a') as f:
        f.write(str(time) + " : " + message + '\n')


def extract(url,attr):
    ds = pd.DataFrame(columns=attr)
    data = requests.get(url).text
    soup = bs(data,'html.parser')
    tables = soup.find_all('table')
    dict = {"Bank_Name":[],"Market_Cap_USD":[]} 
    for row in tables[0].tbody.find_all('tr'):
        columns = row.find_all('td')
        if(columns!=[]):
            name = columns[1].text.strip()
            mc = columns[2].text.strip()
            dict["Bank_Name"].append(name)
            dict["Market_Cap_USD"].append(mc)
            ds1 = pd.DataFrame(dict)
    ds = pd.concat([ds,ds1])
    return ds


def transform():
    eur=[]
    gbp=[]
    inr=[]
    data = pd.read_csv('exchange_rate.csv')
    dict = data.to_dict(orient="records")
    usdlist = ds['Market_Cap_USD'].tolist()
    for i in usdlist:
        eurn = round(float(i)*dict[0]['Rate'],2)
        eur.append(eurn)
        gbpn = round(float(i)*dict[1]['Rate'],2)
        gbp.append(gbpn)
        inrn = round(float(i)*dict[2]['Rate'],2)
        inr.append(inrn)
    ds['Market_Cap_EUR']=eur
    ds['Market_Cap_GBP']=gbp
    ds['Market_Cap_INR']=inr

    return ds


def load_to_csv(ds,csv_path):
    ds.to_csv(csv_path)


def load_to_db(df,sql_connection,table):
    df.to_sql(table,sql_connection,if_exists='replace',index=False)


def run_queries(q,sql_connection):
    qout = pd.read_sql(q,sql_connection)
    print(qout)



log_progress("Preliminaries complete. Initializing ETL Process")

ds = extract(url,attr)
log_progress("Data extraction complete. Initiating Transformation process")

transform()
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(ds,csvpath)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")

load_to_db(ds,sql_connection,'Largest_Banks')
log_progress("Data loaded to Database as a table, Executing queries")

q = input("Enter SQL Query: \n")
run_queries(q,sql_connection)
log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")

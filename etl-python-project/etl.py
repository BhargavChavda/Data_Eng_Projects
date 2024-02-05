import json
import pandas as pd
from datetime import datetime
import requests
import sqlite3
from bs4 import BeautifulSoup as bs

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
attr = ['Bank_Name','Market_Cap_USD','Market_Cap_EUR','Market_Cap_GBP','Market_Cap_INR']


def log_progress(message):
    time = datetime.now()
    with open('/home/bc/Desktop/Data_Eng_Projects/etl-python-project/code_log.txt','a') as f:
        f.write(str(time) + " : " + message + '\n')

#log_progress("Preliminaries complete. Initializing ETL Process")

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
ds = extract(url,attr)

def transform():
    eur=[]
    gbp=[]
    inr=[]
    data = pd.read_csv('exchange_rate.csv')
    dict = data.to_dict(orient="records")
    print(dict)
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

    print(ds)
    return ds
transform()

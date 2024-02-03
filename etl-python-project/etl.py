import json
import pandas as pd
import datetime
import requests
import sqlite3
import BeautifulSoup

def log_progress(message):
    time_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.datetime.now()
    timestamp = now.strftime(time_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ':' + message + '\n')
   
#log_progress("test")

def extract(url,attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=attributes)

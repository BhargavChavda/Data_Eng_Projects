import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log.txt'
final = 'trans.csv'

def extractcsv(file):
    df = pd.read_csv(file)
    return df

def extractjson(file):
    df = pd.read_json(file,lines=True)
    return df

def extractxml(file):
    df = pd.DataFrame(columns=["name","height","weight"])
    tree = ET.parse(file)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat([df,pd.DataFrame([{"name":name,"height":height,"weight":weight}])], ignore_index=True)
        return df

def extract():
    ex_data = pd.DataFrame(columns=['name','height','weight'])

    for csvfile in glob.glob("*.csv"):
        ex_data = pd.concat([ex_data,pd.DataFrame(extractcsv(csvfile))],ignore_index = True)
    

    for jsonfile in glob.glob("*.json"):
        ex_data = pd.concat([ex_data,pd.DataFrame(extractcsv(jsonfile))],ignore_index = True)
    

    for xmlfile in glob.glob("*.xml"):
        ex_data = pd.concat([ex_data,pd.DataFrame(extractxml(xmlfile))],ignore_index = True)

    return ex_data



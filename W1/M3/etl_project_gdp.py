import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import re
import datetime

# decorator for logging
# this decorator helps to log the ETL processes.
# function name, its start and end time, and running time will be logged.
def withLog(func):
    def wrapper(*args, **kwargs):
        with open("./etl_project_log.txt", "a") as f:
            f.write(datetime.datetime.now().strftime("%Y-%B-%d-%H-%M-%S, "))
            f.write(f"{func.__name__} start\n")
            startTime = datetime.datetime.now()

            result = func(*args, **kwargs)

            endTime = datetime.datetime.now()
            f.write(endTime.strftime("%Y-%B-%d-%H-%M-%S, "))
            f.write(f"{func.__name__} end. in {endTime-startTime}\n")
        return result
    return wrapper

    

# get the gdp data from wikipedia
@withLog
def extract(url):
    # get http response
    response = requests.get(url)

    # check reponse
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
    else : 
        print(response.status_code)

    # find all tables
    table = soup.select("table.wikitable")

    return table

# transform the html table data to pandas data frame and process the data
@withLog
def transform(data):
    # html table to pandas data frame
    df = pd.read_html(StringIO(str(data)))[0]

    # data Transformming
    cols = df.columns
    for col in cols:        # for all columns, delete the annotation
        df[col] = df[col].apply(lambda x: re.sub(r'\[.*?\]', '', str(x)))
    for col in cols[2::2]:  # for year columns, transform the data to integer
        df[col] = df[col].apply(lambda x: int(x) if x.isdigit() else -1)
    for col in cols[1::2]:  # for GDP data, make billion unit and float
        df[col] = df[col].apply(lambda x: round(float(x)/1000.0, 2) if x.isdigit() else -1.0)
        
    return df

# write the dataFrame to a json file
@withLog
def load(dataFrame):
    # extract dataFrame by json
    dataFrame.transpose().to_json('Countries_by_GDP.json')
        

url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

a = transform(extract(url))
load(a)
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

def extract(url, table_attribs):
    """
    Extract data from the web
    return a dataframe
    """
    # Get the page
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    # create an empty dataframe 
    df = pd.DataFrame(columns=table_attribs)
    # find the table
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    """
    Transform the data
    return a dataframe
    """

    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',', '')
    df['GDP_USD_millions'] = df['GDP_USD_millions'].astype(float)
    df['GDP_USD_millions'] = df['GDP_USD_millions'].apply(lambda x: round(x/1000, 2))
    df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"}, inplace=True)

    return df

def load_to_csv(df, csv_path):
    """
    Load the data to a csv file
    """
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    """
    Load the data to a sqlite database
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    """
    Run a query on the database
    """
    print(f"Running query: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    """
    Log the progress of the ETL process
    """
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('ETL_log.txt', 'a') as f:
        f.write(f'{timestamp} - {message}\n')


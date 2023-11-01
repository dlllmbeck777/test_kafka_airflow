#!/usr/bin/env python
# coding: utf-8

import datetime
import random

import psycopg2
from psycopg2 import sql
import requests

import pandas as pd
from collections.abc import MutableMapping

from io import StringIO

import logging
logging.basicConfig(level=logging.DEBUG)



def connect(params):
    # Подключение к базе данных PostgreSQL
    try:
        connection = psycopg2.connect(**params)
        print(f"Succesfully connected ...")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        # exit()


def flatten_dict(d, parent_key='', sep='_'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, MutableMapping):
            items.update(flatten_dict(v, new_key, sep=sep))
        elif isinstance(v, list) and all(isinstance(item, MutableMapping) for item in v):
            # for i, item in enumerate(v):
            #     items.update(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep))
            for item in v:
                items.update(flatten_dict(item, f"{new_key}", sep=sep))
        else:
            items[new_key] = v

    return items



def create_new_table(table_new_name,df_table,params_db):
    def getColumnDtypes(dataTypes):
        dataList = []
        for x in dataTypes:
            if(x == 'int64'):
                dataList.append('int')
            elif (x == 'float64'):
                dataList.append('float')
            elif (x == 'bool'):
                dataList.append('boolean')
            else:
                dataList.append('varchar')
        return dataList
    # ----- creating new table -----
    # table_new_name = 'table_metadata'

    columnDataType = getColumnDtypes(df_table.dtypes)
    columnName = list(df_table.columns.values)

    createTableStatement = f'CREATE TABLE IF NOT EXISTS {table_new_name} ('
    for i in range(len(columnDataType)):
        # createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + columnDataType[i] + ','
        createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + 'text' + ','
    createTableStatement = createTableStatement[:-1] + ' );'

    connection = connect(params_db)
    cursor = connection.cursor()

    cursor.execute(createTableStatement)
    connection.commit()

    cursor.close()
    connection.close()


def insert_func(table_insert,df_insert,params_insert_db):
    try:
        # Assuming you have a DataFrame named 'df'
        tmp_df = StringIO()
        df_insert.to_csv(tmp_df, sep=';', header=False, index=False)
        tmp_df.seek(0)

        conn = connect(params_insert_db)
        cursor = conn.cursor()
        
        try:
            # Check if the table exists
            exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {} LIMIT 1);").format(sql.Identifier(table_insert))
            cursor.execute(exists_query)

            # Fetch the result (True if the table exists, False if it doesn't)
            table_exists = cursor.fetchone()[0]            
            
            print(f"The table '{table_insert}' exists in the database.")
            
        except psycopg2.ProgrammingError as error:
            print(f"The table '{table_insert}' does not exist in the database")
            if 'does not exist' in str(error):
                print(f'Creating table {table_insert}...')
                create_new_table(table_insert,df_insert,params_insert_db)
            else:
                cursor.close()
                conn.close()
                return
        
        conn = connect(params_insert_db)
        cursor = conn.cursor()
        
        cursor.copy_expert(f"COPY {table_insert} FROM STDIN WITH CSV DELIMITER E';' NULL ''", tmp_df)
        conn.commit()

        cursor.close()
        conn.close()
        print('Succesfully inserted ...')
    except Exception as e:
        print(f"Error inserting to the database: {e}")
        # exit()  

params_db = {'dbname':'de_db',
            'user':'de_app',
            'password':'de_password',
            'host':'localhost',
            'port':'5432'}
# connect(params)


for i in range(1,11):
    print(f'Page: {i}')
    url = f"https://search.wb.ru/exactmatch/ru/common/v4/search?&query=%D0%BA%D1%83%D1%80%D1%82%D0%BA%D0%B0&curr=rub&dest=-1257786&regions=80,64,38,4,115,83,33,68,70,69,30,86,75,40,1,66,48,110,31,22,71,114,111&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false&limit=300&page={i}"

    payload = {}
    headers= {}

    response = requests.request("GET", url, headers=headers, data = payload)

    json_data = response.json() #text.encode('utf8')
    

    # Create a unique ID using the timestamp and a random number
    timestamp = datetime.datetime.now()
    unique_id = f"{timestamp.strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}"

    # Create product df
    products = json_data['data']['products']
    df_products = pd.DataFrame(products)
    df_products['products_id'] = unique_id    
    
    # Delete product keys from main dataframe
    del json_data['data']['products']
    json_data['data']['product_id'] = unique_id

    # Flatten the nested dictionary while preserving the structure
    flattened_data = flatten_dict(json_data)

    # Create a DataFrame from the flattened dictionary
    df_metadata = pd.DataFrame([flattened_data])
    

    # Inserting
    table_name_metadata = 'table_metadata'
    insert_func(table_name_metadata,df_metadata,params_db)

    table_name_products = 'table_products'
    insert_func(table_name_products,df_products,params_db)

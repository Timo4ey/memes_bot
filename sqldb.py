
import json
import requests
import psycopg2
from psycopg2.extensions import AsIs
import pandas as pd
from time import sleep
from datetime import datetime, timedelta


db_config = dict()
with open('D:/Job/WorkSpace/MemesDocker/db_config.json', 'r') as f:
    db_config = json.load(f)

# getting indexes 
def getIndexesFromMainDB() -> list:
    temp_list = []
    try:
        connection = psycopg2.connect(
            host = db_config['host'],
            user = db_config['user'],
            password = db_config['password'],
            database = db_config['db_name']
            )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("""SELECT id FROM main_db;""")
            temp_list = cursor.fetchall()
    except Exception as _ex:
        print('[INFO] Got an error:', _ex)
    finally:
        if connection:
            connection.close()
            print('[INFO] the connection is closed')
    return temp_list





def GetDataPostsSQL() -> list:
    temp_list = []
    try:
        connection = psycopg2.connect(
            host = db_config['host'],
            user = db_config['user'],
            password = db_config['password'],
            database = db_config['db_name']
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            get_table_query = """SELECT content_id FROM post_db"""
            #cursor.execute(get_table_query)
        #temp_df = pd.read_sql(get_table_query, con=connection)
            cursor.execute(get_table_query)
            temp_list = cursor.fetchall()
            temp_list = [x[0] for x in temp_list]
    except Exception as _ex:
        print('[INFO] got an error', _ex)
    finally:
        if connection:
            connection.close()
            print('[INFO] connection is closed')
    return temp_list


def sendDataContentSQL(arr) -> None:
    try:
        connection = psycopg2.connect(
            host = db_config['host'],
            user = db_config['user'],
            password = db_config['password'],
            database = db_config['db_name']
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            
            for ind in range(len(arr)):
                #insert_postgres = """INSERT INTO post_db 
                #VALUES(%s, %s,%s, %s, %s)
                #"""
                #print(tuple(arr.iloc[ind]))
                insert_postgres = f"""INSERT INTO content_db({', '.join(arr.columns.to_list())}) 
                VALUES{tuple(arr.iloc[ind])}"""
                
                #{','.join(tuple(arr.iloc[ind].to_list()))
                #VALUES({', '.join(cursor.mogrify("(%s, %s,%s, %s, %s)", x) for x in tuple(map(tuple, arr)))})
                cursor.execute(insert_postgres)


    except Exception as _ex:
        print('[INFO] got an error', _ex)
    finally:
        if connection:
            connection.close()
            print('[INFO] connection is closed')

    
def sendDataPostsSQL(arr) -> None:
    try:
        connection = psycopg2.connect(
            host = db_config['host'],
            user = db_config['user'],
            password = db_config['password'],
            database = db_config['db_name']
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            
            for ind in range(len(arr)):
                #insert_postgres = """INSERT INTO post_db 
                #VALUES(%s, %s,%s, %s, %s)
                #"""
                #print(tuple(arr.iloc[ind]))
                insert_postgres = f"""INSERT INTO post_db({', '.join(arr.columns.to_list())}) 
                VALUES{tuple(arr.iloc[ind])}"""
                
                #{','.join(tuple(arr.iloc[ind].to_list()))
                #VALUES({', '.join(cursor.mogrify("(%s, %s,%s, %s, %s)", x) for x in tuple(map(tuple, arr)))})
                cursor.execute(insert_postgres)


    except Exception as _ex:
        print('[INFO] got an error', _ex)
    finally:
        if connection:
            connection.close()
            print('[INFO] connection is closed')
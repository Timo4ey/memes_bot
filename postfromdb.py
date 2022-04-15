import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import tqdm

from time import sleep
import telegram
tg_token = {}

with open( 'D:/Job/WorkSpace/MemesDocker/telegram_access_token.json', 'r') as f:
    tg_token = json.load(f)

#telegram_bot_get_me = bot.get_me()
bot = telegram.Bot(token = tg_token['access_token'])
updates = bot.get_updates()
print(updates)

host = '127.0.0.1'
user = 'postgres'
password = 'Bedema99$$'
db_name = 'My'



def MemesForHoursDB(hours = 6):
    temp_list = []
    certain_time = datetime.strftime((datetime.today() - timedelta(hours=hours)), '%Y-%m-%d %H:%M:%S')
    try:
        connection = psycopg2.connect(
            host = host,
            user = user,
            password = password,
            database = db_name
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            postgres_insert = f"""
            SELECT pd.url, pd.text
            FROM post_db pd
            WHERE pd.content_id in (SELECT cb.content_id FROM content_db cb 
					WHERE public_date > '{str(certain_time)}' and cb.content_id = pd.content_id)"""

            cursor.execute(postgres_insert)
            temp_list = cursor.fetchall()
    except Exception as _ex:
        print('[INFO] Got an error while working PostgreSQL', _ex)
    finally:
        if connection:
            connection.close()
            print('[INFO] The connection is closed')
    return temp_list



def sendMemesDB(id, arr) -> None:
    try:
        for item in arr:
            getting_image = requests.get(item[0])
            bot.send_photo(photo=getting_image.content,  chat_id=id, caption=item[1])
                    
    except Exception as _ex:
        print('[INFO] the error is', _ex)

















def MemesForHours_DB2(hours = 6):
    temp_list = []
    certain_time = datetime.strftime((datetime.today() - timedelta(hours=hours)), '%Y-%m-%d %H:%M:%S')
    try:
        connection = psycopg2.connect(
            host = host,
            user = user,
            password = password,
            database = db_name
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            postgres_insert = f"""SELECT url, text FROM memes_db WHERE new_date > '{str(certain_time)}'"""
            cursor.execute(postgres_insert)
            temp_list = cursor.fetchall()
    except Exception as _ex:
        print('[INFO] Got an error while working PostgreSQL', _ex)
    finally:
        if connection:
            connection.close()
            print('[INFO] The connection is closed')
    return temp_list






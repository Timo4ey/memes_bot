import requests
from psycopg2.extensions import AsIs
import pandas as pd
from time import sleep
from datetime import datetime
from config import vk_access_token, db_db_name, db_host, db_password, db_user
from sqldb import DataBase_Con


def getVkJson(list_of_indexes) -> pd.DataFrame:
    df = pd.DataFrame()
    for x in list_of_indexes:
        url = 'https://api.vk.com/method/'
        method_wall = 'wall.get'
        v = '5.131'
        params_wall = {
        'owner_id':x[0]*-1,
        'access_token':vk_access_token,
        'count': 30,
        'v':v
    }
        req_wall = requests.get(url + method_wall, params = params_wall, allow_redirects=False)
        req_wall.close
        sleep(0.5)
        df = pd.concat([df, pd.DataFrame(req_wall.json()['response']['items'])])

    return df.reset_index()

def currentDate():
    return pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def mainColumns(func) -> pd.DataFrame:
    def inner(*args, **kwargs):
        result = func(*args, **kwargs)
        return result[['id','owner_id','text','date','attachments']]
    return inner

@mainColumns
def requiredData(df) -> pd.DataFrame:
    return df.loc[(df['carousel_offset'] != 0.0)&(df['marked_as_ads'] != 1)&(df['attachments'] != None)]


def getdicts(func):
    def get0(df):
        result = func(df)
        try:
            return result.get('sizes')
        except:
            return None        
    return get0

def gettingUrl(func):
    def inner(*args, **kwargs):
        result = func(*args, **kwargs)
        try:
            for x in result:
                if  x.get('type') == 'x':
                    return x['url']
        except:
            return None
    return inner
    
@gettingUrl
@getdicts
def unpaker(df):
    try:
        return df[0].get('photo')
    except:
        return None

def changetype(func) -> pd.DataFrame:
    def inner(df):
        result = func(df)
        new_df = result.rename(columns = {'id':'content_id', 'date':'public_date'})
        return new_df.astype({'owner_id':int,'public_date':str, 'save_date':str})
    return inner

@changetype
def properformat(df) -> pd.DataFrame:
    new_df = df.dropna().reset_index().drop(columns = ['index','attachments'])
    new_df['owner_id'] = new_df['owner_id'] *-1
    return new_df

def dateStamper(arr):
    return pd.to_datetime(datetime.utcfromtimestamp(arr).strftime('%Y-%m-%d %H:%M:%S'))


def takeNewestPosts(sql_db, cur_db) -> pd.DataFrame:
    temp_df = pd.DataFrame()
    for x in cur_db['content_id'].to_list():
        if x not in sql_db:
            temp_df = pd.concat([temp_df, cur_db.loc[cur_db['content_id'] == x]])
    return temp_df

connect_db = DataBase_Con(db_host,db_user,  db_password,  db_db_name)
connect_db.connection_on()

main_list = connect_db.getIndexesFromMainDB()
df_from_vk = getVkJson(main_list)

frame_main_df = requiredData(df_from_vk)[:]

frame_main_df['date'] = frame_main_df['date'].apply(dateStamper)
frame_main_df['save_date'] = currentDate()
frame_main_df['url'] = frame_main_df['attachments'].apply(unpaker)
data_to_send = properformat(frame_main_df)[:]

temp_df = connect_db.GetIDPostsSQL()
try:
    new_df = takeNewestPosts(temp_df, data_to_send)

    for_content_db = new_df[['content_id', 'owner_id', 'public_date', 'save_date']]
    for_post_db = new_df[[ 'url', 'text','content_id']]
    for_content_db = for_content_db.astype({'public_date':str, 'save_date':str})
    connect_db.sendDataContentSQL(for_content_db)
    connect_db.sendDataPostsSQL(for_post_db)
    print("[INFO] Databases have been updated")
except Exception as _ex:
    print("[INFO] You don't have a new data", _ex)
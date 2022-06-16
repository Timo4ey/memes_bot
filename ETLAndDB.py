import requests
import pandas as pd
from time import sleep
from datetime import datetime
from config import db_db_name, db_host, db_password, db_user, vk_access_token

def getVkJson(list_of_indexes) -> pd.DataFrame:
    """Getting posts from vk"""
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
        print(req_wall.json())
        df = pd.concat([df, pd.DataFrame(req_wall.json()['response']['items'])])

    return df.reset_index()

def currentDate():
    """Getting current date"""
    return pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def mainColumns(func) -> pd.DataFrame:
    """Leave only working columns"""
    def inner(*args, **kwargs):
        result = func(*args, **kwargs)
        return result[['id','owner_id','text','date','attachments']]
    return inner

@mainColumns
def requiredData(df) -> pd.DataFrame:
    """Getting rid of from carousels and ads"""
    return df.loc[(df['carousel_offset'] != 0.0)&(df['marked_as_ads'] != 1)&(df['attachments'] != None)]


def getdicts(func):
    """Getting dicts through a key 'sizes"""
    def get0(df):
        result = func(df)
        try:
            return result.get('sizes')
        except:
            return None        
    return get0

def gettingUrl(func):
    """Getting url if a key 'type' == 'x' (x-means size)"""
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
    """Getting dicts through a key photo"""
    try:
        return df[0].get('photo')
    except:
        return None

def changetype(func) -> pd.DataFrame:
    """Changing columns types and renaming some columns"""
    def inner(df):
        result = func(df)
        new_df = result.rename(columns = {'id':'content_id', 'date':'public_date'})
        return new_df.astype({'owner_id':int,'public_date':str, 'save_date':str})
    return inner

@changetype
def properformat(df) -> pd.DataFrame:
    """Dropping raws with None format"""
    new_df = df.dropna().reset_index().drop(columns = ['index','attachments'])
    new_df['owner_id'] = new_df['owner_id'] *-1
    return new_df

def dateStamper(arr):
    """Getting datetime from code"""
    return pd.to_datetime(datetime.utcfromtimestamp(arr).strftime('%Y-%m-%d %H:%M:%S'))


def takeNewestPosts(sql_db, cur_db) -> pd.DataFrame:
    """Getting posts that have not had been downloaded yet"""
    temp_df = pd.DataFrame()
    for x in cur_db['content_id'].to_list():
        if x not in sql_db:
            temp_df = pd.concat([temp_df, cur_db.loc[cur_db['content_id'] == x]])
    return temp_df
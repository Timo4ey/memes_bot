import json
import requests
import psycopg2
from psycopg2.extensions import AsIs
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
import os

vk_token = {}

with open('D:/Job/WorkSpace/MemesDocker/vk_token.json', 'r') as f:
    vk_token = dict(json.load(f))

def getVkJson(list_of_indexes) -> pd.DataFrame:
    df = pd.DataFrame()
    for x in list_of_indexes:
        url = 'https://api.vk.com/method/'
        method_wall = 'wall.get'
        v = '5.131'
        params_wall = {
        'owner_id':x[0]*-1,
        'access_token':vk_token['access_token'],
        'count': 30,
        'v':v
    }
        req_wall = requests.get(url + method_wall, params = params_wall, allow_redirects=False)
        req_wall.close
        sleep(0.5)
        df = pd.concat([df, pd.DataFrame(req_wall.json()['response']['items'])])

    return df.reset_index()


def dateStamper(arr):
    return pd.to_datetime(datetime.utcfromtimestamp(arr).strftime('%Y-%m-%d %H:%M:%S'))

def currentDate():
    return pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def changeSignOwnerID(arr):
    return arr *-1


def getPhotoMetaData(arr) -> dict:
    if 'photo' in arr[0].values():
        return arr[0]['photo']


def getAttachments(arr) -> dict:
    if 'photo' in arr[0].values():
        return arr[0]['photo']['sizes']

def getPhotoID(arr) -> dict:
    if 'post_id' in arr.keys() :
        return arr['post_id']


def getSizes(arr) -> pd.DataFrame:
    temp_list = []
    for ind in range(len(arr)):
        for x in arr['meta_photo'][ind]['sizes']:
            if 'x' in x.values():
                temp_list.append( x['url'])

    return pd.DataFrame(temp_list, columns=['url'])


def convert_asis(arr):
    return AsIs(arr)


def dateSTAMP(arr):
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def takeNewestPosts(sql_db, cur_db) -> pd.DataFrame:
    temp_df = pd.DataFrame()
    for x in cur_db['content_id'].to_list():
        if x not in sql_db:
            temp_df = pd.concat([temp_df, cur_db.loc[cur_db['content_id'] == x]])
    return temp_df


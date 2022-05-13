from __future__ import print_function
import pandas as pd
from config import db_db_name, db_host, db_password, db_user


#--------MY moduls
from ETLAndDB import getVkJson # get data from VK
from ETLAndDB import dateStamper, changeSignOwnerID,getPhotoMetaData,getAttachments,getPhotoID, getSizes, convert_asis, dateSTAMP, takeNewestPosts, currentDate
from sqldb import DataBase_Con, GetDataPostsSQL, sendDataContentSQL, sendDataPostsSQL,getIndexesFromMainDB


connect_db = DataBase_Con(db_host,db_user,  db_password,  db_db_name)
connect_db.connection_on()
main_list = connect_db.getIndexesFromMainDB()
df_from_vk = getVkJson(main_list)


# Get rid of from 
# posts with many photo
# marketing adds
# empty rows

only_posts = df_from_vk.loc[(df_from_vk['carousel_offset'] != 0.0)&(df_from_vk['marked_as_ads'] != 1)&(df_from_vk['attachments'] != None)]


### leave only necessary
frame_main_df = only_posts[['id','owner_id','text','date','attachments']]

# delete all rows with None
frame_main_df= frame_main_df.dropna()


# change minus to plus
frame_main_df['owner_id'] = frame_main_df['owner_id'].apply(changeSignOwnerID)

# dump index that we could use it for function without mistakes
frame_main_df = frame_main_df.reset_index().drop(columns = 'index')


# get_meta date
frame_main_df['meta_photo'] = frame_main_df['attachments'].apply(getPhotoMetaData)

# delete all rows with empty
frame_main_df = frame_main_df.dropna()

#getting post ids 
frame_main_df['post_id'] = frame_main_df['meta_photo'].apply(getPhotoID)


frame_main_df = frame_main_df.dropna()

# dump index that we could use it for function without mistakes
frame_main_df = frame_main_df.reset_index().drop(columns = 'index')

frame_main_df['url'] = getSizes(frame_main_df)

# dump index that we could C:\Users\Тимофей\WorkSpace\Memes_job\main.pyuse it for function without mistakes
frame_main_df = frame_main_df.reset_index().drop(columns = 'index')


frame_main_df = frame_main_df.astype({'post_id':int})


frame_main_df = frame_main_df.rename(columns = {'id':'content_id'})

frame_main_df['public_date'] = frame_main_df['date'].apply(dateStamper)

save_date = pd.Series(currentDate(), index=range(len(frame_main_df)))
frame_main_df.loc[:, 'save_date'] = save_date


temp_df = connect_db.GetIDPostsSQL()
try:
    new_df = takeNewestPosts(temp_df, frame_main_df)

    for_content_db = new_df[['content_id', 'owner_id', 'public_date', 'save_date']]
    for_post_db = new_df[[ 'url', 'text','content_id']]
    for_content_db = for_content_db.astype({'public_date':str, 'save_date':str})
    connect_db.sendDataContentSQL(for_content_db)
    connect_db.sendDataPostsSQL(for_post_db)
    print("[INFO] Databases have been updated")
except Exception as _ex:
    print("[INFO] You don't have a new data", _ex)


print('The end')
















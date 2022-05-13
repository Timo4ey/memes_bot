from config import db_db_name, db_host, db_password, db_user
from sqldb import DataBase_Con
from ETLAndDB import (getVkJson, currentDate, requiredData,unpaker, 
                    properformat, dateStamper, takeNewestPosts)

# make connection with db
connect_db = DataBase_Con(db_host,db_user,  db_password,  db_db_name)
connect_db.connection_on()

# getting data from vk and make table from json
main_list = connect_db.getIndexesFromMainDB()
df_from_vk = getVkJson(main_list)

# leaving only posts
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
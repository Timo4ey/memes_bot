import os 

from dotenv import load_dotenv
load_dotenv()
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
db_db_name = os.getenv('db_db_name')
vk_access_token = os.getenv('vk_access_token')
vk_expires_in = os.getenv('vk_expires_in')
vk_user_id = os.getenv('vk_user_id')
tg_access_token = os.getenv('tg_access_token')
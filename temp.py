import logging
import datetime
import pandas
import time
import psycopg2
import requests
import json
import telegram
import os

print(pandas.__version__)
print(psycopg2.__version__)
print(requests.__version__)
print(json.__version__)
print(telegram.__version__)

print([x for x in os.listdir(os.getcwd()) if x.find('vk') > -1][0])
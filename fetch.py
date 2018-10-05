#!/usr/bin/env python3
import psycopg2
import sys
import pandas as pd
import pickle
import logging
import datetime
import time
from dateutil.parser import parse
from config import *

logging.basicConfig(filename=LOGFILE, level=logging.INFO, filemode='w')

with open(CREDENTIALS, 'rb') as db_creds:
  credentials_dict = pickle.load(db_creds)
ip = credentials_dict['ip']
dbname = credentials_dict['dbname']
pw = credentials_dict['pw']
user = credentials_dict['user']
port = credentials_dict['port']

with open(QUERY, 'r') as query_file:
  query_sans_where = query_file.read()
last_query_time = parse('Jan 01 2018') # datetime.datetime.utcnow()
dataframes = {}
last_transaction_time = {}
last_transaction_id = {}
first_time = True
counter = {'mf': 0, 'cr': 0, 'mx': 0, 'mlt': 0, 'jp': 0, 'vr': 0, 'all': 0}

while True:
  time_now = datetime.datetime.utcnow()
  if first_time:
    most_recent_midnight = time_now.replace(hour=0, minute=0, second=0, microsecond=0)
    previous_midnight = most_recent_midnight - datetime.timedelta(days=1)
    first_time = False
  elif time_now - most_recent_midnight >= datetime.timedelta(days=1):
    #counter = 0
    df = pd.concat(dataframes.values()).to_csv('../yesterday.csv', index=False)
    time.sleep(60)
    most_recent_midnight = time_now.replace(hour=0, minute=0, second=0, microsecond=0)
    previous_midnight = most_recent_midnight - datetime.timedelta(days=1)
  if (time_now-last_query_time).total_seconds() < QUERY_EVERY_N_SECONDS:
    time.sleep(QUERY_EVERY_N_SECONDS)
  else:
    for broker in port.keys():
      if broker not in dataframes.keys():
        dataframes[broker] = pd.DataFrame()
      if broker not in last_transaction_time.keys():
        last_transaction_time[broker] = most_recent_midnight
        query = query_sans_where+" where (purchase_time >= '"+ last_transaction_time[broker].replace(microsecond=0).isoformat()+"')"
      else:
        query = query_sans_where +  " where (b.id > " + str(last_transaction_id[broker]) + ")"
      db = psycopg2.connect(host=ip, user=user, password=pw, dbname=dbname, port=port[broker])
      con = db.cursor()
      # query = query_sans_where +  " where (id > " + last_transaction_id[broker] + ") limit 10"
      # query2 = query_sans_where+" where (purchase_time >= '"+ last_transaction_time[broker].replace(microsecond=0).isoformat() +"'::TIMESTAMP"+') '
      con.execute(query)
      logging.info('-'*10)
      logging.info('fetching data for: '+broker)
      _df = pd.DataFrame(data=con.fetchall(), columns=COL_NAMES)
      if len(_df) > 0:
        last_transaction_time[broker] = _df['purchase_time'].max()
        last_transaction_id[broker] = _df['id'].max()
        dataframes[broker] = pd.concat([dataframes[broker], _df])
        logging.info('for '+ broker +': num contracts since last query is '+str(len(_df)))
      last_query_time = datetime.datetime.utcnow()
      print(broker+' data fetched at:', last_query_time)
    df = pd.concat(dataframes.values())
    if len(df) >= MIN_RECORDS:
      counter['all'] += 1
      for broker in dataframes.keys():
        if len(dataframes[broker]) > 0:
          logging.info('writing CSV for: '+broker)
          logging.info('total rows for '+broker+' are: '+str(len(dataframes[broker]))+', with a last recorded purchase at '+ last_transaction_time[broker].isoformat()) #.tail())
          dataframes[broker].to_csv('../'+broker+'_today_'+str(counter[broker]).zfill(4)+'.csv', index=False)
          counter[broker] += 1
          dataframes[broker] = pd.DataFrame()
      df.to_csv('../all_today_'+str(counter["all"]).zfill(4)+'.csv', index=False)
      logging.info('total rows for ALL brokers are: '+str(len(df)))
      with open('current_count', 'w') as file:
        file.write(str(counter)+'\n')
      logging.info('done writing all!')


#!/usr/bin/env python
# coding: utf-8

#  This piece of software deplys the number of payment per day the day after migration


import os
from pymongo import MongoClient
import pymongo
import pandas as pd
from datetime import datetime
from datetime import timedelta
import numpy as np
import csv
from bson.objectid import ObjectId
import bson.objectid





string_product=os.environ.get('string_product','localhost:27017')
db_product=os.environ.get('prod_env_db','local')
read_preferences='secondaryPreferred'
myclient_con= pymongo.MongoClient(string_product,readPreference=read_preferences)
mydb_conn = myclient_con[db_product]

myclient_con= pymongo.MongoClient(string_product,readPreference=read_preferences)
mydb_conn = myclient_con[db_product]

today=datetime.now()

endDay=today+pd.Timedelta(days=30)

usersCol = mydb_conn["users"]
def users_to_frames(iterator, chunk_size: int):
  """Turn an iterator into multiple small pandas.DataFrame
  This is a balance between memory and efficiency
  """
  records = []
  frames = []
  for i, record in enumerate(iterator):
    records.append(record)
    if i % chunk_size == chunk_size - 1:
      frames.append(pd.DataFrame(records))
      records = []
  if records:
    frames.append(pd.DataFrame(records))
  return pd.concat(frames) if frames else pd.DataFrame()
  
users_pay = users_to_frames(usersCol.find({'fecha_fin':{'$gte':today,'$lte':endDay}},{'email':1,'fecha_fin':1}), 10000)

users_pay['dia']=users_pay['fecha_fin'].dt.day
users_pay['diff']=users_pay['dia']-today.day

def adjust_dispatch(df_line):
    if df_line['diff'] < 0:
        return df_line['diff'] + 30
    else:
        return df_line['diff']         

users_pay['dispatch_working_days'] = users_pay.apply(adjust_dispatch, axis=1)
users_pay['dispatch_working_days'].unique()
users_pay2 = users_pay.groupby('dia')['email'].count()
users_pay2






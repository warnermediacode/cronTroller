#!/usr/bin/env python
# coding: utf-8

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


users_pay2 = users_pay.groupby('dia')['email'].count().reset_index()


users_pay2.rename(columns = {'dia':'PayDay','email':'total'}, inplace = True)


users_pay2.info()


# PayDay: represents the payment day after migration, where 1 represents the same day of migration
# total: number of user to be charged this day

users_pay2.plot(kind='bar', title='Dias de pago');

# Inactive accounts with transactions count
# (https://turner.slack.com/archives/C03MPUF41QT/p1657303558503909)
# It's the number of accounts with (activeSubscriber=False AND which has historic transactions).  We're sending inactive people with transactions to Vindicia in case a refund would need to be given against an old transaction.


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
  
users_total = users_to_frames(usersCol.find({},{'email':1,'fecha_fin':1,'activeSubscriber':1,'paidDirect':1,'paidSubscriber':1}), 10000)



# ##### Active accounts count
active_users=users_total.loc[users_total['activeSubscriber']==True]
aac=active_users['_id'].count()


# ##### Unactive accounts count
usersGot=users_total.loc[users_total['activeSubscriber']==False]
uac=usersGot['_id'].count()
usersGot['_id']=usersGot['_id'].apply(str)
user_busList=usersGot['_id'].tolist()


pagosCol = mydb_conn["pagos"]
def CuponV2_Pagos_to_dataframes(iterator, chunk_size: int):
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
  
pagosTotal = CuponV2_Pagos_to_dataframes(pagosCol.find({},{'createdAt':1,'user_id':1}), 1000)


pagosTotal['_id'].count()

# ##### Transactions count
tc=pagosTotal['_id'].count()


Unactives_transaccions=pagosTotal.loc[pagosTotal['user_id'].isin(user_busList)]
# ##### Inactive user's transactions count
# This result reflects the number of transactions for those users that are no actives. The count doesn't consider any history. 

iuatc=Unactives_transaccions['_id'].count()





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


# string_product='mongodb+srv://cdfprod_app:tZJ8IPs7A63PWkHb@cdfmaster-prod2.nhfqf.mongodb.net/cdfMaster_prod'
# db_product='cdfMaster_prod'

myclient_con= pymongo.MongoClient(string_product,readPreference=read_preferences)
mydb_conn = myclient_con[db_product]


kitch_off=datetime(2019,1,1,0,0,0)
InitDay=datetime(2017,1,1,0,0,0)
hoy=datetime.now()
today=datetime.now()


usersCol = mydb_conn["users"]
def usersCol_to_dataframes(iterator, chunk_size: int):
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
  
usersGot = usersCol_to_dataframes(usersCol.find({},{'_id':1,'fecha_fin':1}), 10000)
usersGot2=usersGot[~(usersGot['fecha_fin'].isnull())]
usersGot2['fecha_fin']=usersGot2['fecha_fin'].apply(str).str[:19]
usersGot2['fecha_fin'] = pd.to_datetime(usersGot2['fecha_fin'])+ pd.DateOffset(milliseconds=999)
users_Col = mydb_conn["users"]

for k, row in usersGot2.iterrows():
    variable=users_Col.update_one( 
{"_id":ObjectId(row['_id'])}, 
{
    "$set": {"fecha_fin":row['fecha_fin']
                                            
}
})





#!/usr/bin/env python
# coding: utf-8

# ### Double promo flag setter
# This script set a flag at user allowing identify those double promo users.

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
kitch_off=datetime(2019,1,1,0,0,0)
InitDay=datetime(2017,1,1,0,0,0)
hoy=datetime.now()
today=datetime.now()



usersCol = mydb_conn["users_snapshot"]
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
  
usersGot = usersCol_to_dataframes(usersCol.find({'activeSubscriber':True},{'_id':1,'estado_token':1,'pasarela':1,'createdAt':1,'material':1,'activeSubscriber':1,'paidDirect':1,'paidSubscriber':1,'fecha_fin':1,'_acuerdo':1}), 10000)


usersGot['_acuerdo']=usersGot['_acuerdo'].apply(str)
buscList=usersGot['_acuerdo'].tolist()
promosCol = mydb_conn["promociones_snapshot"]
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
  
enPromocion= CuponV2_Pagos_to_dataframes(promosCol.find({'idAcuerdo': {'$in':buscList}},{'_user':1,'createdAt':1,'idProductoOriginal':1,'idProductoPromocion':1,'fechaFin':1,'idAcuerdo':1}), 10000)
enPromocion_Currently=enPromocion[enPromocion['fechaFin']>today]


for k, row in enPromocion_Currently.iterrows():
    variable=usersCol.update_one( 
{"_id":ObjectId(row['_user'])}, 
{
    "$set": {"originProduct":row['idProductoOriginal'],
             "end_coverDate":row['fechaFin']
                                     
}
})












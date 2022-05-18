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
kitch_off=datetime(2019,1,1,0,0,0)
InitDay=datetime(2017,1,1,0,0,0)
hoy=datetime.now()


ayer=datetime.now()-timedelta(hours=24)
ayerPayCriteria=ayer.replace( hour=23,minute=59,second=59)


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
  
usersGot = usersCol_to_dataframes(usersCol.find({},{'_id':1,'_acuerdo':1,'estado_token':1,'pasarela':1,'createdAt':1,'material':1}), 10000)


# # ////ACTIVE_SUBSCRIBER ////

usersGot2=usersGot.loc[~(usersGot['_id'].isnull())]
usersGot2['_id']=usersGot2['_id'].apply(str)


fullAccess=usersGot2.loc[(usersGot2['estado_token'].isin(['completado','cancelado']))]
withoutAccess=usersGot2.loc[~(usersGot2['_id'].isin(fullAccess['_id']))]


for index, row in fullAccess.iterrows():
        variablerar2 =usersCol.update_one({"_id":ObjectId(row['_id'])},{ "$set": { "activeSubscriber":True} })


for index, row in withoutAccess.iterrows():
        variablerar2 =usersCol.update_one({"_id":ObjectId(row['_id'])},{ "$set": { "activeSubscriber":False} })





# ### Getting Information from DB

mycol_transAsig = mydb_conn["transbankfinalizaregistros"]
def transAsigframes(iterator, chunk_size: int):
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
  
transbank = transAsigframes(mycol_transAsig.find({},{'_id':1,'createdAt':1,'user_id':1}), 10000)
transbank=transbank.rename(columns={ "createdAt":"START_DATE","_id":"key"})


mycol_paypalAsig= mydb_conn["acuerdospaypals"]
def paypalAsigframes(iterator, chunk_size: int):
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
  
paypal = paypalAsigframes(mycol_paypalAsig.find({},{'_id':1,'createdAt':1,'user_id':1}), 10000)
paypal=paypal.rename(columns={ "createdAt":"START_DATE","_id":"key"})


mycol_cupAsig= mydb_conn["cuponesasignados"]
def cupAsigframes(iterator, chunk_size: int):
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
cuponesasignados = cupAsigframes(mycol_cupAsig.find({},{'createdAt':1,'user_id_asignado':1,"cupon_id":1}), 10000)
cuponesasignados=cuponesasignados.rename(columns={ "createdAt":"START_DATE","cupon_id":"key"})


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
  
pagos_total = CuponV2_Pagos_to_dataframes(pagosCol.find({},{'user_id':1,'producto_id':1,'monto':1,'createdAt':1,'fecha_fin':1,'acuerdo_id':1,'pasarela':1}), 10000)


# transbanks

transbank['key']=transbank['key'].apply(str)
pagos_total['acuerdo_id']=pagos_total['acuerdo_id'].apply(str)
pagos_transb=pagos_total.loc[pagos_total['acuerdo_id'].isin(transbank['key'])]


ultimoPagoTransbank=pagos_transb.groupby(['user_id']).agg({'fecha_fin': [np.max]}).unstack().reset_index().rename_axis(None, axis=1)
ultimoPagoTransbank['fecha_fin_pago']=ultimoPagoTransbank[0]
prelTransbank=ultimoPagoTransbank.drop(['level_0', 'level_1', 0],axis=1)


pagoActivo_transbank=prelTransbank.loc[prelTransbank['fecha_fin_pago']>ayerPayCriteria]


# paypals

paypal['key']=paypal['key'].apply(str)
pagos_paypals=pagos_total.loc[pagos_total['acuerdo_id'].isin(paypal['key'])]


ultimoPagoPaypal=pagos_paypals.groupby(['user_id']).agg({'fecha_fin': [np.max]}).unstack().reset_index().rename_axis(None, axis=1)
ultimoPagoPaypal['fecha_fin_pago']=ultimoPagoPaypal[0]
prelPaypal=ultimoPagoPaypal.drop(['level_0', 'level_1', 0],axis=1)


pagoActivo_paypal=prelPaypal.loc[prelPaypal['fecha_fin_pago']>ayerPayCriteria]


# cupones

cuponesasignados['key']=cuponesasignados['key'].apply(str)
pagos_cupones=pagos_total.loc[pagos_total['acuerdo_id'].isin(cuponesasignados['key'])]


ultimoPagoCupones=pagos_cupones.groupby(['user_id']).agg({'fecha_fin': [np.max]}).unstack().reset_index().rename_axis(None, axis=1)
ultimoPagoCupones['fecha_fin_pago']=ultimoPagoCupones[0]
prelCupones=ultimoPagoCupones.drop(['level_0', 'level_1', 0],axis=1)


pagoActivo_cupones=prelCupones.loc[prelCupones['fecha_fin_pago']>ayerPayCriteria]


pagoActivo_direct = [pagoActivo_transbank, pagoActivo_paypal]
paidUser=pd.concat(pagoActivo_direct)


paidDirect=usersGot2.loc[(usersGot2['_id'].isin(paidUser['user_id']))]
NopaidSubscriber=usersGot2.loc[~(usersGot2['_id'].isin(paidUser['user_id']))]


usersCol = mydb_conn["users"]
for index, row in paidDirect.iterrows():
    variablerar2 =usersCol.update_many({"_id":ObjectId(row['_id'])},{ "$set":{'paidDirect':True}})
usersCol = mydb_conn["users"]
for index, row in NopaidSubscriber.iterrows():
    variablerar2 =usersCol.update_many({"_id":ObjectId(row['_id'])},{ "$set":{'paidDirect':False}})


ultimPagoGeneral = [pagoActivo_transbank, pagoActivo_paypal,pagoActivo_cupones]
LastPaid=pd.concat(ultimPagoGeneral)


pagoActivo=LastPaid.loc[LastPaid['fecha_fin_pago']>ayerPayCriteria]


pagoActivo['user_id']=pagoActivo['user_id'].apply(str)
usersGot2['_id']=usersGot2['_id'].apply(str)


usersGot2['_id']=usersGot2['_id'].apply(str)


paidSubscriber=usersGot2.loc[(usersGot2['_id'].isin(pagoActivo['user_id']))]
unpaidSubscriber=usersGot2.loc[~(usersGot2['_id'].isin(pagoActivo['user_id']))]


usersCol = mydb_conn["users"]
for index, row in paidSubscriber.iterrows():
    variablerar2 =usersCol.update_many({"_id":ObjectId(row['_id'])},{ "$set":{'paidSubscriber':True}})
for index, row in unpaidSubscriber.iterrows():
    variablerar2 =usersCol.update_many({"_id":ObjectId(row['_id'])},{"$set":{'paidSubscriber':False}})





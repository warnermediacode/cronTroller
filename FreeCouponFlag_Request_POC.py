#!/usr/bin/env python
# coding: utf-8

# ##### Vindicia teams request
# 
# This script finds users who have a free coupon that actually gives access to the content and assigns the category description of this coupon by adding the "coupon_category" field along with the day of the assignment.

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
import requests
import json 
from pandas.io.json import json_normalize


#getting some env var
string_product=os.environ.get('string_product','localhost:27017')
db_product=os.environ.get('prod_env_db','local')
read_preferences='secondaryPreferred'
API_Login=os.environ.get('prod_env_API_Login','local')
userAuth=os.environ.get('prod_env_user','local')
pass_userAuth=os.environ.get('prod_env_past_user','local')
prod_env_API_Login2=os.environ.get('prod_env_API_Login2','local')


# ### Obteniendo las categorias de cupones del Redis!!!

MensToken = requests.post(API_Login,
  headers={
    'Content-Type': 'application/json'
  },
  json={
      "email": userAuth,
      "password": pass_userAuth,
      "appId": "ecdf-mobile"
  }
)


MensToken_token=json.loads(MensToken.text)
MensToken_token2=pd.json_normalize(MensToken_token)
MensToken_token2['token'] = MensToken_token2['token'].map(str)
token=MensToken_token2.token.astype(str)
token=MensToken_token2.values[0]
token2=token[1]
token_beared = token2[:0] + 'Bearer '+token2[:]


response2 = requests.get(prod_env_API_Login2,,
  headers={
    'Content-Type': 'application/json',
    'Authorization': token_beared
  }
)
data = json.loads(response2.text) # carga el string como json
categorias = pd.json_normalize(data=data['message']) # Desenvaina el json embebido


categorias = pd.json_normalize(data=data['message'])
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
  
usersGot = usersCol_to_dataframes(usersCol.find({'paidSubscriber':False,'activeSubscriber':True},{'_id':1,'estado_token':1,'pasarela':1,'createdAt':1,'material':1,'activeSubscriber':1,'paidDirect':1,'paidSubscriber':1,'coupon_category':1,'fecha_fin':1,'_acuerdo':1}), 10000)
FreeCoupons=usersGot.loc[~(usersGot['coupon_category'].isnull())]
FreeCoupons.groupby(['pasarela'])['_id'].count()
theElapted=usersGot.loc[usersGot['coupon_category'].isnull()]


print(theElapted.groupby(['pasarela'])['_id'].count())





# ### Busca los últimos pagos de estos usuarios

usersGot['_id']=usersGot['_id'].apply(str)
acuList=usersGot['_id'].tolist()
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
  
pagos_usuariosFree= CuponV2_Pagos_to_dataframes(pagosCol.find({'user_id': {'$in':acuList}},{'user_id':1,'producto_id':1,'monto':1,'fecha_fin':1,'fecha_pago':1,'pasarela':1,'acuerdo_id':1,'transaccion_id':1}), 10000)
fechaPago1=pagos_usuariosFree.groupby(['user_id']).agg({'fecha_pago': [np.max]}).unstack().reset_index().rename_axis(None, axis=1)
fechaPago1['maxima']=fechaPago1[0]
upagos3=fechaPago1.drop(['level_0', 'level_1', 0],axis=1)


pagos_usuariosFree2= pagos_usuariosFree.merge(upagos3, right_on='user_id',left_on='user_id', how="left", indicator=False)
lastPayment=pagos_usuariosFree2.loc[pagos_usuariosFree2['fecha_pago']==pagos_usuariosFree2['maxima']]
lastPayment.rename(columns = {'_id':'idUsers'}, inplace = True)
lastPayment2=lastPayment.drop([  'pasarela', 'monto',  'producto_id',
       'acuerdo_id',  'fecha_fin', 'maxima'],axis=1)


usersGot_ultimopago= usersGot.merge(lastPayment2, right_on='user_id',left_on='_id', how="left", indicator=False)
parabuscar=usersGot_ultimopago.loc[~(usersGot_ultimopago['transaccion_id'].isnull())]


parabuscar['transaccion_id']=parabuscar['transaccion_id'].apply(str)
parabuscar['use'] = parabuscar['transaccion_id'].map(lambda x: bson.objectid.ObjectId(x))
usersList=parabuscar['use'].tolist()
cupAsigCol = mydb_conn["cuponesasignados"]
def cupAsig_to_dataframes(iterator, chunk_size: int):
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
  
cupAsig = cupAsig_to_dataframes(cupAsigCol.find({'_id':{'$in':usersList}},{'user_id_asignado':1,'cupon_id':1}), 10000)
cupAsig.rename(columns = {'_id':'idCuponAsig'}, inplace = True)
cupAsig['idCuponAsig']=cupAsig['idCuponAsig'].apply(str)

usersGot_ultimopago2= parabuscar.merge(cupAsig, right_on='idCuponAsig',left_on='transaccion_id', how="left", indicator=False)
usersGot_ultimopago2.groupby(['paidDirect','estado_token','paidSubscriber'])['_id'].count()
porCuponAsig=usersGot_ultimopago2.loc[~(usersGot_ultimopago2['idCuponAsig'].isnull())]

sinCuponAsig=usersGot_ultimopago2.loc[(usersGot_ultimopago2['idCuponAsig'].isnull())]

sesgoFremium_freemium=sinCuponAsig.loc[(sinCuponAsig['pasarela']=='freemium')&(sinCuponAsig['estado_token']=='freemium')]

cupAsig['cupon_id']=cupAsig['cupon_id'].apply(str)
categorias['_id']=categorias['_id'].apply(str)

con_cupones= cupAsig.merge(categorias, right_on='_id',left_on='cupon_id', how="left", indicator=False)
con_cupones['user_id_asignado'].nunique()
breffReport=con_cupones.groupby(['descripcion'])['user_id_asignado'].count().reset_index()
con_cupones.groupby(['descripcion','categoria_id'])['user_id_asignado'].count()

# breffReport.to_csv('/home/tecnoboyer/Ahead_helper/RevisionManual/breffReport'+str(hoy)+'.csv', index=False)

CC_date= pd.to_datetime(today,utc=True)

usersCol = mydb_conn["users"]
for k, row in con_cupones.iterrows():
    variable=usersCol.update_one( 
{"_id":ObjectId(row['user_id_asignado'])}, 
{
    "$set": {"coupon_category":row['descripcion'],
             "cc_date":CC_date
}
})





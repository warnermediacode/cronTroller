
# coding: utf-8

# ##### This module adds some value to the payment (transaction) to establish whether it is free or paid. If the product payment does not appear in the payment collection then it is assumed to be a coupon assignment.

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
pagos= CuponV2_Pagos_to_dataframes(pagosCol.find({'forFree': {"$exists":False}},{'user_id':1,'producto_id':1,'monto':1,'fecha_fin':1,'fecha_pago':1,'pasarela':1,'transaccion_id':1}))


pagos.rename(columns = {'_id':'pago_id'}, inplace = True)
pagos2=pago
pagosConProduct= pagos2.loc[~(pagos2['producto_id'].isnull())]
defProducto=pd.read_csv(reference_path)
defProducto2=defProducto.filter(['_id','valorNacional'])
defProducto2['_id']=defProducto2['_id'].apply(str)

defProducto2.loc[defProducto2['valorNacional']==0,'forFree']=True
defProducto2.loc[defProducto2['valorNacional']!=0,'forFree']=False

pagosConProduct['producto_id']=pagosConProduct['producto_id'].apply(str)

pagosConProduct_C= pagosConProduct.merge(defProducto2, right_on='_id',left_on='producto_id', how="left", indicator=False)



# ####  handling it without producto_id


pagosSinProduct= pagos2.loc[pagos2['producto_id'].isnull()]


pagosSinProduct['transaccion_id']=pagosSinProduct['transaccion_id'].apply(str)
pagosSinProduct['use3'] = pagosSinProduct['transaccion_id'].map(lambda x: bson.objectid.ObjectId(x))
usersList=pagosSinProduct['use3'].tolist()

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
  
cupAsig2 = cupAsig_to_dataframes(cupAsigCol.find({'_id':{'$in':usersList}},{'user_id_asignado':1,'cupon_id':1,'codigo':1,'producto_id':1,'acuerdo_id':1,'fecha_activacion':1,'fecha_fin':1}), 10)
cupAsig2['producto_id']=cupAsig2['producto_id'].apply(str)
cupAsig_Producto= cupAsig2.merge(defProducto2, right_on='_id',left_on='producto_id', how="left", indicator=False)

cupAsig_Producto.rename(columns = {'_id_x':'cuponAsignado_id'}, inplace = True)

auxiliar=cupAsig_Producto.filter(['cuponAsignado_id','valorNacional','forFree'])
auxiliar['cuponAsignado_id']=auxiliar['cuponAsignado_id'].apply(str)
pagosSinProduct['transaccion_id']=pagosSinProduct['transaccion_id'].apply(str)

pagosSinProduct_C= pagosSinProduct.merge(auxiliar,left_on='transaccion_id', right_on='cuponAsignado_id', how="left", indicator=False)
pagosSinProduct_C2=pagosSinProduct_C.filter(['pago_id','forFree'])
pagosConProduct_C.columns
pagosConProduct_C2=pagosConProduct_C.filter(['pago_id','forFree'])

both=[pagosSinProduct_C2,pagosConProduct_C2]

pagos_exact = pd.concat(both)


pagosCol = mydb_conn["pagos"]
AlmacenModificados_acuerdo=[]
for index, row in pagos_exact.iterrows():
    variablerar2 =pagosCol.update_one({"_id":ObjectId(row['pago_id'])},{ "$set": { "forFree":row['forFree']} })
    AlmacenModificados_acuerdo.append(row['pago_id'])


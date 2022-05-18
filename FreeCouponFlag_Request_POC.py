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


response2 = requests.get(prod_env_API_Login2,
  headers={
    'Content-Type': 'application/json',
    'Authorization': token_beared
  }
)
data = json.loads(response2.text) # carga el string como json
categorias = pd.json_normalize(data=data['message']) # Desenvaina el json embebido


categorias = pd.json_normalize(data=data['message'])

categorias

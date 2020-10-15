# Imports
import json
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
import pandas as pd
from datetime import datetime, timedelta, date
import requests
import psycopg2

# RDS settings

RDS_USERNAME = 'localhost'
RDS_USER_PWD = 'password'
RDS_HOST = 'localhost'
RDS_DB_PORT = '8080'
RDS_DB_NAME = 'localhost'

def requestSSO():
    try:
      con = psycopg2.connect(host=RDS_HOST, dbname=RDS_DB_NAME, user=RDS_USERNAME, password=RDS_USER_PWD)
      sessions = json.loads(request.content)
      for session in sessions['sessions']:
        result.update(dict([
          ('userId', session['userId']),
          ('origin', session['origin']),   
          ('expiresAt', session['expiresAt']), 
          ('loggedInAt', session['loggedInAt']), 
          ('loggedOutAt', session['loggedOutAt']), 
          ('token', session['token']), 
          ('_id', session['_id']),              
      ]))
        cur = con.cursor()
        select = "select _id from sso_sessions where _id = '" + session['_id'] + "'"
        cur.execute(select)
        selectResult = cur.fetchall()
        if selectResult: # if the session already exists - update
          cur.execute(updateData(session))
          con.commit()
        else: # else - insert new data
          dfToSave = pd.DataFrame.from_dict(result, orient='index').transpose()
          dfToSave.to_sql('sso_sessions', engine, index=False, if_exists='append', method='multi', dtype={'expiresAt': DateTime(timezone=False), 'loggedInAt': DateTime(timezone=False), 'loggedOutAt': DateTime(timezone=False) })
    except Exception as e:
          result['msg'] = str(e)
    finally:
      con.close()     
      return result

def updateData(session):
  try:
    if session['loggedOutAt'] is None:
      update = 'update sso_sessions set "expiresAt" = ' + "'" + session['expiresAt'] + "', " + '"loggedOutAt" = null where _id = ' + "'" + session['_id'] + "'"
    else:
      update = 'update sso_sessions set "expiresAt" = ' + "'" + session['expiresAt'] + "', " + '"loggedOutAt" = ' + "'" + session['loggedOutAt'] + "' " + 'where _id = ' + "'" + session['_id'] + "'"
    print(update)
  except Exception as ef:
        print(str(ef))
  return update

last_day = date.today() - timedelta(1)
url = 'API_URL'
paramsLogin = {'loginStart': last_day}
paramsLogout = {'loginEnd': last_day}
api_token = 'TOKEN'
headers = {'Content-Type': 'application/json',
           'platform': api_token}
request = requests.get(url,params=paramsLogin, headers=headers)

POSTGRES = "postgresql://"+RDS_USERNAME+":" \
        + RDS_USER_PWD + "@" + RDS_HOST + ":" + RDS_DB_PORT + "/" + RDS_DB_NAME
engine = create_engine(POSTGRES, pool_pre_ping=True)

result = dict()

if request.status_code == 200:
  print(requestSSO())
else:
  print('Não foi possível conectar na api, status code: ' + str(request.status_code))

request = requests.get(url,params=paramsLogout, headers=headers)
result = dict()

if request.status_code == 200:
  print(requestSSO())
else:
  print('Não foi possível conectar na api, status code: ' + str(request.status_code))

import logging
import json
from datetime import datetime
from google.cloud import datastore

    
def put_model(project, namespace, onboard_period, return_period, defs, med_lifetime, mean_lifetime):
  client = datastore.Client(project = project)
  key = client.key('ChurnPredictor', '{namespace}_{onboard_period}_{return_period}'.format(namespace = namespace, 
  onboard_period = int(onboard_period), return_period = int(return_period)), namespace = namespace)
  task = datastore.Entity(key)
  model_time = check_model_exists(project, '{namespace}_{onboard_period}_{return_period}'.format(namespace = namespace, 
  onboard_period = int(onboard_period), return_period = int(return_period)), namespace)
  try:
    task.update({'created': model_time,
                 'modified': datetime.now(),
                 'onboard_period': int(onboard_period),
                 'return_period': int(return_period),
                 'def_values': json.dumps(defs),
                 'user_lifetime_median': int(med_lifetime),
                 'user_lifetime_mean': int(mean_lifetime)})
    client.put(task)
    logging.info('{namespace} datastore model put'.format(namespace = namespace))
    response = 200
  except:
    logging.exception('failed: {namespace} datastore model put'.format(namespace = namespace))
    response = 500
  finally:
    return response
    
    
def get_model(project, identifier, namespace):
  client = datastore.Client(project = project)
  key = client.key('ChurnPredictor', identifier, namespace = namespace)
  res = client.get(key)
  if not isinstance(res, type(None)):
    return json.dumps(res, default=str)
  else:
    logging.exception('failed: {namespace} datastore model fetch'.format(namespace = namespace))
    

def check_model_exists(project, identifier, namespace):
  client = datastore.Client(project = project)
  key = client.key('ChurnPredictor', identifier, namespace = namespace)
  res = client.get(key)
  if isinstance(res, type(None)):
    return datetime.now()
  else:
    return res.get('created', datetime.now())

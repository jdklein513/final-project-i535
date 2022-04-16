# load libraries needed for api calls
import pandas as pd
import json
import requests
from pandas.io import gbq
import pandas_gbq
import gcsfs
from sodapy import Socrata

'''
function to respond and validate any HTTP request
'''

def http_validation(request):
  request_json = request.get_json()
  
  if request.args:

    extract_load_api_data(url='data.cdc.gov', set_name='8xkx-amqh', data_filter="date_extract_d(date) = 1", project='upbeat-stratum-347321', dataset='cdc_vax_rates', table='cdc_vax_rates')
    return "Data pulled and loaded"
  
  elif request_json:
    
    extract_load_api_data(url='data.cdc.gov', set_name='8xkx-amqh', data_filter="date_extract_d(date) = 1", project='upbeat-stratum-347321', dataset='cdc_vax_rates', table='cdc_vax_rates')
    return "Data pulled and loaded"
  
  else:
    
    extract_load_api_data(url='data.cdc.gov', set_name='8xkx-amqh', data_filter="date_extract_d(date) = 1", project='upbeat-stratum-347321', dataset='cdc_vax_rates', table='cdc_vax_rates')
    return "Data pulled and loaded"

'''
function to extract entire dataset from government website and load to bigquery
'''
  
def extract_load_api_data(url, set_name, data_filter, project, dataset, table):
  
  # Unauthenticated client works with public data sets
  client = Socrata(url, None)

  # change the timeout variable to an arbitrarily large number of seconds
  client.timeout = 50

  # results returned as JSON from API / converted to Python list of
  # dictionaries by sodapy
  # set the limit to an arbitrarily large number to get all rows
  results = client.get(set_name, where=data_filter, limit=10000000)

  # convert returned data to pandas DataFrame
  df = pd.DataFrame.from_records(results)

  # write to BigQuery
  df.to_gbq(destination_table='{}.{}'.format(dataset, table), project_id=project, if_exists='replace')
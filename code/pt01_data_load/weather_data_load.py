# load libraries needed for api calls
import pandas as pd
import json
import requests
from pandas.io import gbq
import pandas_gbq
import gcsfs
import re
import bs4
from bs4 import BeautifulSoup

'''
function to respond and validate any HTTP request
'''

def http_validation(request):
  request_json = request.get_json()
  
  if request.args:
    extract_load_api_data(url='https://www.ncei.noaa.gov/pub/data/cirs/climdiv/', exp_name='climdiv-(.*)cy-', project='upbeat-stratum-347321', dataset='ncei_weather', table='ncei_weather')
    return 'Data pull and load complete'
  
  elif request_json:
    extract_load_api_data(url='https://www.ncei.noaa.gov/pub/data/cirs/climdiv/', exp_name='climdiv-(.*)cy-', project='upbeat-stratum-347321', dataset='ncei_weather', table='ncei_weather')
    return 'Data pull and load complete'
  
  else:
    extract_load_api_data(url='https://www.ncei.noaa.gov/pub/data/cirs/climdiv/', exp_name='climdiv-(.*)cy-', project='upbeat-stratum-347321', dataset='ncei_weather', table='ncei_weather')
    return 'Data pull and load complete'

'''
function to extract entire dataset from ncei website and load to bigquery
'''
  
def extract_load_api_data(url, exp_name, project, dataset, table):

  # get the filepaths for the data files
  files = get_url_paths(url, exp_name)

  # for each candidate file, extract the measure name, run the etl, and join to previous result
  for i, file_name in enumerate(files):
    
    # get the metric name from the file name
    feature = re.search(exp_name, file_name)
    feature = feature.group(1)
    
    # run the extract and clean function
    tmp_df = extract_transform(file=file_name, measure=feature)
    
    # if this is the first clean set set to df
    # else join the clean set to the previous clean set
    if i == 0:
      df = tmp_df
    else:
      df = df.merge(tmp_df, how='left', on=['date','fips']) # join to the next data frame by date, fips

  # write to BigQuery
  df.to_gbq(destination_table='{}.{}'.format(dataset, table), project_id=project, if_exists='replace')

'''
function to get the files from a website that match a pattern
'''

def get_url_paths(url, exp='', params={}):
    # get the url
    response = requests.get(url, params=params)
    
    # check to make sure api hit successful
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    
    # use the html parser to get the filenames
    soup = BeautifulSoup(response_text, 'html.parser')
    
    # get the file names which match the expression and do not include norm
    parent = [url + node.get('href') for node in soup.find_all('a') if (bool(not re.search('norm', node.get('href'))) & bool(re.search(exp, node.get('href'))))]
    
    # return list of files
    return parent

'''
function to extract files data from NCEI website, transform data, and return cleaned dataframe
'''

def extract_transform(file, measure):
  # read in the data table
  df = pd.read_table(file, header=None, delim_whitespace=True)

  # set the column names
  df.columns = ["code",
                "01-01", "02-01", "03-01",
                "04-01", "05-01", "06-01",
                "07-01", "08-01", "09-01",
                "10-01", "11-01", "12-01"]

  # create year feature
  df['year'] = df['code'].astype(str).str.strip().str[-4:].astype(int)

  # create fips feature
  df['fips'] = df['code'].astype(str).str.strip().str[:-6].str.zfill(5)

  # subset the right columns in data frame
  df = df[df['year'] >= 2019][["year", "fips",
                              "01-01", "02-01", "03-01",
                              "04-01", "05-01", "06-01",
                              "07-01", "08-01", "09-01",
                              "10-01", "11-01", "12-01"]]

  # unpivot the data
  df = df.melt(id_vars=['year','fips'], var_name='month', value_name=measure)

  # remove missing values
  df = df[~df[measure].isin([-9.99, -99.9, -9999.0])]

  # create month variable
  df['date'] = df['year'].astype(str) + "-" + df['month']

  # select the columns in the final data
  df = df[['date', 'fips', measure]]

  # return the clean data
  return(df)
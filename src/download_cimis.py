import getpass as gp
import glob
import os
import sys
import datetime
import numpy as np
import pandas as pd
import json
import requests
import matplotlib.pyplot as plt
from datetime import timedelta
from datetime import date
import re

# 46c39c6d-4d71-464c-a20f-a5c7d21bba8e
# 7f9d336c-13a9-42fa-96f0-d279581c5aa1

#station_ids  = [2, 6, 7, 12, 13, 15, 35, 39, 41, 43, 44, 47, 52, 64, 68, 70, 71, 75, 77, 78, 80, 83, 84, 87, 90, 91, 99, 103, 104, 105, 106, 107, 113, 114, 117, 124, 125, 126, 129, 131, 139, 140, 144, 146, 147, 150, 151, 152, 153, 157, 158, 160, 163, 165, 170, 171, 173, 174, 175, 178, 179, 181, 182, 184, 187, 191, 192, 193, 194, 195, 197, 199, 200, 202, 204, 206, 207, 208, 209, 210, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 235, 236, 237, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 256, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268]	
station_ids = [2]

def cimis_intake(start_date, end_date, directory_path):
	''' This function downloads weather station data from CIMIS '''
	cimis_api_url = "http://et.water.ca.gov/api"
	data_items = '''day-eto,day-precip,day-sol-rad-avg,day-vap-pres-avg,day-air-tmp-max,day-air-tmp-min,day-air-tmp-avg,day-rel-hum-max,day-rel-hum-min,day-rel-hum-avg,day-dew-pnt,day-wind-spd-avg,day-wind-run,day-soil-tmp-avg'''
	dir = "../"
	#this list of stations is all active ETo stations

	year_span = pd.to_datetime(end_date).year - pd.to_datetime(start_date).year
	print(year_span)

	api_key = gp.getpass("Enter your CIMIS API key: ")

	for id in station_ids:
	  rest_url = f'{cimis_api_url}/data?appKey={api_key}&targets={id}&startDate={start_date}&endDate={end_date}&dataItems={data_items}&unitOfMeasure=M'
	  print(rest_url)
	  res = requests.get(rest_url)
	  print(res.text)
	  print(res.status_code)
	  
	  response = json.loads(res.text)
	  payload = response['Data']['Providers'][0]['Records']
	  df = pd.json_normalize(payload)
	  df = df[df.columns.drop(list(df.filter(regex='Unit')))] #drops unnecessary column with unit of measure
	  print(df)
	  filename = f'{directory_path}station_id{id}_cimis_daily_raw.csv'
	  print(filename)
	  df.to_csv(filename, index=False)



yesterday = (pd.Timestamp.now() - timedelta(days=1)).date()
print(yesterday)

#cimis_intake('2025-01-01', yesterday, '../')

def get_latest_preexisting_date(filepath): #filepath
	#for id in station_ids:
  '''
	filename = "station_id187_cimis_daily_raw2019.csv"

	match = re.search(r'station_id(\d+)_.*?raw(\d+)\.csv', filename)
	if match:
	  station_id = match.group(1)
	  year = match.group(2)
	  print(f"Station ID: {station_id}, Year: {year}")
	else:
	  print("Pattern not found.")	'''

  for id in station_ids:
	  file = f'{filepath}/station{id}/station_id{id}_cimis_daily_raw_MASTER.csv'
	  df = pd.read_csv(file)
	  return (pd.to_datetime(df.iloc[-1]['Date']) + timedelta(days=1)).date()

filepath = '~/Documents/cimis/CIMIS/CIMIS_Project_Data/CIMIS_daily_raw_data'
start_date = get_latest_preexisting_date(filepath)
print(start_date)

#cimis_intake(start_date, yesterday, '../')

'''
Tues: use timedelta to determine if a date range is greater than 1750, and if not 
create a csv for it, else split into multiple reqs and concatenate the dataframes prior
to saving as csv
'''

date = pd.to_datetime('2016-01-01') + timedelta(days=1760)

def get_daterange_data(start_date, end_date, filepath):
	days_span = pd.to_datetime(end_date) - pd.to_datetime(start_date)
	if days_span > 1749:
		# split into two
		intermediate = (start_date + timedelta(days=1749)).date()
		print(intermediate)
	else:





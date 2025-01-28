import os
import requests
import json
import pandas as pd
import getpass as getpass
import numpy as np
from datetime import datetime, timedelta

# 46c39c6d-4d71-464c-a20f-a5c7d21bba8e
# 7f9d336c-13a9-42fa-96f0-d279581c5aa1

# Prompt user for API key
#api_key = gp.getpass("Enter your CIMIS API key: ")

def get_api_key():
  api_key = getpass.getpass("Enter your CIMIS API key: ")
  return api_key

def fix_col_names(df):
  '''corrects column names from CIMIS based on dictionary'''
  rename_dict = {'DayAirTmpMin.Value':'Tmin',
               'DayAirTmpMin.Qc':'Tmin_Qc',
               'DayAirTmpMax.Value':'Tmax',
               'DayAirTmpMax.Qc':'Tmax_Qc',
               'DayDewPnt.Value':'Tdew',
               'DayDewPnt.Qc':'Tdew_Qc',
               'DayAirTmpAvg.Value':'Tavg',
               'DayAirTmpAvg.Qc':'Tavg_Qc',
               'DayEto.Value':'ETo',
               'DayEto.Qc':'ETo_Qc',
               'DayRelHumMin.Value':'RHmin',
               'DayRelHumMin.Qc':'RHmin_Qc',
               'DayRelHumMax.Value':'RHmax',
               'DayRelHumMax.Qc':'RHmax_Qc',
               'DayRelHumAvg.Value':'RHavg',
               'DayRelHumAvg.Qc':'RHavg_Qc',
               'DayPrecip.Value':'Pr',
               'DayPrecip.Qc':'Pr_Qc',
               'DaySolRadAvg.Value':'Rs',
               'DaySolRadAvg.Qc':'Rs_Qc',
               'DayVapPresAvg.Value':'Ea',
               'DayVapPresAvg.Qc':'Ea_Qc',
               'DayWindSpdAvg.Value':'u2',
               'DayWindSpdAvg.Qc':'u2_Qc',
               'Julian':'Doy'}
  df.rename(columns=rename_dict, inplace=True)
  return df

# Function to split date ranges into chunks of 1750 days
def split_date_ranges(start_date, end_date, max_days=1750):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    date_ranges = []
    while start_date <= end_date:
        chunk_end = min(start_date + timedelta(days=max_days - 1), end_date)
        date_ranges.append((start_date.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        start_date = chunk_end + timedelta(days=1)
    return date_ranges

# Main function to query CIMIS API and save results
def fetch_cimis_data(station_ids, start_date, end_date, output_dir="cimis_data/"):
    api_key = getpass.getpass("Enter your CIMIS API key: ")
    cimis_api = "http://et.water.ca.gov/api"
    data_items = '''day-eto,day-precip,day-sol-rad-avg,day-vap-pres-avg,day-air-tmp-max,day-air-tmp-min,day-air-tmp-avg,day-rel-hum-max,day-rel-hum-min,day-rel-hum-avg,day-dew-pnt,day-wind-spd-avg,day-wind-run,day-soil-tmp-avg'''

    os.makedirs(output_dir, exist_ok=True)

    for station_id in station_ids:
        print(f"Processing station {station_id}...")
        station_dir = os.path.join(output_dir, f"station{station_id}")
        os.makedirs(station_dir, exist_ok=True)

        # Split date range into chunks of 1750 days
        date_ranges = split_date_ranges(start_date, end_date)
        all_data = []

        for start, end in date_ranges:
            print(f"Fetching data from {start} to {end}...")
            rest_url = (f"{cimis_api}/data?appKey={api_key}&targets={station_id}"
                        f"&startDate={start}&endDate={end}&dataItems={data_items}&unitOfMeasure=M")
            try:
                res = requests.get(rest_url)
                res.raise_for_status()  # Raise an exception for HTTP errors
                response = json.loads(res.text)

                payload = response.get('Data', {}).get('Providers', [{}])[0].get('Records', [])
                if not payload:
                    print(f"No data available for station {station_id} from {start} to {end}.")
                    continue

                df = pd.json_normalize(payload)
                df = df[df.columns.drop(list(df.filter(regex='Unit')))]  # Drop 'Unit' columns
                all_data.append(df)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for station {station_id} from {start} to {end}: {e}")
            except (KeyError, IndexError, ValueError) as e:
                print(f"Unexpected data structure for station {station_id} from {start} to {end}: {e}")

        # Concatenate all data for the station and save as MASTER file
        if all_data:
            master_df = pd.concat(all_data, ignore_index=True)
            master_df = fix_col_names(master_df)
            master_file = os.path.join(station_dir, f"station_id{station_id}_cimis_daily_MASTER.csv")
            master_df.to_csv(master_file, index=False)
            print(f"Saved MASTER file for station {station_id} at {master_file}")
        else:
            print(f"No data found for station {station_id} from {start_date} to {end_date}.")


#station_ids  = [2, 6, 7, 12, 13, 15, 35, 39, 41, 43, 44, 47, 52, 64, 68, 70, 71, 75, 77, 78, 80, 83, 84, 87, 90, 91, 99, 103, 104, 105, 106, 107, 113, 114, 117, 124, 125, 126, 129, 131, 139, 140, 144, 146, 147, 150, 151, 152, 153, 157, 158, 160, 163, 165, 170, 171, 173, 174, 175, 178, 179, 181, 182, 184, 187, 191, 192, 193, 194, 195, 197, 199, 200, 202, 204, 206, 207, 208, 209, 210, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 235, 236, 237, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 256, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268]  

#start_date = input("Enter start date in YYYY-MM-DD format: ")
#end_date = input("Enter end date in YYYY-MM-DD format: ")

#fetch_cimis_data(station_ids, start_date, end_date)

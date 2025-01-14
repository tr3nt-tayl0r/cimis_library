import os
import requests
import json
import pandas as pd
import getpass as gp
from datetime import datetime, timedelta

# 46c39c6d-4d71-464c-a20f-a5c7d21bba8e
# 7f9d336c-13a9-42fa-96f0-d279581c5aa1

# Prompt user for API key
api_key = gp.getpass("Enter your CIMIS API key: ")

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
def fetch_cimis_data(station_ids, start_date, end_date, data_items, output_dir="cimis_data/"):
    cimis_api = "http://et.water.ca.gov/api"
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
            res = requests.get(rest_url)
            if res.status_code != 200:
                print(f"Error fetching data for {station_id}: {res.text}")
                continue

            response = json.loads(res.text)
            try:
                payload = response['Data']['Providers'][0]['Records']
            except (KeyError, IndexError):
                print(f"No data available for {station_id} from {start} to {end}.")
                continue

            df = pd.json_normalize(payload)
            df = df[df.columns.drop(list(df.filter(regex='Unit')))]  # Drop 'Unit' columns
            all_data.append(df)

        # Concatenate all data for the station and save as MASTER file
        if all_data:
            master_df = pd.concat(all_data, ignore_index=True)
            master_file = os.path.join(station_dir, f"station_id{station_id}_cimis_daily_MASTER.csv")
            master_df.to_csv(master_file, index=False)
            print(f"Saved MASTER file for station {station_id} at {master_file}")
        else:
            print(f"No data found for station {station_id}.")


station_ids = [2]  
start_date = "2016-01-01"  
end_date = "2022-09-10"
data_items = '''day-eto,day-precip,day-sol-rad-avg,day-vap-pres-avg,day-air-tmp-max,day-air-tmp-min,day-air-tmp-avg,day-rel-hum-max,day-rel-hum-min,day-rel-hum-avg,day-dew-pnt,day-wind-spd-avg,day-wind-run,day-soil-tmp-avg'''

fetch_cimis_data(station_ids, start_date, end_date, data_items)

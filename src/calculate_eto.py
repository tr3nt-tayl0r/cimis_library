import pandas as pd
import os
import math
import subprocess
import sys

package_name = "refet"
subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
import refet

station_file = './stations/stations.csv'
st_df = pd.read_csv(station_file)

station_ids = st_df[(st_df['IsActive'] == True) & (st_df['IsEtoStation'] == True)]['StationNbr'].tolist()

units = {
  'tmin': 'C', 
  'tmax': 'C', 
  'tdew': 'C', 
  'rs': 'w/m2',
  'uz': 'm/s', 
  'lat': 'deg'
}

def calc_et_uncorr(row):
  eto_uncor = refet.Daily(
    tmin=row['Tmin'], tmax=row['Tmax'], tdew=row['Tdew'], rs=float(row['Rs']), uz=row['u2'],
    zw=2, elev=row['Elev'], lat=row['Lat'], doy=row['Doy'], method='asce',
    input_units=units).eto()
  return eto_uncor[0]

def calc_et_corr(row):
  eto_corr = refet.Daily(
    tmin=row['Tmin_corr'], tmax=row['Tmax_corr'], tdew=row['Tdew_corr'], rs=float(row['Rs']), uz=row['u2'],
    zw=2, elev=row['Elev'], lat=row['Lat'], doy=row['Doy'], method='asce',
    input_units=units).eto()
  return eto_corr[0]

for id in station_ids:
  file = f'./cimis_data/station{id}/station_id{id}_cimis_daily_MASTER.csv'
  if os.path.exists(file):
    df = pd.read_csv(file)

    df_id = st_df[st_df['StationNbr'] == id]

    elev = df_id['Elevation'].values[0]
    df.loc[:,'Elev'] = elev

    lat = df_id['Lat'].values[0]
    df.loc[:,'Lat'] = lat

    longtd = df_id['Long'].values[0]
    df.loc[:,'Long'] = longtd

    df['ETo_uncor'] = df.apply(calc_et_uncorr, axis=1)
    df['ETo_corr'] = df.apply(calc_et_corr, axis=1)
    df['ETo_CIMIS'] = df['ETo']

    df.to_csv(file, index=False)

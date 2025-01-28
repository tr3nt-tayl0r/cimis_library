import pandas as pd
import numpy as np
import re
import os
import pprint

st_dir = './stations/'
st_file = 'stations-meta.csv'
st_df = pd.read_csv(f'{st_dir}{st_file}')

station_ids  = [2, 6, 7, 12, 13, 15, 35, 39, 41, 43, 44, 47, 52, 64, 68, 70, 71, 75, 77, 78, 80, 83, 84, 87, 90, 91, 99, 103, 104, 105, 106, 107, 113, 114, 117, 124, 125, 126, 129, 131, 139, 140, 144, 146, 147, 150, 151, 152, 153, 157, 158, 160, 163, 165, 170, 171, 173, 174, 175, 178, 179, 181, 182, 184, 187, 191, 192, 193, 194, 195, 197, 199, 200, 202, 204, 206, 207, 208, 209, 210, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 235, 236, 237, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 256, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268]  

eto_flags = ['H', 'R']
pr_flags = ['H', 'R', 'S']
temp_flags = ['R', 'S', 'Y']
dew_flags = ['I', 'Q', 'Y']
solar_flags = ['H', 'R', 'S']

for id in station_ids:
  dir = f'./cimis_data/station{id}'
  with os.scandir(dir) as entries:
    for entry in entries:    
      file = f'{dir}/{entry.name}'
      df = pd.read_csv(f'{file}')

      df.loc[df['ETo_Qc'].isin(eto_flags), 'ETo'] = np.nan
      df.loc[df['Pr_Qc'].isin(pr_flags), 'Pr'] = np.nan
      df.loc[df['Rs_Qc'].isin(solar_flags), 'Rs'] = np.nan
      df.loc[df['Tmax_Qc'].isin(temp_flags), 'Tmax'] = np.nan
      df.loc[df['Tmin_Qc'].isin(temp_flags), 'Tmin'] = np.nan
      df.loc[df['Tdew_Qc'].isin(dew_flags), 'Tdew'] = np.nan

      df.to_csv(f'{file}', index=False)
      df['Date']=pd.to_datetime(df['Date'])
      df.set_index(pd.to_datetime(df.Date), inplace=True)

      df_mjjas = df[(df['Date'].dt.month == 5) | (df['Date'].dt.month == 6) | (df['Date'].dt.month == 7) | (df['Date'].dt.month == 8) | (df['Date'].dt.month == 9)]
      denom = df_mjjas.shape[0]

      eto_num = df_mjjas['ETo'].isna().sum()
      eto_frac = 1 - eto_num/denom
      st_df.loc[st_df['StationNbr'] == id, 'ETo_Qc_fraction'] = eto_frac

      pr_num = df_mjjas['Pr'].isna().sum()
      pr_frac = 1 - pr_num/denom
      st_df.loc[st_df['StationNbr'] == id, 'Pr_Qc_fraction'] = pr_frac

      rs_num = df_mjjas['Rs'].isna().sum()
      rs_frac = 1 - rs_num/denom
      st_df.loc[st_df['StationNbr'] == id, 'Rs_Qc_fraction'] = rs_frac

      tmax_num = df_mjjas['Tmax'].isna().sum()
      tmax_frac = 1 - tmax_num/denom
      st_df.loc[st_df['StationNbr'] == id, 'Tmax_Qc_fraction'] = tmax_frac

      tmin_num = df_mjjas['Tmin'].isna().sum()
      tmin_frac = 1 - tmin_num/denom
      st_df.loc[st_df['StationNbr'] == id, 'Tmin_Qc_fraction'] = tmin_frac

      tdew_num = df_mjjas['Tdew'].isna().sum()
      tdew_frac = 1 - tdew_num/denom
      st_df.loc[st_df['StationNbr'] == id, 'Tdew_Qc_fraction'] = tdew_frac

    print(st_df[['StationNbr','ETo_Qc_fraction', 'Pr_Qc_fraction', 'Rs_Qc_fraction', 'Tmax_Qc_fraction', 'Tmin_Qc_fraction', 'Tdew_Qc_fraction']])
    st_df.to_csv(f'{st_dir}{st_file}', index=False)

'''
dir = './cimis_data/'
anomalies = []
anom_dict = {}

for id in station_ids:
  file = f'{dir}station{id}/station_id{id}_cimis_daily_raw_MASTER.csv'
  df = pd.read_csv(file)
  df['Date'] = pd.to_datetime(df.Date)
  df.set_index(pd.to_datetime(df.Date),inplace=True)
  for year in range(2003,2025):
    df_yr = df[(df['Date'].dt.year == year)]
    if not df_yr.empty:
      dfAI = df_yr[['Pr','ETo']].dropna(axis=0)
      AI = dfAI['Pr'].mean()/dfAI['ETo'].mean()
      if not ((AI >= 0.0) & (AI <= 1.0)):
        datum = (id, year, np.round(AI,2))
        anomalies.append(datum)
        if id not in anom_dict:
          anom_dict[id] = [(year, np.round(AI,2))]
        else:
          years = anom_dict.get(id)
          years.append((year, np.round(AI,2)))
          anom_dict[id] = years
      else:
        continue
    else:
      continue

station_anom = anom_dict.keys()
print(anomalies)
print()
print(station_anom)
print(len(station_anom))

pprint.pprint(anom_dict)
print(len(anom_dict))

to_remove = []
for st in station_anom:
  anoms = anom_dict.get(st)
  if len(anoms) > 3:
    to_remove.append(st)

# removes stations that have greater than 3 anomalous AI yrs
station_ids = [st for st in station_ids if st not in to_remove]
print(to_remove)
print(station_ids)
print(len(station_ids))
'''
      


import pandas as pd
import numpy as np
import re
import os

station_ids  = [2, 6, 7, 12, 13, 15, 35, 39, 41, 43, 44, 47, 52, 64, 68, 70, 71, 75, 77, 78, 80, 83, 84, 87, 90, 91, 99, 103, 104, 105, 106, 107, 113, 114, 117, 124, 125, 126, 129, 131, 139, 140, 144, 146, 147, 150, 151, 152, 153, 157, 158, 160, 163, 165, 170, 171, 173, 174, 175, 178, 179, 181, 182, 184, 187, 191, 192, 193, 194, 195, 197, 199, 200, 202, 204, 206, 207, 208, 209, 210, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 235, 236, 237, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 256, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268]  

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

def corr_nref(df, tminNRef='Tmin', tdewNRef='Tdew', tmaxNRef='Tmax', bT=True):
    # Calculate dT
    df['dT'] = df[tminNRef] - df[tdewNRef]

    # Add bT column if bT is True
    if bT:
        df['bT'] = 0.3

    # Calculate AI
    dfAI = df[['Pr', 'ETo']].dropna(axis=0)
    AI = dfAI['Pr'].mean() / dfAI['ETo'].mean()
    print('\t\tAI = ' + str(np.round(AI, 2)))

    # Determine aT based on AI
    if AI < 0.05:
        aT = 5
    elif 0.05 <= AI < 0.2:
        aT = 2.5
    elif 0.2 <= AI < 0.5:
        aT = 1.5
    elif 0.5 <= AI < 0.65:
        aT = 0.5
    else:
        aT = 0

    df['aT'] = aT

    # Create correction columns
    df['Tmax_corr'] = df[tmaxNRef]
    df['Tmin_corr'] = df[tminNRef]
    df['Tdew_corr'] = df[tdewNRef]

    # Apply corrections based on dT and aT
    mask = df['dT'] > df['aT']

    df.loc[mask, 'Tmax_corr'] = df[tmaxNRef] - (df['bT'] * (df['dT'] - df['aT']))  #(2.13) pg. 41
    df.loc[mask, 'Tmin_corr'] = df[tminNRef] - (df['bT'] * (df['dT'] - df['aT']))  #(2.14) pg. 41   
    df.loc[mask, 'Tdew_corr'] = df[tdewNRef] + ((1.0 - df['bT']) * (df['dT'] - df['aT']))  #(2.15) pg. 41

    # Add AI column
    df['AI'] = AI

    # Return the updated DataFrame
    return df

# station_id2_cimis_daily_2014-01-01_to_2024-12-31_MASTER.csv


def apply_temp_corrections(id): 
    dir = f'./cimis_data/station{id}/'
    with os.scandir(dir) as entries:   
        for entry in entries:            
            df = pd.read_csv(f'{dir}{entry.name}')
            df = fix_col_names(df)
            print(f"Columns in the DataFrame: {df.columns}")
            df = corr_nref(df)
            print(df)  
            df.to_csv(f'{dir}{entry.name}', index=False)


def do_temp_corrections():
    for id in station_ids:
        apply_temp_corrections(id) 


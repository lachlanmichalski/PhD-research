#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 28 15:02:38 2020
@author: lachlan
"""

import wrds
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import DateOffset
import os
import pandas as pd
import numpy as np
import datetime as dt
import itertools
import eikon as ek
import pandas as pd
from functools import reduce

from sklearn.preprocessing import LabelEncoder

os.chdir()

from WRDS_Globalratios import  wrds_ratios_global
from WRDS_USratios import  wrds_ratios_US
from S1_functions import *
ek.set_app_key('XYZ')


###################
# Connect to WRDS #
###################

'''Connects to WRDS API, have to manually input username and password in 
console'''

conn = wrds.Connection(wrds_username = 'WRDS_login')

'''Extracting data for Ratios Based on Annual Data and Quarterly Data'''

###################
   # SQL Block #
###################
#/*Get pricing for primary US common shares from Security Monthly table*/

comppricing='''
SELECT distinct cusip, gvkey
FROM comp.secm
ORDER BY cusip'''
uniqueusfirms = conn.raw_sql(comppricing)

list_uniqueus = list((uniqueusfirms[uniqueusfirms['cusip'].notnull()].iloc[:,0]))
len(list_uniqueus)

comppricing = '''
SELECT distinct isin
FROM comp.g_secd
ORDER BY isin'''
uniqueglofirms = conn.raw_sql(comppricing)
list_uniquegl = list((uniqueglofirms[uniqueglofirms['isin'].notnull()].iloc[:,0]))

##############################################################################

'''U.S. Sample'''

start_date='1983-01-01'
end_date='2020-01-01'

'''1. Returns a list of U.S. firm ids entered that have a credit rating'''
us_firms_wrds = cr_firms_us(list_uniqueus,start_date, end_date)

'''2. Return df of quarterly mapped to monthly financial ratios of U.S. firms inputted'''
us_financials = cr_firms_financials_wrds_us(us_firms_wrds, 'WRDS_login')

#us_financials.to_csv('us_full_S1.csv', sep=',') #save to csv file 468MB
us_full = pd.read_csv('us_full_S1.csv').iloc[:,1:] # read in us_financial wrds ratios
us_full['gvkey'].nunique() #3888 unique firms
us_unique_fin = list((us_full['cusip'].unique())) #unique id cusip to use to retrieve cr
us_full['publicdate'] = pd.to_datetime(us_full['publicdate'])

'''3. Return df of credit ratings of firms with financial ratio data U.S. and global'''
cr_firms_us = cr_with_financials(us_unique_fin, start_date, end_date)

'''4. Merge credit rating and financial df'''
us_cr_fin = merge_fin_cr(us_full, cr_firms_us, 'cusip', 'SPI')

'''5. Remove us_full, cr_firms_us from memory'''
del us_full, cr_firms_us

'''6. Merge in sovereign ratings, watchlist and outlook'''
unique_country_us = us_cr_fin['loc'].unique()

sov,wl,ol = country_sov_rating(unique_country_us, start_date, end_date)

for df in (sov,ol):
    del df['Instrument']
    us_cr_fin = us_cr_fin.merge(df, how='left', on=['loc','Date'])
    if df.equals(sov):
        us_cr_fin.update(us_cr_fin.groupby('loc')['sov_rating'].ffill()) #forward fill sov_rating
        us_cr_fin.update(us_cr_fin.groupby('loc')['sov_rating'].bfill()) #back fill sov_rating
    elif df.equals(ol):
        us_cr_fin.update(us_cr_fin.groupby('loc')['sov_outlook'].ffill()) #forward fill sov_rating
        us_cr_fin.update(us_cr_fin.groupby('loc')['sov_outlook'].bfill()) #back fill sov_rating

#encode string variable rating to numbers
labelencoder = LabelEncoder()
us_cr_fin['sov_rating_rank'] = labelencoder.fit_transform((us_cr_fin['sov_rating'].astype(str)))
    
#encode string variable rating to numbers
labelencoder = LabelEncoder()
us_cr_fin['sov_outlook_rank'] = labelencoder.fit_transform((us_cr_fin['sov_outlook']).astype(str))

us_cr_fin.rename(columns={'Rating Rank':'rating_rank'},inplace=True)

'''7. Recommendation data for US'''
from ibes_rec_function import ibes_rec_data

ibes_data_list = list(us_cr_fin['isin'].dropna().unique()) # list as input for rec function
ibes_data = ibes_rec_data(ibes_data_list, '1993-11-01', '2020-01-01')
#ibes_data.to_csv('US_recommendation_data_S1.csv')
ibes_data = pd.read_csv('US_recommendation_data_S1.csv').iloc[:,1:]
ibes_data['Date']=pd.to_datetime(ibes_data['Date'])

#merge ibes with us_sample
us_cr_fin = us_cr_fin.merge(ibes_data, how='left', on=['isin','Date'])

'''8. Institutional holdings for US'''

'''Cannot retrieve directly through WRDS API. Have to manually download from website
    and use the following list of cusip codes to retrieve the data'''

inst_cusip = pd.DataFrame(us_cr_fin['Instrument'].astype(str).str[:-1].unique())
#inst_cusip.to_csv('inst_cusip.txt', header=None, index=None, sep=' ', mode='a')

inst_hold = pd.read_csv('inst_hold_S1.csv')
len(inst_hold['cusip'].unique()) #2982 of the 3015
inst_hold.rename(columns={'rdate':'Date'},inplace=True)
inst_hold['Date'] = pd.to_datetime(inst_hold['Date'].astype(str))

us_cr_fin['cusip'] = us_cr_fin['Instrument'].astype(str).str[:-1] #inst holding cusip 8 # not 9#

us_cr_fin = us_cr_fin.merge(inst_hold, how='left', on=['cusip','Date'])
us_cr_fin.update(us_cr_fin.groupby('Instrument')[inst_hold.columns[9:]].ffill(limit=2)) #forward fill sov_rating

'''9. Merge financials and credit ratings with macro data'''
'''Refer to macro_S1.py for collection of macro data'''

macro = pd.read_excel('country_counts_S1.xlsx')
macro = macro.dropna(subset=['GDP'])
macro = macro[macro['count']>=10]
macro_country_list = list(macro['country'])
macro['loc']=macro.iloc[:,2].astype(str).str[1:3]
macro_country_name = macro[['country', 'loc']] #macro country and reuters 2 digit code
us_cr_fin = us_cr_fin.merge(macro_country_name, how = 'left', on = 'loc')

macro_stand=pd.ExcelFile('standardized_macro_S1.xlsx')

for idx, sheet_name in enumerate(macro_stand.sheet_names):
    df = macro_stand.parse(sheet_name) #read in sheet from xlsx file
    df.set_index('Date', inplace=True) 
    if sheet_name == 'population':
        print(sheet_name)
        df.reset_index(inplace=True)
    elif sheet_name == 'population%':
        print(sheet_name)
        df.reset_index(inplace=True)
    else:
        df = df.resample('Q').mean() #resample monthly to quarterly
        df.reset_index(inplace=True)
    
    new_date_idx = pd.date_range(df.Date.min(), df.Date.max(), freq = 'M') #resample to monthly
    df.set_index(['Date'], inplace=True)
    df.index = pd.DatetimeIndex(df.index) #reindex sample occurs here
    df = df.reindex(new_date_idx,method='ffill') #forward fill quarterly data to the three months in that quarter
    df = df.rename(columns=macro_country_name.set_index('country')['loc']).reset_index()
    df.rename(columns={'index':'qdate'},inplace=True)
    df = df.melt('qdate', var_name = 'loc', value_name=sheet_name) #melt for merge into main df
    us_cr_fin = pd.merge(us_cr_fin,df,left_on=['qdate','loc'],right_on=['qdate','loc'], how='left') #merge
    print('Merged US sample with '+ str(idx) + '.' + ' ' +sheet_name)   
    
#sample in monthly format    
us_cr_fin.to_csv('US_sample_No_ESG_S1.csv', sep=',')

#sample to use - quarterly format
data_us = us_cr_fin.drop_duplicates(subset=['qdate','isin']) #116,843 observations

'''10. ESG data merge'''

us_isin_unique = list(us_cr_fin['isin'].unique())  #unique isin U.S. codes for identifier
len(us_isin_unique) #2727 U.S. firms

#read in ESG function inputs for data retrieval in Eikon
ESG = pd.read_excel('')
ESG_df = ESG.iloc[:, 1:]
ESG_df = ESG_df.combine_first(pd.Series(ESG.values.ravel('F')).to_frame('ESG_variables'))
ESG_df = ESG_df['ESG_variables'].dropna()

ESG_list = list(ESG_df)
ESG_list.append('TR.TRESGScore.date')

start_date = '2000-01-01'
end_date = '2020-01-01'

'''Function to retrieve all available ESG variables for U.S. firms in sample'''
ESG_us = ESG_data(us_isin_unique, ESG_list, start_date, end_date) #takes approx 30mins
#ESG_us.to_csv('ESG_us_S1.csv', sep=',')

us_cr_fin_ESG = us_cr_fin.merge(ESG_us, how='left', on=['isin', 'Date'])
us_cr_fin_ESG.to_csv('US_sample_ESG_S1.csv', sep=',')

################################################################################
################################################################################
'''Global Sample'''

'''1. Returns a list of global firm ids entered that have a credit rating'''
gl_firms_wrds = cr_firms_gl(list_uniquegl,start_date, end_date)

gl_financials = cr_firms_financials_wrds_gl(gl_firms_wrds, 'WRDS_login')
#gl_financials.to_csv('global_full_S1.csv', sep=',') #save to csv file 285MB
global_full = pd.read_csv('global_full_S1.csv').iloc[:,1:] #read in gl_financial wrds ratios
global_full['publicdate'] = pd.to_datetime(global_full['publicdate'])
global_full['gvkey'].nunique() #5159 unique firms 
global_full['loc'] = global_full['isin'].astype(str).str[:2] #location e.g. AU, US, GB
global_unique_fin = list((global_full['isin'].unique())) #unique id isin to use to retrieve cr
df_count = (global_full.groupby('loc')['isin'].apply(lambda x: len(np.unique(x))))
df_count = df_count.sort_values(ascending=False) #count for each country before limiting to credit rating only sample

'''Count by location'''
df_count = (global_full.groupby('loc')['isin'].apply(lambda x: len(np.unique(x))))

'''2. Return df of credit ratings of firms with financial ratio data U.S. and global'''

cr_firms_gl = cr_with_financials(global_unique_fin, start_date, end_date)
cr_firms_gl.rename(columns={'ISIN':'isin'},inplace=True)

'''3. Merge credit rating and financial df'''
gl_cr_fin = merge_fin_cr(global_full, cr_firms_gl, 'isin', 'SPI')
gl_cr_fin.to_csv('a.csv',sep=',')
gl_cr_fin[gl_cr_fin['loc']=='CN']  
df_count = (gl_cr_fin.groupby('loc')['Instrument'].apply(lambda x: len(np.unique(x))))
df_count = df_count.sort_values(ascending=False)

'''4. Remove us_full, cr_firms_us from memory'''
del global_full, cr_firms_gl

''5. 'Merge in sovereign ratings, watchlist and outlook'''
unique_country_gl = gl_cr_fin['loc'].unique()

sov,wl,ol = country_sov_rating(unique_country_gl, start_date, end_date)

for df in (sov,ol):
    del df['Instrument']
    gl_cr_fin = gl_cr_fin.merge(df, how='left', on=['loc','Date'])
    if df.equals(sov):
        gl_cr_fin.update(gl_cr_fin.groupby('loc')['sov_rating'].ffill()) #forward fill sov_rating
        gl_cr_fin.update(gl_cr_fin.groupby('loc')['sov_rating'].bfill()) #back fill sov_rating
    elif df.equals(ol):
        gl_cr_fin.update(gl_cr_fin.groupby('loc')['sov_outlook'].ffill()) #forward fill sov_rating
        gl_cr_fin.update(gl_cr_fin.groupby('loc')['sov_outlook'].bfill()) #back fill sov_rating

#encode string variable rating to numbers
labelencoder = LabelEncoder()
gl_cr_fin['sov_rating_rank'] = labelencoder.fit_transform((gl_cr_fin['sov_rating'].astype(str)))
    
#encode string variable rating to numbers
labelencoder = LabelEncoder()
gl_cr_fin['sov_outlook_rank'] = labelencoder.fit_transform((gl_cr_fin['sov_outlook']).astype(str))

gl_cr_fin.rename(columns={'Rating Rank':'rating_rank'},inplace=True)

'''6. Recommendation data for global'''
from ibes_rec_function import ibes_rec_data

ibes_data_list = list(gl_cr_fin['isin'].dropna().unique()) # list as input for rec function
ibes_data = ibes_rec_data(ibes_data_list, '1993-11-01', '2020-01-01')
ibes_data.to_csv('gl_recommendation_data_S1.csv')
ibes_data = pd.read_csv('gl_recommendation_data_S1.csv').iloc[:,1:]
ibes_data['Date']=pd.to_datetime(ibes_data['Date'])

#merge ibes with gl_sample
gl_cr_fin = gl_cr_fin.merge(ibes_data, how='left', on=['isin','Date'])

'''7. Merge financials and credit ratings with macro data'''
'''Refer to macro_S1.py for collection of macro data'''

macro = pd.read_excel('country_counts_S1.xlsx')
macro = macro.dropna(subset=['GDP'])
macro = macro[macro['count']>=10]
macro_country_list = list(macro['country'])
macro['loc']=macro.iloc[:,2].astype(str).str[1:3]
macro_country_name = macro[['country', 'loc']] #macro country and reuters 2 digit code
gl_cr_fin = gl_cr_fin.merge(macro_country_name, how = 'left', on = 'loc')

macro_stand=pd.ExcelFile('standardized_macro_S1.xlsx')

for idx, sheet_name in enumerate(macro_stand.sheet_names):
    df = macro_stand.parse(sheet_name) #read in sheet from xlsx file
    df.set_index('Date', inplace=True) 
    if sheet_name == 'population':
        print(sheet_name)
        df.reset_index(inplace=True)
    elif sheet_name == 'population%':
        print(sheet_name)
        df.reset_index(inplace=True)
    else:
        df = df.resample('Q').mean() #resample monthly to quarterly
        df.reset_index(inplace=True)
    
    new_date_idx = pd.date_range(df.Date.min(), df.Date.max(), freq = 'M') #resample to monthly
    df.set_index(['Date'], inplace=True)
    df.index = pd.DatetimeIndex(df.index) #reindex sample occurs here
    df = df.reindex(new_date_idx,method='ffill') #forward fill quarterly data to the three months in that quarter
    df = df.rename(columns=macro_country_name.set_index('country')['loc']).reset_index()
    df.rename(columns={'index':'qdate'},inplace=True)
    df = df.melt('qdate', var_name = 'loc', value_name=sheet_name) #melt for merge into main df
    gl_cr_fin = pd.merge(gl_cr_fin,df,left_on=['qdate','loc'],right_on=['qdate','loc'], how='left') #merge
    print('Merged global sample with '+ str(idx) + '.' + ' ' +sheet_name)    

'''8. Institutional holdings for global'''

'''Do not have subscription access to this data for global firms'''

'''Currency conversion to USD for mktcap and price for global firms'''

gl_country = data_gl.groupby(by=['country'], as_index=False).first() #get all unique countries 
gl_country = gl_country[['country','loc']] #interested only in country and location
#gl_country.to_csv('gl_country.csv',sep=',')
fx=pd.read_csv('fx_codes.csv',sep=',') #manually add eikon fx codes of countries in sample to USD
fx=fx.iloc[:,1:]
US = np.where(fx.values == 'US') #find which row is 'US'
US_idx = [x[0] for x in US][0]
idx = [US_idx] + [i for i in range(len(fx)) if i != US_idx]
fx = fx.iloc[idx].reset_index(drop=True)

fx_= fx_USD(fx, start_date, end_date) #conversion happens here
fx_['Date'] = pd.to_datetime(fx_['Date'])

gl_cr_fin = gl_cr_fin.merge(fx_, how='left', on=['Date','loc']) #merge into global sample
gl_cr_fin = gl_cr_fin.iloc[:,1:]

#create USD_mktcap, price_adj and price_unadj
gl_cr_fin['USD_mktcap']=gl_cr_fin['mktcap']*gl_cr_fin['fx_USD']
gl_cr_fin['USD_price_adj']=gl_cr_fin['price_adj']*gl_cr_fin['fx_USD']
gl_cr_fin['USD_price_unadj']=gl_cr_fin['price_unadj']*gl_cr_fin['fx_USD']

gl_cr_fin.to_csv('gl_sample_No_ESG_S1.csv', sep=',') #save to excel file #338MB

gl_cr_fin = pd.read_csv('gl_sample_No_ESG_S1.csv')
#gl_cr_fin['Date']=pd.to_datetime(gl_cr_fin['Date'])


'''9. ESG data merge'''

gl_isin_unique = list(gl_cr_fin['isin'].unique())  #unique isin global codes for identifier
len(gl_isin_unique) #1855 global firms

#read in ESG function inputs for data retrieval in Eikon
ESG = pd.read_excel('')
ESG_df = ESG.iloc[:, 1:]
ESG_df = ESG_df.combine_first(pd.Series(ESG.values.ravel('F')).to_frame('ESG_variables'))
ESG_df = ESG_df['ESG_variables'].dropna()

ESG_list = list(ESG_df)
ESG_list.append('TR.TRESGScore.date')

start_date = '2000-01-01'
end_date = '2020-01-01'

'''Function to retrieve all available ESG variables for U.S. firms in sample'''
ESG_gl = ESG_data(gl_isin_unique, ESG_list, start_date, end_date) #takes approx 10mins
#ESG_gl.to_csv('ESG_gl_S1.csv', sep=',')
ESG_gl = pd.read_csv('ESG_gl_S1.csv')
ESG_gl = ESG_gl.iloc[:,1:]
ESG_gl['Date'] = pd.to_datetime(ESG_gl['Date'])


gl_cr_fin_ESG = gl_cr_fin.merge(ESG_gl, how='left', on=['isin', 'Date'])
gl_cr_fin_ESG.to_csv('gl_sample_ESG_S1.csv', sep=',')

##############################################################################
################################################################################

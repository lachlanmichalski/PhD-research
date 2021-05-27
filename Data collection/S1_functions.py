#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 09:43:52 2020

@author: lockie
"""
############################################################################

def cr_firms_us(list_unique_id,start_date, end_date):
    '''Returns a list of U.S. firm ids entered that have a credit rating'''
    df_noratinglist = []
    df_rating = []
    batch_size = 1000    
    for idx, i in enumerate(range(0, len(list_unique_id), batch_size)):
        unique_rated = list_unique_id[i:i+batch_size]                    
        start_date = start_date
        end_date = end_date
        try:
            df, err = ek.get_data(
            instruments = unique_rated,
            fields = ['TR.IR.RatingDate','TR.IssuerRating'],
            parameters={'SDate': '{}'.format(start_date), 
                        'EDate': '{}'.format(end_date), 
                        'Frq':'CQ'})
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df = df.dropna(subset=[df.columns[2]])
            unique_list = list(df['Instrument'].unique())
            df_rating.append(unique_list)
            print('{} firms DONE :)'.format(idx*batch_size))
        except KeyError as e:
            print('Error', e)
            df_noratinglist.append(unique_rated)
            continue      
    rated_firms = list(itertools.chain.from_iterable(df_rating))
    return rated_firms

############################################################################
    
def cr_firms_gl(list_unique_id,start_date, end_date):
    '''Returns a list of global firm ids entered that have a credit rating'''
    df_noratinglist = []
    df_rating = []
    batch_size = 1000    
    for idx, i in enumerate(range(0, len(list_unique_id), batch_size)):
        unique_rated = list_unique_id[i:i+batch_size]                    
        start_date = start_date
        end_date = end_date
        try:
            df, err = ek.get_data(
            instruments = unique_rated,
            fields = ['TR.IR.RatingDate','TR.IssuerRating'],
            parameters={'SDate': '{}'.format(start_date), 
                        'EDate': '{}'.format(end_date), 
                        'Frq':'CQ'})
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df = df.dropna(subset=[df.columns[2]])
            unique_list = list(df['Instrument'].unique())
            df_rating.append(unique_list)
            print('{} firms DONE :)'.format(idx*batch_size))
        except KeyError as e:
            print('Error', e)
            df_noratinglist.append(unique_rated)
            continue      
    rated_firms = list(itertools.chain.from_iterable(df_rating))
    return rated_firms

############################################################################
    
def cr_firms_financials_wrds_us(list_rated_firms, wrds_username):
    '''Return df of quarterly mapped to monthly financial ratios of U.S. firms inputted'''
    df_q_f=[]
    batch_size = 3000 # read through and retrieve in batches
    for idx, i in enumerate(range(0, len(list_rated_firms), batch_size)):
        unique_rated = list_rated_firms[i:i+batch_size]                    
        dfq = wrds_ratios_US(unique_rated, wrds_username)
        df_q_f.append(dfq)
        print('{} firms DONE :)'.format(idx*batch_size))
        
    financials_df = pd.concat(df_q_f)
    return financials_df

############################################################################
    
def cr_firms_financials_wrds_gl(list_rated_firms, wrds_username):
    '''Return df of quarterly mapped to monthly financial ratios of global firms inputted'''
    df_q_f=[]
    batch_size = 3000    
    for idx, i in enumerate(range(0, len(gl_rated), batch_size)):
        unique_rated = gl_rated[i:i+batch_size]                    
        dfq = wrds_ratios_global(unique_rated, wrds_username)
        df_q_f.append(dfq)
        print('{} firms DONE :)'.format(idx*batch_size))
    
    financials_df = pd.concat(df_q_f)
    return financials_df

############################################################################
    
def cr_with_financials(list_unique_id, start_date, end_date):
    '''Return df of credit ratings of firms with financial ratio data U.S. and global'''
    df_noratinglist = []
    df_rating = []
    batch_size = 500    
    for idx, i in enumerate(range(0, len(list_unique_id), batch_size)):
        unique_rated = list_unique_id[i:i+batch_size]                    
        start_date = start_date
        end_date = end_date
        try:
            df, err = ek.get_data(
            instruments = unique_rated,
            fields = ['TR.IR.RatingDate','TR.IssuerRating','TR.IR.RatingRank',
                  'TR.IR.RatingSourceType','TR.IR.RatingSourceDescription',
                  'TR.IR.RatingScopeDescription'],
            parameters={'SDate': '{}'.format(start_date), 
                        'EDate': '{}'.format(end_date), 
                        'Frq':'CQ'})
            
            df_1, err = ek.get_data(
            instruments = unique_rated,
            fields = ['TR.CompanyPublicSinceDate','TR.IPODate','TR.CompanyIncorpDate', 
                      'TR.RegCountryCode','TR.RegistrationCountry','TR.ExchangeCountry',
                      'TR.ExchangeName', 'TR.PrimaryQuote', 'TR.ISIN'],
            parameters={'SDate': '{}'.format(start_date), 
                        'EDate': '{}'.format(end_date), 
                        'Frq':'CQ'})
    
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df = df.dropna(subset=[df.columns[2]])
            df = df.merge(df_1, how = 'left', on = 'Instrument')
            df_rating.append(df)
            print('{} firms DONE :)'.format(idx*batch_size))
        except KeyError as e:
            print('Error', e)
            df_noratinglist.append(unique_rated)
            continue      
    rated_firm_fin = pd.concat(df_rating)
    return rated_firm_fin

############################################################################
    
def merge_fin_cr(fin_df, cr_df, id_var, rating_agency):
    '''Merge financial and credit rating df together for U.S. and global firms.
    For S&P ratings, rating_agency = 'SPI'
    For Moody's ratings, rating_agency = 'MIS
    For U.S. firms, id_var = 'cusip'
    For global firms, id_var = 'isin'''
    
    cr_df = cr_df.drop_duplicates(['Instrument','Date'],keep= 'last')

    #Make S&P or Moody's sample
    if rating_agency == 'SPI':
        cr_df = cr_df[cr_df['Rating Source Type']=='SPI'] 
        print(str(cr_df['Instrument'].nunique())+ ' SPI'+' firms') #S&P Long term issuer rating unique firms
    elif rating_agency == 'MIS':
        cr_df = cr_df[cr_df['Rating Source Type']=='MIS'] 
        print(str(cr_df['Instrument'].nunique())+ ' MIS'+' firms') #S&P Long term issuer rating unique firms
    else:
        print('Enter SPI or MIS in rating_agency')
    #date in Y-m-d format, datetime
    cr_df['Date'] = pd.to_datetime(cr_df['Date']).dt.date #date without timestamp
    cr_df['Date'] = pd.to_datetime(cr_df['Date']) + MonthEnd(0) #end of month to merge
    
    #reindex quarterly and fill to make monthly. (Fill the quarter values to the respective 3 months)
    new_date_idx = pd.date_range(cr_df.Date.min(), cr_df.Date.max(), freq = 'M')
    cr_df.set_index(['Instrument', 'Date'], inplace=True)
    
    mux = pd.MultiIndex.from_product([cr_df.index.levels[0], new_date_idx], 
                                     names=cr_df.index.names)
    cr_df=cr_df.reindex(mux)
    cr_df.reset_index(inplace=True)
    f = lambda x: x.ffill() #lambda function to forward fill to qtr, only 2 after last obs
    cr_df = cr_df.groupby('Instrument')[cr_df.columns].apply(f).\
        dropna(subset = ['Issuer Rating']).reset_index(drop=True)
    #drop values that are NR - means firm is no longer rated
    cr_df = cr_df[cr_df['Issuer Rating'] != 'NR'] # 481,510 US rated
    
    if 'isin' in cr_df.columns:
        #global merge credit rating with financial data
        cr_df=cr_df.merge(fin_df, how = 'left', left_on=['isin', 'Date'], 
                          right_on = [id_var,'publicdate'])
    else:
        #U.S. merge credit rating with financial data
        cr_df=cr_df.merge(fin_df, how = 'left', left_on=['Instrument', 'Date'], 
                          right_on = [id_var,'publicdate'])
    
    #datetime quarterly for merge with macro data
    cr_df['qdate'] = pd.to_datetime(cr_df['qdate']) #date without timestamp
    if 'ISIN' in cr_df.columns:
        cr_df.rename(columns={'ISIN':'isin'},inplace=True)
    cr_df['loc']=cr_df['isin'].astype(str).str[:2]
    cr_df=cr_df[cr_df['loc']!='']
    
    return cr_df

############################################################################
"""STILL WORKING ON THIS
        
def firm_wl_ol(list_of_firms, start_date, end_date):
    '''Return df of 10 year sovereign ratings, watchlist and outlook info'''
            
    #retrive sovereign 10 year bond watchlist listing
    wl, err = ek.get_data(
        instruments=us_unique_fin,
        fields=['TR.IW.WatchType','TR.IW.WatchDate','TR.IW.WatchEndDate'],
        parameters={'SDate': '{}'.format(start_date), 
                    'EDate': '{}'.format(end_date), 
                    'IssuerRatingSrc':'SPI'})
    
    wl = wl.drop_duplicates(['Instrument','Date'],keep= 'last')
    wl.replace(r'^\s*$', np.nan, regex=True, inplace=True) #replace blank cells with NaN
    wl.dropna(subset=['Watch Type'], inplace=True)
    wl['Date'] = pd.to_datetime(wl['Date']).dt.date #date without timestamp
    wl['Date'] = pd.to_datetime(wl['Date']) + MonthEnd(0) #end of month to merge
    wl.rename(columns={'Watch End Date':'we_date',
                       'Watch Type':'watch_type'},inplace=True)
    wl['we_date'] = pd.to_datetime(wl['we_date']).dt.date #date without timestamp
    wl['we_date'] = pd.to_datetime(wl['we_date']) + MonthEnd(0) #end of month to merge
    wl['watch_type'] = wl['watch_type'].str.lower()
    wl['watch_type'].unique()
    wl = wl[wl['watch_type']!='evo']
    wl = wl.drop_duplicates(['Instrument','Date'],keep= 'last')

    #encode string variable ['pos', 'neg', 'dev', 'upg', 'dng', 'unc'] to numbers
    labelencoder = LabelEncoder()
    wl['watch_type_rank'] = labelencoder.fit_transform(wl['watch_type'])
    
    #reindex quarterly and fill to make monthly. (Fill the quarter values to the respective 3 months)
    new_date_idx = pd.date_range(wl.Date.min(), wl.Date.max(), freq = 'M')
    wl.set_index(['Instrument', 'Date'], inplace=True)
    
    mux = pd.MultiIndex.from_product([wl.index.levels[0], new_date_idx], 
                                     names=wl.index.names)
    wl=wl.reindex(mux)
    wl.reset_index(inplace=True)
    
    df=wl
    df.update(df.groupby('Instrument')[fill_cols].ffill()) #forward fill sov_rating
    df[fill_cols] = df[fill_cols].mask(df['we_date'] > df['Date'])

    
    f = lambda x: x.ffill() #lambda function to forward fill to qtr, only 2 after last obs
    df = df.groupby('Instrument')[fill_cols].apply(f)
    
    
    
    
    fill_cols = ['watch_type', 'we_date', 'watch_type_rank']
    df[fill_cols] = df[fill_cols].ffill()
df[fill_cols] = df[fill_cols].mask(df['admitTime'] > df['dischargeTime'])
    
    
    f = lambda x: x.ffill() #lambda function to forward fill to qtr, only 2 after last obs
    cr_df = cr_df.groupby('Instrument')[cr_df.columns].apply(f).\
        dropna(subset = ['Issuer Rating']).reset_index(drop=True)
    


    wl['Instrument']
    sov = us_cr_fin[us_cr_fin['Instrument']=='001669100']

    
    001669100
            us_cr_fin.update(us_cr_fin.groupby('loc')['sov_rating'].ffill()) #forward fill sov_rating

    us_unique_fin
    
    
    watchlist_sov['Date'] = pd.to_datetime(watchlist_sov['Date']).dt.date #date without timestamp
    watchlist_sov['Date'] = pd.to_datetime(watchlist_sov['Date']) + MonthEnd(0) #end of month to merge
    watchlist_sov['loc'] = watchlist_sov['Instrument'].astype(str).str[:2]
    print('Watchlist DONE...')

    #retrive sovereign 10 year bond outlook
    outlook_sov, err = ek.get_data(
        instruments=country_list_inputs,
        fields=['TR.IO.Outlook','TR.IO.OutlookDate'],
        parameters={'SDate': '{}'.format(start_date), 
                    'EDate': '{}'.format(end_date), 
                    'IssuerRatingSrc':'SPI'})
    outlook_sov['Outlook'] = outlook_sov['Outlook'].str.lower()
    outlook_sov['Outlook'] = outlook_sov['Outlook'].replace({'sta':'stable', 'stb':'stable',
                                                             'neg':'negative',
                                                             'pos':'positive'})

    outlook_sov['Date'] = pd.to_datetime(outlook_sov['Date']).dt.date #date without timestamp
    outlook_sov['Date'] = pd.to_datetime(outlook_sov['Date']) + MonthEnd(0) #end of month to merge
    outlook_sov['loc'] = outlook_sov['Instrument'].astype(str).str[:2]
    outlook_sov.rename(columns={'Outlook':'sov_outlook'},inplace=True)

    print('Outlook DONE...')

    return cr_sov, watchlist_sov, outlook_sov
    
"""

############################################################################

def fx_USD(fx_df, start_date, end_date):
    
    fx_d=[]
    for idx, fx_ in fx_df.iterrows():
        loc=fx_[1]
        df = ek.get_timeseries([fx_[0]], 
                               ['Close'], 
                               start_date = start_date, 
                               end_date = end_date,
                               interval='monthly')
        df.rename(columns={'CLOSE':'fx_USD'},inplace=True)
        df['loc']=loc
        df.reset_index(inplace=True)
        fx_d.append(df)
        print(str(idx)+',' + ' '+ loc + ' currency')
        
    fx_d = pd.concat(fx_d)
    fx_d['fx_USD']= 1/fx_d['fx_USD']
    
    return fx_d

############################################################################
    
def country_sov_rating(list_countrycodes, start_date, end_date):
    '''Return df of 10 year sovereign ratings, watchlist and outlook info'''
    
    country_list_inputs = [s + '10YT=RR' for s in list_countrycodes] #adds 10 year gov bond code to each country
    #retrive sovereign 10 year bond issuer rating
    cr_sov, err = ek.get_data(
        instruments=country_list_inputs,
        fields=['TR.IssuerRating','TR.IR.RatingDate'],
        parameters={'SDate': '{}'.format(start_date), 
                    'EDate': '{}'.format(end_date), 
                    'IssuerRatingSrc':'SPI'})
    cr_sov['Date'] = pd.to_datetime(cr_sov['Date']).dt.date #date without timestamp
    cr_sov['Date'] = pd.to_datetime(cr_sov['Date']) + MonthEnd(0) #end of month to merge
    cr_sov['loc'] = cr_sov['Instrument'].astype(str).str[:2]
    cr_sov.rename(columns={'Issuer Rating':'sov_rating'},inplace=True)    
    print('Sov_rating DONE...')
        
    #retrive sovereign 10 year bond watchlist listing
    watchlist_sov, err = ek.get_data(
        instruments=country_list_inputs,
        fields=['TR.IW.WatchType','TR.IW.WatchDate','TR.IW.WatchEndDate'],
        parameters={'SDate': '{}'.format(start_date), 
                    'EDate': '{}'.format(end_date), 
                    'IssuerRatingSrc':'SPI'})
    watchlist_sov['Date'] = pd.to_datetime(watchlist_sov['Date']).dt.date #date without timestamp
    watchlist_sov['Date'] = pd.to_datetime(watchlist_sov['Date']) + MonthEnd(0) #end of month to merge
    watchlist_sov['loc'] = watchlist_sov['Instrument'].astype(str).str[:2]
    print('Watchlist DONE...')

    #retrive sovereign 10 year bond outlook
    outlook_sov, err = ek.get_data(
        instruments=country_list_inputs,
        fields=['TR.IO.Outlook','TR.IO.OutlookDate'],
        parameters={'SDate': '{}'.format(start_date), 
                    'EDate': '{}'.format(end_date), 
                    'IssuerRatingSrc':'SPI'})
    outlook_sov['Outlook'] = outlook_sov['Outlook'].str.lower()
    outlook_sov['Outlook'] = outlook_sov['Outlook'].replace({'sta':'stable', 'stb':'stable',
                                                             'neg':'negative'})

    outlook_sov['Date'] = pd.to_datetime(outlook_sov['Date']).dt.date #date without timestamp
    outlook_sov['Date'] = pd.to_datetime(outlook_sov['Date']) + MonthEnd(0) #end of month to merge
    outlook_sov['loc'] = outlook_sov['Instrument'].astype(str).str[:2]
    outlook_sov.rename(columns={'Outlook':'sov_outlook'},inplace=True)    
    print('Outlook DONE...')

    return cr_sov, watchlist_sov, outlook_sov

############################################################################
    
def ESG_data(unique_list_isin, ESG_list, start_date, end_date):
    df_noESG = []
    df_ESG = []
    batch_size = 250   
    for idx, i in enumerate(range(0, len(unique_list_isin), batch_size)):
        unique_ESG = unique_list_isin[i:i+batch_size]    
        start_date = start_date
        end_date = end_date
        try:
            df, err = ek.get_data(
                instruments = unique_ESG,
                fields = ESG_list,
                parameters={'SDate': '{}'.format(start_date), 
                            'EDate': '{}'.format(end_date)})
            
            df.set_index('Date',inplace=True)
            df.reset_index(inplace=True)
            
            df['Date'] = pd.to_datetime(df['Date']).dt.date #date without timestamp
            df['Date'] = pd.to_datetime(df['Date']) + MonthEnd(0) #end of month to merge
            df = df.drop_duplicates(['Instrument','Date'],keep= 'last')
            
            #reindex annual and fill to make monthly. (Fill the quarter values to the respective 3 months)
            new_date_idx = pd.date_range(df.Date.min(), df.Date.max(), freq = 'M')
            df.set_index(['Instrument', 'Date'], inplace=True)
            
            mux = pd.MultiIndex.from_product([df.index.levels[0], new_date_idx], 
                                             names=df.index.names)
            df=df.reindex(mux)
            df.reset_index(inplace=True)
            df.update(df.groupby('Instrument')[df.columns[2:]].ffill()) #forward fill sov_rating
            df_ESG.append(df)
            print('{} firms DONE :)'.format(idx*batch_size))
        except KeyError as e:
            print('Error', e)
            df_noESG.append(unique_rated)
            continue      
    df_ESG = pd.concat(df_ESG)
    df_ESG.rename(columns={'Instrument':'isin'},inplace=True)
    return df_ESG

############################################################################

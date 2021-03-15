#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 11:38:46 2020
@author: lachlan
"""

import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import LabelEncoder

##############################################################################
'''Final sample update'''
##############################################################################

'''US sample update - rather bias down the sample'''

def clean_finaldata(data, data_type, ESG):
    
    # list of features
    if data_type == 'us':
        financial_features = ['accrualq', 'aftret_eqq', 'aftret_equityq', 
                              'aftret_invcapxq', 'at_turnq', 'bmq', 'capeiq', 
                              'capital_ratioq', 'cash_conversionq', 'cash_debtq', 
                              'cash_ltq','cash_ratioq', 'cfmq', 'curr_debtq',
                              'curr_ratioq', 'debt_atq', 'de_ratioq', 
                              'debt_capitalq', 'debt_ebitdaq', 'debt_invcapq', 
                              'divyieldq', 'dltt_beq', 'dprq', 'efftaxq', 
                              'equity_invcapq', 'evmq', 'fcf_ocfq', 'gpmq', 'GProfq', 
                              'int_debtq', 'int_totdebtq', 'intcov_ratioq', 'intcovq', 
                              'inv_turnq', 'invt_actq', 'lt_atq', 'lt_debtq', 
                              'lt_ppentq', 'mktcap', 'npmq', 'ocf_lctq', 'opmadq', 
                              'opmbdq', 'pay_turnq', 'pcfq', 'pe_exiq', 'pe_incq', 
                              'pe_op_basicq', 'pe_op_dilq', 'PEG_1yrforward', 
                              'PEG_ltgforward', 'PEG_trailing', 'pretret_earnatq',
                              'pretret_noq', 'price_adj', 'profit_lctq', 
                              'psq', 'ptbq', 'ptpmq', 'quick_ratioq', 'rd_saleq', 
                              'rect_actq', 'rect_turnq', 'roaq', 'roceq', 'roeq',
                              'sale_equityq', 'sale_invcapq', 'sale_nwcq', 
                              'short_debtq', 'totdebt_invcapq']    
    
    elif data_type == 'gl':
        financial_features = ['aftret_eqq', 'aftret_equityq', 'at_turnq','bmq',
                              'capeiq', 'capital_ratioq', 'cash_conversionq',
                              'cash_debtq', 'cash_ltq', 'cash_ratioq', 'cfmq', 
                              'curr_debtq','curr_ratioq','debt_atq','de_ratioq',
                              'debt_capitalq','debt_ebitdaq', 'dltt_beq', 'dprq',
                              'efftaxq', 'evmq', 'fcf_ocfq', 'gpmq', 'GProfq',
                              'int_debtq', 'int_totdebtq', 'intcov_ratioq',
                              'intcovq', 'inv_turnq', 'invt_actq', 'lt_atq',
                              'lt_debtq', 'lt_ppentq', 'npmq', 'ocf_lctq',
                              'opmadq', 'opmbdq', 'pay_turnq', 'pcfq',
                              'pretret_earnatq', 'pretret_noq', 'profit_lctq',
                              'psq', 'ptbq', 'ptpmq', 'quick_ratioq','rect_actq',
                              'rect_turnq', 'roaq', 'roceq', 'roeq', 'sale_equityq',
                              'sale_nwcq', 'short_debtq', 'USD_mktcap', 'USD_price_adj']
        
    categorical_inst = ['Top10InstOwn','NumInstBlockOwners','InstBlockOwn','NumInstOwners',
     'MaxInstOwn','InstOwn','InstOwn_HHI','InstOwn_Perc', 'Top5InstOwn']
    
    categorical_analyst = ['Rec_Total','Rec_Median','Rec_Mean','Rec_Low','Rec_High','Rec_SBuy','Rec_Buy','Rec_Hold',
     'Rec_Sell','Rec_SSell','Rec_NoOpinion','LTG_Mean','Num_Analyst']
    
    macro_features = ['balance_on_trade_bop', 'business_confidence', 'cab%gdp', 'CPI_SA',
                         'central_government_deficit', 'consumer_confidence', 
                         'consumer_spending', 'current_account_balance', 
                         'export_goods_bop', 'export_prices', 'GDP',
                         'exports_goods_services', 'foreign_trade_balance', 
                         'government_bond', 'government_external_debt', 
                         'government_spending', 'gross_fixed_capital_investment', 
                         'import_prices', 'imports_goods_bop', 
                         'imports_goods_services', 'industrial_production', 
                         'international_reserves','labour_force_survey', 
                         'merchandise_exports', 'merchandise_imports', 'money_supply_M0', 
                         'money_supply_M1', 'money_supply_M2', 'overnight_rate', 'population', 
                         'retail_sales','stock_index', 'terms_of_trade', 
                         'unemployment_rate']
    
    macro_features_growth = [col for col in data if col.endswith('%')]
    macro_features_growth = [e for e in macro_features_growth if e not in \
                             ('money_supply_M3%','total_external_debt%')]
        
    data = data.replace([np.inf, -np.inf], np.nan)

    '''1.Median groupby fill for financial ratios groupby rating class'''
    if data_type == 'us':
        data[financial_features] = data[financial_features]\
            .fillna(data.groupby('rating_rank')[financial_features].transform('median'))
        #backfill by company if needed
        data[financial_features] = data[financial_features +['Instrument']].groupby('Instrument').\
            bfill()
    elif data_type == 'gl':
        data[financial_features] = data[financial_features]\
            .fillna(data.groupby(['rating_rank','loc_rank'])[financial_features].transform('median'))
        data[financial_features] = data[financial_features +['Instrument']].\
            groupby(['Instrument']).bfill()
    '''2.fill recommendation and inst with 0'''
    if data_type == 'us':
        inst_analyst = categorical_inst + categorical_analyst
        data[inst_analyst] = data[inst_analyst].fillna(0)
    elif data_type == 'gl':
        data[categorical_analyst] = data[categorical_analyst].fillna(0)
        
    '''3.macro and macro growth backfill with first value and then fill with 0'''
    if data_type == 'us':
        data[macro_features] = data[macro_features].bfill()
        data[macro_features] = data[macro_features].fillna(0)
        data[macro_features_growth] = data[macro_features_growth].bfill()
        data[macro_features_growth] = data[macro_features_growth].fillna(0)
    elif data_type == 'gl':
        data[macro_features] = data[macro_features +['loc_rank']].groupby('loc_rank').\
            bfill()
        data[macro_features] = data[macro_features].fillna(0)
        data[macro_features_growth] = data[macro_features_growth+['loc_rank']].\
            groupby('loc_rank').bfill()
        data[macro_features_growth] = data[macro_features_growth].fillna(0)
        
    #extra cols needed     
    if data_type == 'us':
        extra_cols = ['rating_rank','loc','gind','gsector','ggroup','nber_ri',
                      'fqtr']
        cols_use = extra_cols + financial_features + categorical_inst + \
        categorical_analyst + macro_features + macro_features_growth

    if data_type == 'gl':
        #fill OECD_ri with mode of developing nation at time t. e.g. AR with developing nations mode
        data['OECD_ri'] = data['OECD_ri']\
            .fillna(data.groupby(['developed_nation','publicdate'])['OECD_ri'].transform('median')) 
        #no obs for last qtr - forward fill previous qtr
        data['OECD_ri'] = data[['OECD_ri','developed_nation']].groupby('developed_nation').\
            ffill()
                                                 
        extra_cols = ['rating_rank','loc_rank','gind','gsector','ggroup','OECD_ri',
                      'developed_nation', 'fqtr']
        
        cols_use = extra_cols + financial_features + \
        categorical_analyst + macro_features + macro_features_growth
        
    '''4. ESG data'''
    if ESG=='ESG':
        data = data[data['ESG Combined Score']>0] # make sure no missing data
       
        '''4.ESG data'''
        if data_type == 'us':
            ESG_features = list(data.iloc[:,317:-1].columns) #cols of ESG features
            #encode three features in US sample
            encode = ['CO2 Estimation Method','Board Structure Type','ISO 14000 or EMS']
            le = LabelEncoder()
            for feat in encode:
                data[feat] = le.fit_transform(data[feat].astype(str))

        if data_type == 'gl':
            ESG_features = list(data.iloc[:,182:-1].columns) #cols of ESG features
            
        #non-missing % of ESG cols
        non_mis_ESG = pd.DataFrame(data[ESG_features].\
                                     isnull().sum()/len(data), 
                                     columns=['missing%'])
        
        '''Function to return the non-missing % cols matching <=5% criteria or >5%'''
        def ESG_cols(non_mis_ESG, value):
            if value == 0.05:
                non_mis_ESG = non_mis_ESG[(non_mis_ESG['missing%'] <= 0.05)]
            elif value == 0.85:
                non_mis_ESG = non_mis_ESG[(non_mis_ESG['missing%'] > 0.05) & (non_mis_ESG['missing%'] <= 0.85)]
            elif value == 0.86:
                non_mis_ESG = non_mis_ESG[(non_mis_ESG['missing%'] > 0.85)]
            non_mis_ESG = non_mis_ESG.T
            non_mis_ESG.sort_values(by=['missing%'], 
                                       ascending=True, axis=1, inplace=True)
            non_mis_ESG = list(non_mis_ESG.columns)
            describe_ESG = data[non_mis_ESG].describe()
            return non_mis_ESG, describe_ESG
        
        ESG_5cols,describe_ESG_5 = ESG_cols(non_mis_ESG, 0.05) #<= 5% missing data
        ESG_85cols,describe_ESG_85 = ESG_cols(non_mis_ESG, 0.85) #> 5% missing data
        ESG_drop_cols,describe_drop_cols = ESG_cols(non_mis_ESG, 0.86) #> 85% missing data remove
        output_cols = ESG_5cols + ESG_85cols
        cols_use = cols_use + output_cols
        data.drop(ESG_drop_cols, axis=1, inplace=True) # remove ESG columns that only have 15% data
        
        #binary ESG features for features <= 5% missing data with 0 if min 0 and max 0
        binary_ESG_5_0 = list(describe_ESG_5.columns[(describe_ESG_5.loc[describe_ESG_5.index == 'min']\
                    .values==0)[0] & (describe_ESG_5.loc[describe_ESG_5.index == 'max'].values==1)[0]])
        #binary ESG features for features <= 5% missing data with 0 if min and max both 0
        binary_ESG_5_0_0 = list(describe_ESG_5.columns[(describe_ESG_5.loc[describe_ESG_5.index == 'min']\
                    .values==0)[0] & (describe_ESG_5.loc[describe_ESG_5.index == 'max'].values==0)[0]])
        #binary ESG features for features <= 5% missing data with 1 if min and max both 1
        binary_ESG_5_1 = list(describe_ESG_5.columns[(describe_ESG_5.loc[describe_ESG_5.index == 'min']\
                    .values==1)[0] & (describe_ESG_5.loc[describe_ESG_5.index == 'max'].values==1)[0]])            
            
        binary_5_ESG = binary_ESG_5_0 + binary_ESG_5_0_0 +binary_ESG_5_1
        value_5_ESG = list(set(list(describe_ESG_5.columns)) - set(binary_5_ESG))
             
        #binary ESG features for features > 5% missing data & < 85% fill with if min 0 and max 1
        binary_ESG_85_0 = list(describe_ESG_85.columns[(describe_ESG_85.loc[describe_ESG_85.index == 'min']\
                    .values==0)[0] & (describe_ESG_85.loc[describe_ESG_85.index == 'max'].values==1)[0]])
        #binary ESG features for features > 5% missing data & < 85% fill with 0 min and max both 0
        binary_ESG_85_0_0 = list(describe_ESG_85.columns[(describe_ESG_85.loc[describe_ESG_85.index == 'min']\
                    .values==0)[0] & (describe_ESG_85.loc[describe_ESG_85.index == 'max'].values==0)[0]])
            #binary ESG features for features > 5% missing data & < 85% fill with 1 min and max both 1
        binary_ESG_85_1 = list(describe_ESG_85.columns[(describe_ESG_85.loc[describe_ESG_85.index == 'min']\
                    .values==1)[0] & (describe_ESG_85.loc[describe_ESG_85.index == 'max'].values==1)[0]])
        
        binary_85_ESG = binary_ESG_85_0 + binary_ESG_85_0_0 +binary_ESG_85_1
        value_85_ESG = list(set(list(describe_ESG_85.columns)) - set(binary_85_ESG))
        
        if data_type == 'us':
            #fill <=5% missing values with industry median
            data[value_5_ESG] = data[value_5_ESG]\
                .fillna(data.groupby(['gsector','loc'])[value_5_ESG].transform('median'))
                
        elif data_type == 'gl':
            #fill <=5% missing values with industry median
            data[value_5_ESG] = data[value_5_ESG]\
                .fillna(data.groupby(['gsector','loc_rank'])[value_5_ESG].transform('median'))        
        
        #fill binary missing with 0
        data[binary_5_ESG] = data[binary_5_ESG].fillna(0) 
                
        if data_type == 'us':
            #fill >5% missing values with industry min    
            data[value_85_ESG] = data[value_85_ESG]\
                .fillna(data.groupby(['gsector','loc'])[value_85_ESG].transform('min'))    

        elif data_type == 'gl':
            #fill >5% missing values with industry min    
            data[value_85_ESG] = data[value_85_ESG]\
                .fillna(data.groupby(['gsector','loc_rank'])[value_85_ESG].transform('min'))    
        
        #fill binary missing with 0
        data[binary_85_ESG] = data[binary_85_ESG].fillna(0) 
        
        data[output_cols] = data[output_cols].fillna(0)
        
        #df to use in modelling - without the unncessary columns
        remove_cols = ['Poison Pill Expiration Date','ESG Period Last Update Date',
        'Poison Pill Adoption Date','Environmental Innovation Score Grade',
        'ESG Combined Score Grade',	'CSR Strategy Score Grade','Shareholders Score Grade',
        'Management Score Grade', 'Product Responsibility Score Grade',
        'Community Score Grade','ESG Controversies Score Grade','Emissions Score Grade',
        'ESG Score Grade', 'Social Pillar Score Grade','Governance Pillar Score Grade',
        'Environment Pillar Score Grade','Workforce Score Grade','Human Rights Score Grade',
        'Resource Use Score Grade']
    
        cols_use = [x for x in cols_use if x not in remove_cols]    
        data_use = data[cols_use]
        
        if data_type == 'us':
            del data_use['loc'] # delete string location 
        
        return data, data_use

    else:
        #df to use in modelling - without the unncessary columns        
        remove_cols = ['Poison Pill Expiration Date','ESG Period Last Update Date',
        'Poison Pill Adoption Date','Environmental Innovation Score Grade',
        'ESG Combined Score Grade',	'CSR Strategy Score Grade','Shareholders Score Grade',
        'Management Score Grade', 'Product Responsibility Score Grade',
        'Community Score Grade','ESG Controversies Score Grade','Emissions Score Grade',
        'ESG Score Grade','Social Pillar Score Grade','Governance Pillar Score Grade',
        'Environment Pillar Score Grade','Workforce Score Grade','Human Rights Score Grade',
        'Resource Use Score Grade']
        
        cols_use = [x for x in cols_use if x not in remove_cols]    
        data_use = data[cols_use]
        
        if data_type == 'us':
            del data_use['loc'] # delete string location 
        
        return data, data_use

    
'''Address to ESG features and description'''
#http://zeerovery.nl/blogfiles/DataStream_ESG_Glossary_Extranet.xlsx

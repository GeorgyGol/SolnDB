"""Create local database from Internet BIS-data. Update database from Internet BIS-data
====================================================================
work with two BIS csv-files:
strCREDIT=r'https://www.bis.org/statistics/full_bis_total_credit_csv.zip'
strPRICES=r'https://www.bis.org/statistics/full_bis_selected_pp_csv.zip'
==================================================================== 
create_db - creates sqlite3 database file (param name - path to new db-file);

                read needed indicators list from csv-file (param 'indi_file' - path to file, file must have right format)
                and needed country list from txt-file (param 'country_file' - path to file, file must have right format)

                function create two service table: INDICATORS with indicators list for next update db, 
                and COUNTRIES with list of needed countries.
                
                then create data-tables, one for each indicator, and run 'update' function for read all
                needed indicators for all needed countries from IMF web source started from 1970 year 
                
                then create INDICATORS_FULL view for reading all data in database at once
====================================================================
update_db - read list of needed indicators and countries from existing BIS-database (param 'db_name' - path to sqlite3-file),
            then read BIS-csv-files from Internet started from year in 'start' param, 
            and save it in given sqlite3 database, using UPSERT (append new data and update existing)
            return all readed data in one pandas DataFrame
====================================================================
 """

import pandas as pd
import datetime as dt
import COMMON.readers as cmm
from COMMON import pandas_sql as pds
import sqlite3

import sqlalchemy as sa
import sys
import os
import numpy as np

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

strMainBIS = r'https://www.bis.org/statistics/full_data_sets.htm'

bis_indis = ['PPRICES', 'CREDIT', 'BROAD_REAL', 'CBRPOL',  'CREDIT_NON_FIN', 'USD_ESCH', 'DEBT_SERV_NF']

db_indicators=cmm.work_db_BIS

def create_db(name=db_indicators, indi_file=os.path.join('Source', 'codes_need.csv'),
              country_file=os.path.join('Source', 'work_countries.txt')):

    pdfCSV = cmm.read_indicators_from_csv(indi_file)
    country_list = cmm.read_countries(file_name=country_file)

    return _update_db(db_name=name, pdfCSV_Indi=pdfCSV, pdfCntry=country_list, start=0)


def update_db(db_name=db_indicators, start=2015, write_db=True, keys=bis_indis,
               end=dt.datetime.now().year):
    coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
    pdfDB_indi=pd.read_sql('SELECT * FROM {0}'.format(cmm.strINDI_db_name), con=coni, index_col='Code')
    pdfDB_cntry=pd.read_sql('SELECT * FROM {0}'.format(cmm.strCOUNTRY_db_name), con=coni, index_col='id')

    return _update_db(db_name=db_name, pdfCSV_Indi=pdfDB_indi, pdfCntry=pdfDB_cntry,
                      start=start, end=end, write_db=write_db, keys=keys)


def _update_db(db_name=db_indicators, start=1950, write_db=True, keys=bis_indis,
               end=dt.datetime.now().year, pdfCSV_Indi=None, pdfCntry=None):
    def print_mess(strMes, end='\n'):
        print('UPDATE BIS: ', strMes, end=end)

    bis_vals = list()
    #0 - vals, 1 - cntry, 2 - indi

    for indi in bis_indis:
        print_mess('read {indi}...'.format(indi=indi), end='')
        rd_bis=pds.read_bis(indiTYPE=indi, get_countries=True)
        bis_vals.append(rd_bis)
        print('done reading {indi_list}, {val_cnt} vals'.format(indi_list=rd_bis[2].index.tolist(), val_cnt=rd_bis[0].shape[0]))

    print_mess('merging and filtering data...', end='' )
    pdfVals = pd.concat([item[0] for item in bis_vals], ignore_index=True)
    pdfCntr = pd.concat([item[1] for item in bis_vals], ignore_index=False)
    pdfIndi = pd.concat([item[2] for item in bis_vals], ignore_index=False)

    if type(pdfCntry)==type(list()):
        country_list = pdfCntry
    else:
        country_list = pdfCntry.index.tolist()

    pdfIndi=pdfIndi.drop_duplicates().loc[pdfCSV_Indi.index]
    pdfIndi['LastUpdateDate'] = dt.datetime.now().strftime('%Y-%m-%d')
    pdfIndi['LastUpdateDate'] = pdfIndi['LastUpdateDate'].astype(str)
    pdfCntr=pdfCntr.drop_duplicates().loc[country_list]
    pdfVals=pdfVals.loc[pdfVals['country'].isin(pdfCntr.index) & pdfVals['indi'].isin(pdfIndi.index)]
    print('done')

    if write_db:
        coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))

        print_mess('writing data to DB for {} indicators...'.format(pdfIndi.shape[0]))
        for c, v in  pdfIndi.iterrows():
            print('\tfor {}: calculating fields...'.format(c), end='')
            pdf=pds.DataFrameDATA(pdfVals.loc[pdfVals['indi']==c])
            pdf['time_dop'] = pdf['time'].apply(lambda x: x.split('-')[-1].replace('Q', '')).astype(int)
            pdf['time'] = pdf['time'].apply(lambda x: x.split('-')[0]).astype(int)
            pdf['id'] = pdf[['country', 'time', 'time_dop']].apply(cmm.get_hash, axis=1)
            pdf.set_index('id', inplace=True)
            pdf=pdf.loc[pdf['time'].between(start, end)]
            print('done... ', end='')
            print('writing {0}: {1} points, start year = {2}'.format(c, pdf.shape[0], pdf['time'].min()), end='...')
            pdfIndi.loc[c, 'LastUpdateCount']=pdf.shape[0]
            pdfIndi.loc[c, 'Start'] = pdf['time'].min()
            pdf.to_sql(c, con=coni, if_exists='upsert')
            print('done')
    print('='*40)
    print_mess('updating {} table'.format(cmm.strINDI_db_name), end=' ... ')
    pdfIndi.to_sql(cmm.strINDI_db_name, con=coni, if_exists='replace')
    print('done')
    print('=' * 40)
    print_mess('updating {} table'.format(cmm.strCOUNTRY_db_name), end=' ... ')
    pdfCntr.to_sql(cmm.strCOUNTRY_db_name, con=coni, if_exists='replace')
    print('done')
    print('=' * 40)
    print_mess('Creating viewa...', end=' ... ')
    cmm.create_views(db_name)
    print('=' * 40)
    print_mess('All done.')
    return pdfVals, pdfIndi, pdfCntr

if __name__ == "__main__":
    #print(get_countryes())
    create_db()
    #update_db(write_db=True)
    cmm.create_views(db_name=db_indicators)
    print('all done')
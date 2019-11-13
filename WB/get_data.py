"""Create local database from Internet World Bank-data. Update database from Internet World Bank-data
====================================================================
work with pandas_datareader (COMMON module of this project)
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

def create_db(name=cmm.work_db_WB, indi_file=os.path.join('Source', 'codes_need.csv'),
              country_file=os.path.join('Source', 'work_countries.txt')):

    pdfCSV = cmm.read_indicators_from_csv(indi_file)
    country_list = cmm.read_countries(file_name=country_file)

    pdfQ = pdfCSV[pdfCSV['Freq'] == 'Q']
    pdfM = pdfCSV[pdfCSV['Freq'] == 'M']

    nameM = cmm.db_name2annu(name, suff='_M')

    dbw=True
    if pdfQ.shape[0]>0:
        pdf_ret, pdf_cntr, pdf_indis = _update_db(db_name=name, pdfCSV_Indi=pdfQ, pdfCntry=country_list, write_db=dbw)
        if dbw: cmm.create_views(name)
    if pdfM.shape[0]>0:
        pdf_retM, pdf_cntrM, pdf_indisM = _update_db(db_name=nameM, pdfCSV_Indi=pdfM, frequency='M',
                                                     pdfCntry=country_list, write_db=dbw)
        if dbw: cmm.create_views(nameM, freq='M')

    print('Done create DB for World Bank')
    #return pdf_ret, pdf_cntr, pdf_indis

def update_db(db_name=cmm.work_db_WB, start=2015, write_db=True, end=dt.datetime.now().year):
    coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
    pdfDB_indi=pd.read_sql('SELECT * FROM {0}'.format(cmm.strINDI_db_name), con=coni, index_col='Code')
    pdfDB_cntry=pd.read_sql('SELECT * FROM {0}'.format(cmm.strCOUNTRY_db_name), con=coni, index_col='id')

    pdfDB_indi.index=pdfDB_indi.index.str.replace('_', '.')

    pdf_ret, pdf_cntr, pdf_indis = _update_db(db_name=db_name, pdfCSV_Indi=pdfDB_indi, write_db=write_db,
                                              frequency=pdfDB_indi['Freq'].unique()[0],
                                              pdfCntry=pdfDB_cntry.index.tolist(), start=start, end=end)

    print('Done update DB for World Bank')
    return pdf_ret, pdf_cntr, pdf_indis


def _update_db(db_name=cmm.work_db_WB, start=1950, write_db=True, keys='', frequency='Q',
               end=dt.datetime.now().year, pdfCSV_Indi=None, pdfCntry=None):
    def print_mess(strMes, end='\n'):
        if frequency=='Q':
            print('UPDATE WORLD BANK QUOTERLY: ', strMes, end=end)
        if frequency == 'M':
            print('UPDATE WORLD BANK MONTHLY: ', strMes, end=end)

    dct_db = dict()
    print_mess('reading indicators...')
    for k, v in pdfCSV_Indi.iterrows():
        print('\tindicator {0} for {1} countries...'.format(k, len(pdfCntry)), end='')
        dct_db.setdefault(k, pds.read_worldbank(symbol=k, countries=pdfCntry, get_countries=True,
                                                start=start, end=end, freq=frequency))
        print('ok for {} records'.format(dct_db[k][0].shape[0]))
    print_mess('+'*50)
    print_mess('merging data...', end='')
    pdf_indis = pd.concat([v[2] for _, v in dct_db.items()]).reset_index(drop=True)
    pdf_indis['Code'] = pdf_indis['Code'].str.replace('\.', '_')
    pdf_indis['Freq']=frequency
    pdf_indis = pdf_indis.set_index('Code')


    pdf_cntr = pd.concat([v[1] for _, v in dct_db.items()]).drop_duplicates()
    pdf_ret = pd.concat([v[0] for _, v in dct_db.items()]).drop_duplicates()

    print('done')
    if write_db:
        print_mess('+' * 50)
        print_mess('writing to db...')
        coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
        for k, v in dct_db.items():
            print('\t for ', k, end='...')
            v[0][['country', 'time', 'time_dop', 'value']].to_sql(k.replace('.', '_'), con=coni, if_exists='upsert')
            print('ok for {} records'.format(v[0].shape[0]))

        print_mess('+' * 50)
        print_mess('writing countries and indicators to db...', end='')
        pdf_indis.to_sql(cmm.strINDI_db_name, con=coni, if_exists='replace')
        pdf_cntr.to_sql(cmm.strCOUNTRY_db_name, con=coni, if_exists='replace')
        print('done')

    return pdf_ret, pdf_cntr, pdf_indis

def create_db_struct(db_name='WB_STRUCT.sqlite3'):
    print('READ WB DATABASE STRUCT...', end='')
    pdf_i, pdf_c=pds.read_worldbank_struct()
    print('done')
    coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
    print('WRITE WB STRUCT TO LOCAL DB...', end='')
    pdf_i.to_sql('WB_INDI', con=coni, index=False, if_exists='replace')
    pdf_c.to_sql('WB_CNTRY', con=coni, index=False, if_exists='replace')
    print('done')
    return pdf_i, pdf_c

if __name__ == "__main__":
    create_db_struct()
    #print(get_countryes())
    #create_db()
    #update_db(db_name=cmm.db_name2annu(cmm.work_db_WB, suff='_M'))
    #cmm.create_views(db_name=cmm.work_db_WB)
    print('all done')
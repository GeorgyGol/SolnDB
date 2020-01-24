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
from pandas_datareader import wb as pddr

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

    def get_indi(symbols, ret):
        db_indi = pddr.get_indicators()

        # db_indi.to_csv('wb_indi.csv', sep=';')

        db_indi = db_indi.loc[db_indi['id'].isin(symbols)].rename(
            columns={'id': 'Code', 'name': 'Name', 'source': 'Dataset'})
        db_indi['Freq'] = frequency
        db_indi['Start'] = ret['time'].min()
        db_indi['LastUpdateDate'] = dt.datetime.now()
        db_indi['LastUpdateCount'] = ret.shape[0]
        db_indi['MULT'] = 0
        db_indi['Code'] = db_indi['Code'].str.replace('\.', '_')
        db_indi.set_index('Code')
        return db_indi

    def get_countries(ret):
        db_c = pddr.get_countries()[['iso2c', 'name']].rename(columns={'iso2c':'id', 'name':'Country'}).set_index('id')
        return db_c

    print_mess('reading indicators...')
    pdfl=[]
    for k, v in pdfCSV_Indi.iterrows():
        for c in cmm.iterate_group(pdfCntry, 10):
            print('\tindicator {0} for {1} countries...'.format(k, c), end='')
            try:
                pdfl.append(pds.read_worldbank(symbol=k, countries=c, get_countries=False,
                                           start=start, end=end, freq=frequency))
                print('done')
            except ValueError:
                print('no data')

        dct_db.setdefault(k, pd.concat(pdfl))
        print('ok for {} records'.format(dct_db[k].shape[0]))
    print_mess('+'*50)
    print_mess('merging data...', end='')
    pdf_ret = pd.concat([v for _, v in dct_db.items()]).drop_duplicates()

    pdf_indis = get_indi(pdfCSV_Indi.index.tolist(), pdf_ret)

    pdf_cntr = get_countries(pdf_ret)

    print('done')
    if write_db:
        print_mess('+' * 50)
        print_mess('writing to db...')
        coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
        for k, v in dct_db.items():
            print('\t for ', k, end='...')
            v[['country', 'time', 'time_dop', 'value']].to_sql(k.replace('.', '_'), con=coni, if_exists='upsert')
            print('ok for {} records'.format(v.shape[0]))

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
    # #i, c = create_db_struct()
    # cr=cmm.read_countries(file_name=os.path.join('Source', 'work_countries.txt'))
    #
    # for c in cmm.iterate_group(cr, 10):
    #     print(c)
    # lst_wb=[pddr.WorldBankReader(symbols='TOT', countries=r, start=1980, end=2019, freq='M').read().dropna() for r in cmm.iterate_group(cr, 10)]
    # pdd=pd.concat(lst_wb)
    #
    # # pdf=pds.read_worldbank(symbol='TOT', countries='all', get_countries=True,
    # #                    start=1980, end=2019, freq='M')[0]
    # # print(pdf)
    # # print(pdf['country'].unique().tolist())
    # # print(len(pdf['country'].unique().tolist()))
    # #
    # # pdd=pddr.WorldBankReader(symbols='TOT', countries=cr, start=1980, end=2019, freq='M').read().dropna()
    # print(pdd)
    # print(pdd.reset_index()['country'].unique().tolist())

    create_db()
    #update_db(db_name=cmm.db_name2annu(cmm.work_db_WB, suff='_M'))
    #cmm.create_views(db_name=cmm.work_db_WB)
    print('all done')
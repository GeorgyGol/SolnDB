"""Create Database for IMF-data. Update Database with IMF-data
====================================================================
create_db - creates sqlite3 database file (param name - path to new db-file);
                it reads needed indicators list from csv-file (param 'indi_file' - path to file, file must have right format)
                and needed country list from txt-file (param 'country_file' - path to file, file must have right format)

                function create two service table: INDICATORS with indicators list for next update db, 
                and COUNTRIES with list of needed countries.

                then create INDICATORS_FULL view for reading all data in database at once

                then run 'update' function for read all needed indicators for all needed countries from IMF web source
                started from 1970 year
====================================================================
update_db - read list of needed indicators and countries from existing IMF-database (param 'db_name' - path to sqlite3-file),
            then read IMF-database from Internet started from year in 'start' param, 
            and save it in given sqlite3 database, using UPSERT (append new data and update existing)
            return all readed data in one pandas DataFrame
====================================================================
get_countryes - read countries list from IMF-database on Internet,
                list of datasets read from existing sqlite3 database (param 'db_name' - path)
                list of needed ountries read from txt-file (param 'country_txt_file' - path)
                make and return pandas DataFrame with intersection countries lists from Internet and local file, 
                not save results in sqlite3 file
 """

import os.path
import pandas as pd
import sqlalchemy as sa
import datetime as dt

from COMMON import pandas_sql as pds
from COMMON import readers as cmm

_db_indicators=cmm.work_db_IMF

def update_db(db_name=_db_indicators, start=1950, end=dt.datetime.now().year, write_db=True):
    """update existing sqlite3 local database with data readed from IMF Internet database"""
    def read_indicators(pdfI=None, freq='Q', coutries=[], ctry_chunksize=50, write_db=True):
        print('UPDATE IMF: Start reading {0} indicators'.format(pdfI.shape[0]))
        #dct_not_data=dict()
        lst_ret=[]
        for k, v in pdfI.iterrows():

            lst_pdf=list()
            lst_not_country=list()
            tbl_name=k #'{0}_{1}'.format(k, freq)
            print('UPDATE IMF ({2}-{3}): reading {0}, tDS={1}\t'.format(k, v['Dataset'], start, end), end='... ')
            for cs in cmm.iterate_group(coutries, ctry_chunksize):

                try:
                    pdf = pds.read_imf(strDataSetID=v['Dataset'], indiID=k, countryCode=cs,
                                       frequency=freq, startDate=start, endDate=end)

                    lst_pdf.append(pdf)
                    lst_not_country+=pdf.not_country
                    #print(pdf.name, pdf.shape, len(pdf.not_country))
                except ValueError as e:
                    lst_not_country += cs

                    #print(e, k, 0, 50)
            try:
                pdfC=pds.DataFrameDATA(pd.concat([ppdf for ppdf in lst_pdf if not ppdf.empty]))
                pdfC.name=tbl_name
                #dct_not_data.update({'IND_NOT':tbl_name, 'NOT_DATA':lst_not_country})
                print('read {name},\tlen {len_df},\tnot data countries - {nc}'.format(name=pdfC.name,
                                                                                    len_df=pdfC.shape[0],
                                                                                    nc=len(lst_not_country)), end='... ')
                if write_db:
                    print('write to DB...', end='')

                    lstWrite=[c for c in pdfC.columns.tolist() if c !='mult']

                    pdfC[lstWrite].to_sql(pdfC.name, coni, if_exists='upsert')
                    cmm.write_status(db_name, k, pdfC.shape[0], mult=pdfC['mult'].unique()[0])

                print('done', end='\n')
                pdfC['INDI']=k
                lst_ret.append(pdfC)
                #print(dct_not_data)
            except ValueError as e:
                print(e, 'not data for ', k, v['Dataset'], len(cs))

        return pd.concat(lst_ret)

    coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
    # pdfIndi=pd.read_sql('select * from INDICATORS where LastUpdateDateA is NULL', coni, index_col='Code')
    pdfIndi = pd.read_sql('select * from {INDI_NAME}'.format(INDI_NAME=cmm.strINDI_db_name), coni, index_col='Code')#.iloc[:40]
    pdfCountry = pd.read_sql('select * from {COUNTRY_NAME}'.format(COUNTRY_NAME=cmm.strCOUNTRY_db_name), coni, index_col='id')
    country_list = pdfCountry.index.tolist()
    print('UPDATE IMF: reading {0} countries'.format(len(country_list)))

    pdfQ=read_indicators(pdfI=pdfIndi.sort_index(), coutries=country_list, write_db=write_db)
    print('=' * 50)

    print('UPDATE IMF: all done')
    return pdfQ

def create_db(name=_db_indicators,
              indi_file=os.path.join('Source', 'codes_need.csv'),
              country_file=os.path.join('Source', 'work_countries.txt')):
    """ Create local sqlite3 database file with data readed from IMF Internet database """
    pdf = cmm.read_indicators_from_csv(indi_file)
    coni = sa.create_engine('sqlite+pysqlite:///{name}'.format(name=name))

    pdf.to_sql(cmm.strINDI_db_name, coni, if_exists='replace')
    print('CREATE IMF.INDICATORS table for {} indicators'.format( pdf.shape[0]))

    pdfC=get_countryes(db_name=name, country_txt_file=country_file)
    pdfC.to_sql(cmm.strCOUNTRY_db_name, con=coni, if_exists='replace')
    print('CREATE IMF.COUNTRIES for {0} countries.'.format(pdfC.shape[0]))

    update_db(db_name=name, start=1970, end=2000)
    update_db(db_name=name, start=1999)

    cmm.create_views(name)


def get_countryes(db_name=_db_indicators, country_txt_file=os.path.join('Source', 'work_countries.txt')):
    """ Read and return counties list as two-chars code <-> Country's name from IMF Internet database"""
    imf = cmm.READ_DB(db_name=None)
    country_list = cmm.read_countries(file_name=country_txt_file)
    print('CREATE IMF: reading countries from all neede datasets...', end=' ')
    coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))
    dbSETS=pd.read_sql('SELECT DISTINCT Dataset from {INDI_NAME}'.format(INDI_NAME=cmm.strINDI_db_name), con=coni)
    pdfC = pd.concat([pd.DataFrame(imf.get_datastructure_list(d['Dataset'])['Geographical Areas']).set_index('CL_AREA_{}'.format(d['Dataset'])) for k, d in dbSETS.iterrows() ])
    pdfC=pdfC[pdfC.index.isin(country_list)]
    pdfC = pdfC[~pdfC.index.duplicated()]
    pdfC.index.name='id'
    pdfC=pdfC.rename(columns={'Geographical Areas':'Country'})
    print('done reading countries', end='\n')
    return pdfC


    #print(dbSETS)


import sqlite3
if __name__ == "__main__":
    #create_db(name=_db_indicators)
    con=sqlite3.connect(_db_indicators)
    #pdfT=pd.read_sql('select * from INDICATORS_FULL', index_col='id', con=con)

    #update_db(db_name=_db_indicators, start=2010)
    pdfTU = pd.read_sql('select * from INDICATORS_FULL', index_col='id', con=con)

    #print('before {0}, after {1}'.format(pdfT.shape[0], pdfTU.shape[0]))

    print(pdfTU.loc[pdfTU.index.duplicated(), :])
    #print(update_db(db_name=db_indicators))
    #cmm.create_views(db_name=db_indicators)
    #print(get_countryes())
    #create_views(db_name=db_indicators, freq='Q')



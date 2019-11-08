import pandas as pd
import datetime as dt
import COMMON.readers as cmm
from COMMON import pandas_sql as pds
import sqlalchemy as sa
import sys, os

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

_db_indicators=cmm.work_db_OECD
_start_year=1970

def create_db(name=_db_indicators,
              indi_file=os.path.join('Source', 'codes_need.csv'),
              country_file=os.path.join('Source', 'work_countries.txt')):

    def db_freq(pdfI, countries, con, dbname, freq, mess):
        if pdfI.shape[0]==0:
            return
        print('='*40, 'CREATE DB OECD FOR {}:'.format(mess), '='*40)

        pdfI.to_sql(cmm.strINDI_db_name, con, if_exists='replace')

        print('CREATE OECD.INDICATORS table for {} indicators'.format(pdfI.shape[0]))
        years = [y for y in range(_start_year, dt.datetime.now().year, 10)]
        lst_cntr = []
        for y in years:
            print('CREATE OECD: for {0} to {1}'.format(y, y + 10))
            _, cntr = update_db(db_name=dbname, start=y, end=y + 10, get_countries=True, countries_list=cntr_list, freq=freq)
            lst_cntr.append(cntr)
            print('=' * 50)
        pd_cntr = pd.concat(lst_cntr)
        pd_cntr = pd_cntr.loc[~pd_cntr.index.duplicated(), :]
        pd_cntr = pd_cntr.loc[pd_cntr.index.isin(countries)]
        pd_cntr.to_sql(cmm.strCOUNTRY_db_name, coni, if_exists='replace')
        print('CREATE OECD.COUNTRIES table for {} indicators'.format(pd_cntr.shape[0]))
        cmm.create_views(db_name=dbname, freq=freq)

    pdf = cmm.read_indicators_from_csv(indi_file)
    coni = sa.create_engine('sqlite+pysqlite:///{name}'.format(name=name))
    cntr_list=cmm.read_countries(country_file)

    pdfQ = pdf[pdf['Freq'] == 'Q']
    pdfA = pdf[pdf['Freq'] == 'Y']
    pdfM = pdf[pdf['Freq'] == 'M']

    nameA = cmm.db_name2annu(name)
    nameM = cmm.db_name2annu(name, suff='_M')

    coni = sa.create_engine('sqlite+pysqlite:///{name}'.format(name=name))
    coniA = sa.create_engine('sqlite+pysqlite:///{name}'.format(name=nameA))
    coniM = sa.create_engine('sqlite+pysqlite:///{name}'.format(name=nameM))

    db_freq(pdfM, cntr_list, coniM, nameM, 'M', 'MONTHLY')
    db_freq(pdfQ, cntr_list, coni, name, 'Q', 'QUOTERLY')
    db_freq(pdfA, cntr_list, coniA, nameA, 'Y', 'YEARLY')

    # pdf.to_sql(cmm.strINDI_db_name, coni, if_exists='replace')
    #
    # print('CREATE OECD.INDICATORS table for {} indicators'.format( pdf.shape[0]))
    # years=[y for y in range(_start_year, dt.datetime.now().year, 10)]
    # lst_cntr=[]
    # for y in years:
    #     print('CREATE OECD: for {0} to {1}'.format(y, y+10))
    #     _, cntr=update_db(db_name=name, start=y, end=y+10, get_countries=True, countries_list=cntr_list)
    #     lst_cntr.append(cntr)
    #     print('='*50)
    # pd_cntr=pd.concat(lst_cntr)
    # pd_cntr=pd_cntr.loc[~pd_cntr.index.duplicated(), :]
    # pd_cntr = pd_cntr.loc[pd_cntr.index.isin(cntr_list)]
    # pd_cntr.to_sql(cmm.strCOUNTRY_db_name, coni, if_exists='replace')
    # print('CREATE OECD.COUNTRIES table for {} indicators'.format(pd_cntr.shape[0]))
    # cmm.create_views(db_name=name)

def update_db(db_name=_db_indicators, start=_start_year, write_db=True,
              end=dt.datetime.now().year, get_countries=False, countries_list=None, freq='Q'):

    coni = sa.create_engine('sqlite+pysqlite:///{db_name}'.format(db_name=db_name))

    if countries_list is None:
        pdfCountry = pd.read_sql('select * from {COUNTRY_NAME}'.format(COUNTRY_NAME=cmm.strCOUNTRY_db_name), coni,
                                 index_col='id')
        cntry = pdfCountry.index.tolist()
    else:
        cntry=countries_list

    #cntry = ['USA', 'RUS']

    pdfIndi = pd.read_sql('select * from {INDI_NAME}'.format(INDI_NAME=cmm.strINDI_db_name), coni, index_col='Code')  # .iloc[:40]

#    frq=pdfIndi['Freq'].unique().tolist()[0]

    ret_list=[]
    ret_list_c=[]

    for code, v in pdfIndi.iterrows():
        print('UPDATE OECD: read {} from OECD'.format(code), end=' ... ')
        if get_countries:
            pdf, p_cntr=pds.read_oecd(countryCode=cntry, indiID=code, startDate=start, strDataSetID=v['Dataset'],
                                      endDate=end, get_countries=get_countries, frequency=v['Freq'])
        else:
            pdf = pds.read_oecd(countryCode=cntry, indiID=code, startDate=start, endDate=end, strDataSetID=v['Dataset'],
                                        get_countries=get_countries, frequency=v['Freq'])
        print('ok for {}'.format(pdf.shape[0]), end=' ... ')

        if write_db:
            print('write to db ... ', end='')
            lstWrite=[c for c in pdf.columns.tolist() if c !='mult']
            pdf[lstWrite].to_sql(pdf.name, coni, if_exists='upsert')
            cmm.write_status(db_name, code, pdf.shape[0], mult=pdf['mult'].unique()[0])
            print('done write', end =' ... ')
        pdf['indi']=code
        ret_list.append(pdf)
        if get_countries:
            ret_list_c.append(p_cntr)

        print('done', end='\n')
    if get_countries:
        pdfCNTR = pd.concat(ret_list_c).sort_values(by='name').set_index('id').rename(columns={'name':'Country'})
        return pd.concat(ret_list), pdfCNTR.loc[~pdfCNTR.index.duplicated(), :]
    else:
        return pd.concat(ret_list)

if __name__ == "__main__":
    create_db()

    #p = update_db(write_db=True, start=1950)
    #print(p)
    #print(p[~p.isin(pch.index)].dropna())
    #check()
    #cmm.create_views(db_name=db_indicators)


    #print(pdfCNTR)
    print('all done')
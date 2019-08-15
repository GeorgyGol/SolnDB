import pandas as pd
import sqlite3
import requests
import os
import json

import COMMON.readers as cmm
from IMF import IMF_get_data
#'http://datahelp.imf.org/knowledgebase/articles/667681-using-json-restful-web-service' - help link

cat_ds_Struct='JS_DS_STRUST'
work_db='IMF.sqlite'

class IMF_DB:
    request_headers = {'Accept-Language': 'ru,en-US;q=0.8,en;q=0.6,sr;q=0.4,hr;q=0.2',
                       'Connection': 'keep-alive',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 5.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36}'
                       }

    _strListDS = r'http://dataservices.imf.org/REST/SDMX_JSON.svc/Dataflow'  # list of datasets
    _strStructDS = r'http://dataservices.imf.org/REST/SDMX_JSON.svc/DataStructure/{dataset}' # strauct, metadata for dataset
    _strIndi=r'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/{DATASET}/{FREQ}.{CONTRY_CODE}.{INDI}.?startPeriod={START_DT}&endPeriod={END_DT}'
    _strCodeList=r'http://dataservices.imf.org/REST/SDMX_JSON.svc/CodeList/{codelist_code}_{databaseID}'

    _sess=None
    _conn=None

    def __init__(self, db_name=work_db):
        self._sess=requests.session()
        if db_name:
            self._conn=sqlite3.connect(db_name)

    def get_datasets_list(self, save_db=False):
        dct_ds=cmm.get_json(self._sess, self._strListDS) #self._get_json(self._strListDS)

        #print(json.dumps(dct_ds, indent=4, sort_keys=True))

        lst_ds=[dict(code_ds=df['KeyFamilyRef']['KeyFamilyID'],
                     name=df['Name']['#text'],
                     agency=df['KeyFamilyRef']['KeyFamilyAgencyID'],
                     lang=df['Name']['@xml:lang']) for df in dct_ds['Structure']['Dataflows']['Dataflow']]
        pdf=pd.DataFrame(lst_ds).set_index('code_ds')
        if save_db:
            pdf.to_sql('DATASETS', con=self._conn, if_exists='replace')
        return pdf

    def get_datastructure_list(self, ds_code, save=True):
        '''Get structre info for selected dataset'''
        '''
            dct_struct=imf.get_datastructure_list(ds_code='IFS')
            print(list(dct_struct.keys()))
            print(dct_struct['Geographical Areas'])
        '''
        dct_struct=cmm.get_json(self._sess, self._strStructDS.format(dataset=ds_code))
        dct_ret=dict()

        for d in dct_struct['Structure']['CodeLists']['CodeList']:
            #print(d['Name']['#text'], d['@id'])

            code=[(c['Description']['#text'], c['@value']) for c in d['Code']]
            pdf = pd.DataFrame(code, columns=[d['Name']['#text'], d['@id']])
            dct_ret.update({d['Name']['#text']:pdf})
            #print(code)

        return dct_ret

    @property
    def connection(self):
        return self._conn
    @property
    def session(self):
        return self._sess

    def get_full_struct(self, save=True):
        dts = self.get_datasets_list()
        indi_struct_list = []
        ds_not_data = []
        for k, v in dts.iterrows():
            try:
                indi = tst.get_datastructure_list(k, save=False)['Indicator'].rename(
                    columns={'CL_INDICATOR_' + k: 'CODE',
                             'Indicator': 'DESCRIPTION', 'CL_Indicator_'+k:'CODE'})
                indi['DATASET'] = k
                indi_struct_list.append(indi)
                print('FOR {ds} READ {cnt} INDICATORS OK'.format(ds=k, cnt=indi.shape[0]))
                # print(indi.head(10))
            except:
                print('for {code}: {name} - not data'.format(code=k, name=v['name']))
                ds_not_data.append(k)
        indi_full=pd.concat(indi_struct_list)
        if save:
            indi_full.to_sql('DS_INDI_FULL', con=self._conn, if_exists='replace', index=False)
        return indi_full, ds_not_data

    def get_full_countries(self, save=True):
        ds = self.get_datasets_list()
        cntr_list = []
        for k, v in ds.iterrows():
            try:
                area = self.get_datastructure_list(k, save=False)['Geographical Areas'].rename(
                    columns={'CL_AREA_' + k: 'CODE'})
                area.set_index('CODE', inplace=True)
                cntr_list.append(area)
                print('FOR {dataset} READ {cntr} COUNTRIES'.format(dataset=k, cntr=area.shape[0]))
            except:
                print('FOR {} NOT COUNTIES'.format(k))
        countries = pd.concat(cntr_list)
        countries = countries.loc[~countries.index.duplicated(), :]
        if save:
            countries.to_sql('COUNTRIES', con=self._conn, if_exists='replace')
        return countries


    def _check_cat(self, strCat, create=True):
        if not os.path.exists(strCat):
            if create:
                os.makedirs(strCat)
            else:
                return False
        return True

    def get_code_list(self, dataset, codelist):
        '''GetCodeList method returns the description of CodeLists
        In order to obtain the data use the following request:'''

        dct_ds = self._get_json(self._strCodeList.format(databaseID=dataset, codelist_code=codelist))

        print(json.dumps(dct_ds, indent=4, sort_keys=True))

    def __delete__(self, instance):
        self._sess.close()
        self._conn.close()

def main():
    print('all done')

if __name__ == "__main__":
    tst=IMF_DB(db_name='IMF_STRUCT.sqlite3')
    area=tst.get_full_countries()
    print(area)
    print(area.shape)
    print('All done')
    #
    #print(indi)
    #print(indi.columns.tolist())
import datetime as dt
import hashlib
import json
import xml.etree.ElementTree as ET
import os
import os.path
import sqlite3
import time
import pandas as pd
import requests


work_db_OECD = 'OECD.sqlite3'
work_db_BIS = 'BIS.sqlite3'
work_db_IMF  = 'IMF1.sqlite3'
work_db_WB  = 'WB.sqlite3'

strINDI_db_name='INDICATORS'
strCOUNTRY_db_name='COUNTRIES'

work_fields=['id', 'country', 'time', 'time_dop', 'value']
indi_field='INDI'

strQueryINDI_tables = "select name from sqlite_master where type = 'table' and name NOT IN ('{INDI_NAME}', '{COUNTRY_NAME}')".format(
            INDI_NAME=strINDI_db_name, COUNTRY_NAME=strCOUNTRY_db_name)

def print_json(str_json):
    print(json.dumps(str_json, indent=4, sort_keys=True))


def iterate_group(iterator, count):
    itr = iter(iterator)
    for i in range(0, len(iterator), count):
        yield iterator[i:i + count]


def get_hash(x):
    m = hashlib.md5()
    strHash = ''.join([str(i) for i in x])
    m.update(bytearray(strHash.encode('utf-8')))
    return m.hexdigest()


request_headers = {'Accept-Language': 'ru,en-US;q=0.8,en;q=0.6,sr;q=0.4,hr;q=0.2',
                       'Connection': 'keep-alive',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 5.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36}'
                       }


def get_json(session, strURL):
    try:
        strRet = session.get(strURL, headers=request_headers).json()
    except:
        print('ERROR READING - TRYING RE-GET AFTER 5 SEC')
        print(strURL)
        time.sleep(5)
        try:
            strRet = session.get(strURL, headers=request_headers).json()
        except:
            raise ValueError('FATAL ERROR READING! - ' + strURL)
    return strRet

def read_countries(file_name=os.path.join('Source', 'work_countries.txt')):
    with open(file_name, 'r') as country_f:
        cntry = country_f.read().split('+')

        return cntry

def read_indicators_from_csv(file):
    pdf = pd.read_csv(file, encoding='cp1251', sep=';')
    return pdf.loc[~pdf.duplicated(subset=['Code', 'Freq'], keep='first') & pdf.index.notnull()].set_index('Code')

class READ_DB:

    imfURLS={'strListDS': r'http://dataservices.imf.org/REST/SDMX_JSON.svc/Dataflow',
             'strStructDS': r'http://dataservices.imf.org/REST/SDMX_JSON.svc/DataStructure/{dataset}',
             'strIndi': 'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/{DATASET}/{FREQ}.{CONTRY_CODE}.{INDI}.?startPeriod={START_DT}&endPeriod={END_DT}',
             'strCodeList':r'http://dataservices.imf.org/REST/SDMX_JSON.svc/CodeList/{codelist_code}_{databaseID}'}

    oecdURL={'struct':r'https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/{dataset}'}
    _sess=None
    conn=None

    def _xml_parse(self, xml_file):
        """
        Parse an XML file, returns a tree of nodes and a dict of namespaces
        :param xml_file: the input XML file
        :returns: (doc, ns_map)
        """
        root = None
        ns_map = {}  # prefix -> ns_uri
        for event, elem in ET.iterparse(xml_file, ['start-ns', 'start', 'end']):
            if event == 'start-ns':
                # elem = (prefix, ns_uri)
                ns_map[elem[0]] = elem[1]
            elif event == 'start':
                if root is None:
                    root = elem
        for prefix, uri in ns_map.items():
            ET.register_namespace(prefix, uri)

        return (ET.ElementTree(root), ns_map)

    def get_xml(self, strURL):
        xml=self._sess.get(strURL, headers=request_headers)
        with open('temp.xml', 'w', encoding='utf-8') as fw:
            fw.write(xml.text)
        et, nsmap=self._xml_parse('temp.xml')
        root=et.getroot()
        cdl=root[1].findall('{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}CodeList', nsmap)
        lstPDF=[]
        for c in cdl:
            codes=[ {'code': cd.attrib['value'], 'value':cd[0].text} for cd in c.findall('{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}Code')]
            pdf=pd.DataFrame(codes).set_index('code')

            pdf.name=c.attrib['id']
            lstPDF.append(pdf)
        return lstPDF


        #root=ET.fromstring(xml.text)

    def get_json(self, strURL):
        try:
            strRet = self._sess.get(strURL, headers=request_headers).json()
        except:
            print('ERROR READING - TRYING RE-GET AFTER 5 SEC')
            print(strURL)
            time.sleep(5)
            try:
                strRet = self._sess.get(strURL, headers=request_headers).json()
            except:
                print('FATAL ERROR READING!')
                return None
        return strRet

    def __init__(self, db_name=work_db_OECD):
        self._sess=requests.session()
        if db_name:
            self.conn=sqlite3.connect(db_name)

    def get_datasets_list(self, strURL, save_db=False):
        dct_ds=self._get_json(strURL)
        print_json(dct_ds)


    def get_datastructure_list(self, ds_code, save=True):
        '''Get structre info for selected dataset'''
        '''
            dct_struct=imf.get_datastructure_list(ds_code='IFS')
            print(list(dct_struct.keys()))
            print(dct_struct['Geographical Areas'])
        '''
        dct_struct=get_json(self._sess, self.imfURLS['strStructDS'].format(dataset=ds_code))
        dct_ret=dict()

        for d in dct_struct['Structure']['CodeLists']['CodeList']:
            #print(d['Name']['#text'], d['@id'])

            code=[(c['Description']['#text'], c['@value']) for c in d['Code']]
            pdf = pd.DataFrame(code, columns=[d['Name']['#text'], d['@id']])
            dct_ret.update({d['Name']['#text']:pdf})
            #print(code)

        return dct_ret

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

def write_status_q(db_name, strCode, strCount, freq, mult=0):
    with sqlite3.connect(db_name) as cn:
        strSQL = '''
            UPDATE {INDI_NAME} SET LastUpdateDate{freq}='{date}', 
            mult={mult},
            LastUpdateCount{freq}='{cnt}' where Code='{id}' '''.format(
            date=dt.datetime.now().strftime('%Y-%m-%d'), cnt=strCount, id=strCode,
            freq=freq.capitalize(), mult=mult, INDI_NAME=strINDI_db_name)
        cn.execute(strSQL)

def write_status(db_name, strCode, strCount, mult=0):
    with sqlite3.connect(db_name) as cn:
        strSQL = '''
            UPDATE {INDI_NAME} SET LastUpdateDate='{date}', 
            mult={mult},
            LastUpdateCount='{cnt}' where Code='{id}' '''.format(
            date=dt.datetime.now().strftime('%Y-%m-%d'), cnt=strCount, id=strCode, mult=mult, INDI_NAME=strINDI_db_name)
        cn.execute(strSQL)

def create_views(db_name='', freq='Q'):
    def create_q(strQuery, name):
        strCreate='CREATE VIEW {name} as SELECT * FROM ({select})'.format(name=name, select=strQuery)
        #print(strCreate)
        cursor = con.cursor()
        try:
            cursor.execute(strCreate)
        except sqlite3.OperationalError:
            cursor.execute('DROP VIEW IF EXISTS {};'.format(name))
            cursor.execute(strCreate)

    def create_full_view():
        nameQ = 'INDICATORS_FULL'
        if freq=='Q':
            strUnion = "SELECT id, country, value, time, time_dop, '{0}' as 'INDI' from {0}"
        else:
            strUnion = "SELECT id, country, value, time, '{0}' as 'INDI' from {0}"

        tbls = pd.read_sql(strQueryINDI_tables, con)

        if tbls.shape[0]==1:
            create_q(strUnion.format(tbls['name'].values[0]), nameQ)
        else:
            strSelect=' UNION '.join([strUnion.format(nm) for nm in tbls['name'].values])
            create_q(strSelect, nameQ)
        print('Create FULL A VIEW done')

    con=sqlite3.connect(db_name)
    create_full_view()

def make_country_translate(conIMF=None, conBIS=None, conOECD=None, conWB=None):

    pdCI = pd.read_sql('select * from {}'.format(strCOUNTRY_db_name), con=conIMF, index_col='id')
    pdCB = pd.read_sql('select * from {}'.format(strCOUNTRY_db_name), con=conBIS, index_col='id')
    pdCO = pd.read_sql('select * from {}'.format(strCOUNTRY_db_name), con=conOECD, index_col='id')
    pdWB = pd.read_sql('select * from {}'.format(strCOUNTRY_db_name), con=conWB, index_col='id')

    pdfALLC = pdCI.merge(pdCB, how='outer', left_index=True, right_index=True)
    pdfALLC = pdfALLC.merge(pdWB, how='outer', left_index=True, right_index=True)

    pdfALLC['Country'] = pdfALLC.apply(lambda x: list(filter(None, x))[0], axis=1)
    pdfALLC = pdfALLC[['Country']]
    pdCO['Cntr'] = pdCO.replace(to_replace={'China (People\'s Republic of)': 'China',
                                            'Russia': 'Russian Federation',
                                            'Slovak Republic': 'Slovakia',
                                            'Korea':'Korea, Republic of'})
    return pdfALLC.reset_index().merge(pdCO.reset_index(), how='outer',
                                      left_on='Country', right_on='Cntr').rename(columns={'Country_x': 'CountryIMF',
                                                                                          'Country_y': 'CountryOECD',
                                                                                          'id_x': 'idIMF',
                                                                                          'id_y': 'idOECD'}).drop_duplicates()

def make_full_panel_dtf(strIMF_DB_path=work_db_IMF,
                       strBIS_DB_path=work_db_BIS,
                        strOECD_DB_path=work_db_OECD,
                        strWB_DB_path=work_db_WB):
    conIMF = sqlite3.connect(strIMF_DB_path)
    conBIS = sqlite3.connect(strBIS_DB_path)
    conOECD = sqlite3.connect(strOECD_DB_path)
    conWB = sqlite3.connect(strWB_DB_path)

    country_translate = make_country_translate(conIMF=conIMF, conBIS=conBIS, conOECD=conOECD)

    country_translate = country_translate.loc[country_translate['idOECD'].notnull(), ('idIMF', 'idOECD')].set_index(
        'idOECD')
    country_translate=country_translate['idIMF']

    strSelectAll='select * from INDICATORS_FULL'

    pdfIMF = pd.read_sql(strSelectAll, con=conIMF)
    print('MAKE PANEL DTF: Reading IMF full database for {} records'.format(pdfIMF.shape[0]))

    pdfBIS = pd.read_sql(strSelectAll, con=conBIS)
    print('MAKE PANEL DTF: Reading BIS full database for {} records'.format(pdfBIS.shape[0]))

    pdfOECD = pd.read_sql(strSelectAll, con=conOECD)
    pdfOECD['country'] = pdfOECD['country'].map(lambda x: country_translate[x])
    print('MAKE PANEL DTF: Reading OECD full database for {} records'.format(pdfOECD.shape[0]))

    pdfWB = pd.read_sql(strSelectAll, con=conWB)
    print('MAKE PANEL DTF: Reading WORLD BANK full database for {} records'.format(pdfWB.shape[0]))

    pdfRes = pd.concat([pdfIMF, pdfBIS, pdfOECD, pdfWB])  #

    pdfRes = pdfRes[['country', 'time', 'time_dop', 'INDI', 'value']].set_index(['country', 'time', 'time_dop', 'INDI'])

    ppp = pdfRes.unstack()  # .reset_index().dropna()
    return ppp

def get_needed_data(databases_path=dict(), indicators=(), countries=()):
    def get_countries_list(dctDBNames,  list_names):
        cntr_l = make_country_translate(conIMF=dctDBNames['IMF'], conWB=dctDBNames['WB'],
                                        conBIS=dctDBNames['BIS'], conOECD=dctDBNames['OECD'])


        strFF = 'CountryIMF' if any(len(l)>2 for l in list_names) else 'idIMF'
        cntr_l = cntr_l.loc[cntr_l[strFF].isin(list_names)]
        return tuple(cntr_l['idIMF'].dropna().tolist() + cntr_l['idOECD'].dropna().tolist()),  \
               cntr_l[['idOECD', 'idIMF']].dropna().set_index('idOECD').to_dict()['idIMF']

    db_conn={key:sqlite3.connect(val) for key, val in databases_path.items()}

    area_code, translate_dict=get_countries_list(db_conn, countries)
    strMQ = "select name from sqlite_master where type = 'table' and name IN {INDI_LIST}".format(INDI_LIST=indicators)
    if len(countries)>1:
        strSelect = "SELECT id, country, value, time, time_dop, '{table}' as 'INDI' from {table} WHERE country IN {cntr_list}"
    else:
        strSelect = "SELECT id, country, value, time, time_dop, '{table}' as 'INDI' from {table} WHERE country = '{cntr_list}'"

    data_list = []
    for key, cn in db_conn.items():

        tbls = pd.read_sql(strMQ, con=cn)['name'].tolist()

        if len(countries)>1:
            slct_q_list =[strSelect.format(table=t, cntr_list=area_code) for t in tbls]
        else:
            slct_q_list = [strSelect.format(table=t, cntr_list=area_code[0]) for t in tbls]

        if slct_q_list:
            data = pd.read_sql(' UNION '.join(slct_q_list), con=cn, index_col='id')
            data.loc[data['country'].str.len()>2, 'country']=data['country'].map(translate_dict)
            data_list.append(data)

    return pd.concat(data_list)


def not_readed_indi(imf_db_path):
    with sqlite3.connect(imf_db_path) as con:
        curs=con.cursor()
        tabls=[l[0] for l in curs.execute(strQueryINDI_tables).fetchall()]
        indis=pd.read_sql('SELECT * FROM {}'.format(strINDI_db_name), con=con, index_col='Code')
        return indis.loc[~indis.index.isin(tabls), :]

def main():
    rd=READ_DB()
    dataset='MEI_CLI'
    pdfl=rd.get_xml(rd.oecdURL['struct'].format(dataset=dataset))
    for pdf in pdfl:
        tab_name=dataset+'_'+pdf.name
        print('write to DB Table', tab_name, end=' ... ')
        pdf.to_sql(tab_name, con=rd.conn, if_exists='replace')
        print('ok')
    print('='*50)
    print('all done')
    #reader=pdr.oecd.OECDReader()

def db_name2annu(strName, suff='_A'):
    tmpName = strName.split('.')
    tmpName[-2] = tmpName[-2] + suff
    return '.'.join(tmpName)


if __name__ == "__main__":
    # str_common_data=os.path.join('..' , 'DATA')
    #
    # dbs_paths=[os.path.join(str_common_data, 'imf.sqlite3'),
    #            os.path.join(str_common_data, 'bis.sqlite3'),
    #            os.path.join(str_common_data, 'oecd.sqlite3')]
    # imf_struct_db=os.path.join(str_common_data, 'IMF_STRUCT.sqlite3')
    #
    # indi_list=('TXG_D_FOB_IX', 'TMG_D_CIF_IX', 'PXP_IX', 'PMP_IX', 'NGDP_R_XDC', 'PCPI_IX', 'FPOLM_PA', 'FM2_XDC',
    #            'EREER_IX', 'FPE_IX', 'FPE_EOP_IX', 'FOSAON_XDC', 'FOSAOP_XDC', 'FILR_PA', 'BGS_BP6_USD',
    #            'INDEX_2010_100_N', 'H_A_M_A_XDC', 'N_A_M_A_XDC')
    #
    # cntry_list=('Argentina', 'Brazil', 'Chile', 'Ecuador', 'India', 'Indonesia', 'Kazakhstan', 'Malaysia', 'Mexico',
    #             'Peru', 'Phillipines', 'Russia', 'South Africa',  'Thailand', 'Turkey')
    #
    # areas=pd.read_sql("select * from COUNTRIES where COUNTRIES.'Geographical Areas' IN {CNTR_LIST}".format(CNTR_LIST=cntry_list),
    #                        index_col='CODE', con=sqlite3.connect(imf_struct_db))
    # area_code=tuple((c for c in areas.index.tolist() if c.isalpha()))
    #
    #
    # all_data=get_needed_data(databases_path=dbs_paths, indicators=indi_list, countries=area_code)
    # print(all_data)
    print('All done')






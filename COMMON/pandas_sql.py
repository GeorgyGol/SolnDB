import datetime as dt
import pandas as pd
import sqlalchemy as sa
import re
import os
import requests
import ssl
from pandas_datareader import wb as pddr

import COMMON.readers as cmm

from abc import ABC, abstractmethod

class _bis_elem(ABC):
    strUrl=''
    pdf_value = None
    pdf_countries = None
    pdf_indi=None
    pdf_source=None

    result_cols_indi = ['Code', 'Name', 'Freq', 'Start', 'Dataset', 'LastUpdateDate', 'LastUpdateCount', 'MULT']
    result_cols_values=['indi', 'country', 'time', 'value']

    @property
    def URL(self):
        return self.strUrl

    @classmethod
    def quartal_mean(self, pdf):
        pdf_indi = pdf.reset_index().groupby(by=['country', pd.Grouper(key='time', freq='Q')])['value'].mean()
        pdf_indi = pdf_indi.reset_index()
        pdf_indi['time'] = pdf_indi['time'].apply(lambda x: '{0}-Q{1}'.format(x.year, x.quarter))
        return pdf_indi

    @property
    def values(self):
        return self.pdf_value

    @property
    def countries(self):
        return self.pdf_countries

    @property
    def indi(self):
        return self.pdf_indi

    @property
    def pdf(self):
        return self.pdf_source

    @abstractmethod
    def get_coutries(self, pdf):
        pass

    @abstractmethod
    def get_indi(self, pdf):
        pass

    @abstractmethod
    def make_code(self, x):
        pass

    @abstractmethod
    def read(self, indiList=[], countryList=[]):
        pass

class bis_prices(_bis_elem):
    strUrl=r'https://www.bis.org/statistics/full_bis_selected_pp_csv.zip'

    def make_code(self, x):
        return re.sub(r'[=,-]', '_', '_'.join(x).upper()).replace(' ', '')

    def get_coutries(self, pdf):
        pdfPP = pdf[['REF_AREA', 'Reference area']].rename(
            columns={'REF_AREA': 'id', 'Reference area': 'Country'}).set_index('id')
        pdfPP = pdfPP.loc[~pdfPP.index.duplicated(), :].sort_index()
        return pdfPP[['Country']]

    def get_indi(self, pdf):
        work_cols = ['FREQ', 'VALUE', 'Value', 'Unit of measure']
        # result_cols = ['Code', 'Name', 'Freq', 'Start', 'Dataset', 'LastUpdateDate', 'LastUpdateCount', 'MULT']

        pdf = pdf.loc[:,work_cols]
        # pdf['Code'] = pdf[['Unit of measure', 'VALUE']].apply(lambda x: re.sub(r'[=,-]', '_', '_'.join(x).upper()).replace(' ', ''),
        #                                          axis=1)
        pdf['Code'] = pdf[['Unit of measure', 'VALUE']].apply(self.make_code, axis=1)
        pdf['Name'] = pdf[['Unit of measure', 'Value']].apply(
            lambda x: 'Property prices: selected series, ' + ', '.join(x), axis=1)
        pdf = pdf.drop_duplicates()
        pdf = pdf.rename(columns={'FREQ': 'Freq'})
        pdf['Dataset'] = self.URL
        pdf['LastUpdateDate'] = 0
        pdf['LastUpdateCount'] = 0
        pdf['Start'] = 1970
        pdf['MULT'] = 0
        return pdf[self.result_cols_indi].set_index('Code')

    def read(self, indiList=[], countryList=[]):
        pdf = pd.read_csv(self.URL, compression='zip')
        self.pdf_source=pdf
        self.pdf_countries = self.get_coutries(pdf)
        self.pdf_indi=self.get_indi(pdf)
        work_cols = ['REF_AREA', 'VALUE', 'Unit of measure']
        dop_cols = ['FREQ', 'Frequency', 'Reference area', 'Value', 'UNIT_MEASURE', 'Time Period']

        data_cols = [c for c in pdf.columns.tolist() if c not in work_cols + dop_cols]

        pdf = pdf[work_cols + data_cols]
        pdfR = pdf.set_index(work_cols).stack()
        pdfR = pdfR.reset_index().rename(columns={'level_3': 'time', 0: 'value',
                                                  'REF_AREA': 'country', 'VALUE': 'RN', 'Unit of measure': 'INDI'})

        pdfR['indi'] = pdfR[['INDI', 'RN']].apply(self.make_code, axis=1)
        self.pdf_value=pdfR[self.result_cols_values]

class bis_credit(_bis_elem):
    strUrl = r'https://www.bis.org/statistics/full_bis_total_credit_csv.zip'

    def make_code(self, x):
        return '_'.join(x[['TC_BORROWERS', 'TC_LENDERS', 'VALUATION', 'TC_ADJUST', 'UNIT_TYPE']])

    def get_coutries(self, pdf):
        pdfCR = pdf[['BORROWERS_CTY', 'Borrowers\' country']].rename(
            columns={'BORROWERS_CTY': 'id', 'Borrowers\' country': 'Country'}).set_index('id')
        pdfCR = pdfCR.loc[~pdfCR.index.duplicated(), :].sort_index()
        return pdfCR[['Country']]

    def get_indi(self, pdf):
        work_cols = ['FREQ', 'TC_BORROWERS', 'Borrowing sector', 'TC_LENDERS', 'Lending sector',
                     'VALUATION', 'Valuation', 'Unit type', 'TC_ADJUST', 'Type of adjustment', 'UNIT_TYPE']
        name_filed = ['Borrowing sector', 'Lending sector', 'Valuation', 'Unit type', 'Type of adjustment']

        pdf = pdf.loc[:,work_cols]
        pdf['Code'] = pdf.apply(self.make_code, axis=1)
        pdf['Name'] = pdf[name_filed].apply(
            lambda x: 'Credit to the non-financial sector, ' + ', '.join([c + ': ' + x[c] for c in x.index]), axis=1)
        pdf = pdf.drop_duplicates()
        pdf = pdf.rename(columns={'FREQ': 'Freq'})

        pdf['Dataset'] = self.URL
        pdf['LastUpdateDate'] = 0
        pdf['LastUpdateCount'] = 0
        pdf['Start'] = 1970
        pdf['MULT'] = pdf['Unit type'].map(lambda x: 9 if re.search('Domestic currency', x) else 0)
        return pdf[self.result_cols_indi].set_index('Code')

    def read(self, indiList=[], countryList=[]):
        work_cols = ['TC_BORROWERS', 'TC_LENDERS', 'VALUATION', 'TC_ADJUST', 'UNIT_TYPE', 'BORROWERS_CTY']
        dop_cols = ['FREQ', 'Frequency', 'BORROWERS_CTY', "Borrowers' country", 'TC_BORROWERS', 'Borrowing sector',
                    'TC_LENDERS',
                    'Lending sector', 'VALUATION', 'Valuation', 'UNIT_TYPE', 'Unit type', 'TC_ADJUST',
                    'Type of adjustment',
                    'Time Period']
        pdf = pd.read_csv(self.URL, compression='zip')
        self.pdf_source=pdf
        self.pdf_countries=self.get_coutries(pdf)
        self.pdf_indi=self.get_indi(pdf)

        data_cols = [c for c in pdf.columns.tolist() if c not in work_cols + dop_cols]
        pdfR = pdf[work_cols + data_cols]
        pdfR['indi'] = pdfR.apply(self.make_code, axis=1)
        pdfR = pdfR[data_cols + ['indi',
                                 'BORROWERS_CTY']].set_index(['indi',
                                                              'BORROWERS_CTY']).stack().reset_index().rename(
            columns={'BORROWERS_CTY': 'country', 'level_2': 'time', 0: 'value'})
        pdfR['value'] = pdfR[['indi', 'value']].apply(
            lambda x: x['value'] * (10 ** (self.pdf_indi.loc[x['indi'], 'MULT'])), axis=1)
        self.pdf_value = pdfR[self.result_cols_values]

class bis_broad_real(_bis_elem):
    strUrl = r'https://www.bis.org/statistics/eer/broad.xlsx'

    def make_code(self, x):
        return 'EER_REAL_2010_100'

    def get_coutries(self, pdf):
        pdf_c = pdf.iloc[1:]
        pdf_cntry = pdf.iloc[:1].reset_index().T.dropna().reset_index().rename(columns={'index': 'Country', 0: 'id'})
        pdf_cntry['id'] = pdf_cntry['id'].apply(lambda x: x[2:])

        return pdf_cntry.set_index('id')

    def get_indi(self, pdf):
        vals = [self.make_code(''), 'BIS effective exchange rate, Real (CPI-based), Broad Indices, Monthly averages; 2010=100',
                'Q', 1994, self.strUrl, dt.datetime.now(), pdf.shape[0], 0]
        return pd.DataFrame(dict(zip(self.result_cols_indi, vals)), index=[0, ]).set_index('Code')

    def read(self, indiList=[], countryList=[]):
        pdf = pd.read_excel(self.URL, sheet_name='Real', header=3, index_col=0)
        self.pdf_source=pdf
        self.pdf_countries=self.get_coutries(pdf)
        self.pdf_indi=self.get_indi(pdf)
        pdf_indi = pdf.iloc[1:]

        pdf_indi.columns = pdf.iloc[0]
        pdf_indi = pdf_indi.reset_index().rename(columns={'EER for:': 'time'}).set_index('time')

        pdf_indi.rename(columns=lambda x: x[2:], inplace=True)
        pdf_indi = pdf_indi.reset_index().set_index('time')

        pdf_indi.index = pd.to_datetime(pdf_indi.index)
        pdf_indi = pdf_indi.stack()
        pdf_indi = pdf_indi.reset_index(0).reset_index()

        pdf_indi.rename(columns={pd.NaT: 'country', 0: 'value'}, inplace=True)
        pdf_indi['value'] = pd.to_numeric(pdf_indi['value'])
        pdf_indi = self.quartal_mean(pdf_indi)

        pdf_indi['indi'] = self.make_code('')
        self.pdf_value=pdf_indi[self.result_cols_values]

class bis_cbr_pol(_bis_elem):
    strUrl = r'https://www.bis.org/statistics/full_webstats_cbpol_d_dataflow_csv_row.zip'

    def make_code(self, x):
        return 'CBRPOL'

    def get_coutries(self, pdf):
        work_cols = [c for c in pdf if re.search(r'D\:', c)]
        pdf_r = pdf.iloc[:2][work_cols]
        pdf_r = pdf_r.applymap(lambda x: re.sub(r'[A-Z]{2}:', '', x))
        pdf_r = pdf_r.applymap(lambda x: x.replace('D:', ''))
        return pdf_r.T.reset_index(drop=True).rename(columns={0: 'Country', 1: 'id'}).set_index('id')

    def get_indi(self, pdf):
        vals = [self.make_code(''),
                'Central bank policy rates',
                'Q', 1946, self.strUrl, dt.datetime.now(), pdf.shape[0], 0]
        return pd.DataFrame(dict(zip(self.result_cols_indi, vals)), index=[0, ]).set_index('Code')

    def read(self, indiList=[], countryList=[]):
        pdf = pd.read_csv(self.URL, compression='zip', low_memory=False)
        self.pdf_source=pdf

        self.pdf_countries = self.get_coutries(pdf)
        self.pdf_indi = self.get_indi(pdf)

        pdf_w = pdf.iloc[2:]
        pdf_w.columns = pdf.iloc[1]

        pdf_ret = pdf_w.rename(columns={'Time Period': 'time'}).rename(columns=lambda x: x.replace('D:', ''))
        pdf_ret['time'] = pd.to_datetime(pdf_ret['time'])  # ???
        pdf_ret = pdf_ret.set_index('time').astype(float)  # ???
        pdf_ret = pdf_ret.stack().reset_index().rename(columns={1: 'country', 0: 'value'})
        pdf_ret = self.quartal_mean(pdf_ret)
        pdf_ret['indi'] = self.make_code('')
        self.pdf_value=pdf_ret[self.result_cols_values]

class bis_credit_non_fin(_bis_elem):
    strUrl = r'https://www.bis.org/statistics/totcredit/totcredit.xlsx'
    strTempl1='Credit to Private non-financial sector from Banks, total at Market value - Domestic currency - Adjusted for breaks'
    strTempl2='Credit to Private non-financial sector from Banks, total at Market value - Percentage of GDP - Adjusted for breaks'

    def make_code(self, x):
        return 'Q_' + '_'.join(x.split(':')[2:])

    def get_indi(self, pdf):
        pdfI = pdf.loc[:2].T.reset_index().rename(columns={'index': 'Name', 0: 'MULT', 1: 'Country', 2: 'Code'})
        pdfI['Code'] = pdfI['Code'].map(self.make_code)
        pdfI['Name'] = pdfI['Name'].map(lambda x: ' - '.join(x.split(' - ')[1:]))
        pdfI['Freq'] = 'Q'
        pdfI['Start'] = 1940
        pdfI['Dataset'] = self.URL
        pdfI['LastUpdateDate'] = 0
        pdfI['LastUpdateCount'] = 0
        pdfI['MULT']=pdfI['MULT'].map(lambda x: 9 if re.search('Billions', x) else 0)
        return pdfI[self.result_cols_indi].drop_duplicates().set_index('Code')

    def get_coutries(self, pdf):
        pdfC = pdf.loc[1:2]
        pdfC = pdfC.T.reset_index(drop=True).rename(columns={1: 'Country', 2: 'id'})
        pdfC['id'] = pdfC['id'].map(lambda x: x.split(':')[1])
        return pdfC.set_index('id').drop_duplicates()

    def read(self, indiList=[], countryList=[]):
        pdf=pd.read_excel(self.URL, sheet_name='Quarterly Series')
        self.pdf_source=pdf
        lstWorkCols = pdf.columns.tolist()[1:]

        pdf=pdf[lstWorkCols]
        self.pdf_countries=self.get_coutries(pdf)
        self.pdf_indi = self.get_indi(pdf)
        pdfD=pdf.loc[2:].T
        pdfD['indi']=pdfD[2].map(self.make_code)
        pdfD['country'] = pdfD[2].map(lambda x: x.split(':')[1])

        pdfW=self.pdf_source.set_index('Back to menu')[lstWorkCols].iloc[2:]
        pdfW.columns=pdfW.iloc[0]
        pdfW=pdfW.iloc[1:].reset_index().rename(columns={'Back to menu': 'time'}).set_index('time').stack().reset_index()
        pdfW=pdfW.rename(columns={'Period':'indi', 0:'value'})
        pdfW['country']=pdfW['indi'].map(lambda x: x.split(':')[1])
        pdfW['indi']=pdfW['indi'].map(self.make_code)
        pdfW['time']=pdfW['time'].map(lambda x: '{year}-Q{quart}'.format(year=x.year, quart=x.quarter))
        pdfW['value']=pdfW[['indi', 'value']].apply(lambda x: x['value'] * (10**self.pdf_indi.loc[x['indi'], 'MULT']), axis=1)
        self.pdf_value=pdfW[self.result_cols_values]
        #print(pdfW.loc[2].map(self.make_code).unique().tolist())

class bis_usd_exchange(_bis_elem):
    strUrl = r'https://www.bis.org/statistics/full_webstats_xru_current_dataflow_csv.zip'

    def make_code(self, x):
        return 'Q_USD_EXCHANGE_A'

    def get_coutries(self, pdf):
        pdfC=pdf[['REF_AREA', 'Reference area']].rename(columns={'REF_AREA':'id', 'Reference area':'Country'}).drop_duplicates()
        return pdfC.set_index('id')

    def get_indi(self, pdf):
        pdfI=pdf.rename(columns={'Time Period':'Code', 'Collection':'Name'})
        pdfI['Name']=pdfI['Name'].map(lambda x : 'US dollar exchange rates, ' + x)
        pdfI['Code']=pdfI['Code'].map(self.make_code)
        pdfI['Dataset']=self.URL
        pdfI['LastUpdateDate'] = 0
        pdfI['LastUpdateCount'] = 0
        pdfI['Start'] = 1970
        pdfI['Freq'] = 'Q'
        pdfI['MULT'] = 0
        return pdfI.drop_duplicates()[self.result_cols_indi].set_index('Code')

    def read(self, indiList=[], countryList=[]):
        pdf = pd.read_csv(self.URL, compression='zip')
        self.pdf_source = pdf
        pdfW=pdf.loc[(pdf['COLLECTION']=='A') & (pdf['FREQ']=='Q')]
        self.pdf_countries = self.get_coutries(pdfW)
        self.pdf_indi = self.get_indi(pdfW[['Collection', 'Time Period']])
        w_cols=[c for c in pdfW.columns.tolist() if re.search(r'\d{4}-Q\d', c) or (c in {'REF_AREA', 'Time Period'})]
        pdfW=pdfW[w_cols].rename(columns={'REF_AREA':'country', 'Time Period':'indi'})
        pdfW['indi']=pdfW['indi'].map(self.make_code)
        pdfW=pdfW.set_index(['country', 'indi']).stack().reset_index().rename(columns={'level_2':'time', 0:'value'})

        self.pdf_value=pdfW[self.result_cols_values]


class bis_debt_serv_nf(_bis_elem):
    strUrl = r'https://www.bis.org/statistics/full_bis_dsr_csv.zip'

    def make_code(self, x):
        return 'DEBT_SERV_RATIO_'+ x

    def get_coutries(self, pdf):
        pdfC=pdf[['BORROWERS_CTY', 'Borrowers\' country']].rename(columns={'BORROWERS_CTY':'id', 'Borrowers\' country':'Country'}).drop_duplicates()
        return pdfC.set_index('id')

    def get_indi(self, pdf):
        pdfI=pdf.rename(columns={'DSR_BORROWERS':'Code', 'Borrowers':'Name'})
        pdfI['Name']=pdfI['Name'].map(lambda x : 'Debt service ratios for the private non-financial sector, borrowers - ' + x)
        pdfI['Code']=pdfI['Code'].map(self.make_code)
        pdfI['Dataset']=self.URL
        pdfI['LastUpdateDate'] = 0
        pdfI['LastUpdateCount'] = 0
        pdfI['Start'] = 1999
        pdfI['Freq'] = 'Q'
        pdfI['MULT'] = 0
        return pdfI.drop_duplicates()[self.result_cols_indi].set_index('Code')

    def read(self, indiList=[], countryList=[]):
        pdf = pd.read_csv(self.URL, compression='zip')
        self.pdf_source = pdf
        pdfW=pdf.loc[pdf['FREQ']=='Q']
        self.pdf_countries = self.get_coutries(pdfW)

        w_cols=[c for c in pdfW.columns.tolist() if re.search(r'\d{4}-Q\d', c) or (c in {'BORROWERS_CTY', 'DSR_BORROWERS'})]
        pdfW=pdfW[w_cols].rename(columns={'BORROWERS_CTY':'country', 'DSR_BORROWERS':'indi'})
        pdfW['indi']=pdfW['indi'].map(self.make_code)

        self.pdf_indi = self.get_indi(pdf[['DSR_BORROWERS', 'Borrowers']].drop_duplicates())

        pdfW=pdfW.set_index(['country', 'indi']).stack().reset_index().rename(columns={'level_2':'time', 0:'value'})

        self.pdf_value=pdfW[self.result_cols_values]



def read_bis(indiTYPE='PPRICES', debug_info=False, get_countries=False):
    bis_indis={'PPRICES':bis_prices(), 'CREDIT':bis_credit(), 'BROAD_REAL':bis_broad_real(), 'CBRPOL':bis_cbr_pol(),
               'CREDIT_NON_FIN':bis_credit_non_fin(), 'USD_ESCH':bis_usd_exchange(), 'DEBT_SERV_NF':bis_debt_serv_nf()}

    ssl_cntxt=ssl._create_default_https_context
    ssl._create_default_https_context = ssl._create_unverified_context

    bi_i=bis_indis[indiTYPE]
    bi_i.read()

    ssl._create_default_https_context = ssl_cntxt

    if get_countries:
        if debug_info:
            return bi_i.values, bi_i.countries, bi_i.indi, bi_i.pdf
        else:
            return bi_i.values, bi_i.countries, bi_i.indi
    else:
        if debug_info:
            return bi_i.values, bi_i.pdf
        else:
            return bi_i.values

def read_oecd(strDataSetID='MEI_CLI', frequency='M', countryCode='RUS', indiID='LOLITOAA',
             startDate=1957, endDate=dt.datetime.now().year, debug_info=False, get_countries=False):

    def get_county_series(lstDimSer):
        for s in lstDimSer:
            if s['id']=='LOCATION':
                return s['values']
        return None

    def make_param(x):
        if type(x) is str:
            return x
        else:
            return '+'.join(x)

    if type(indiID) is not str:
        raise TypeError('Indicator ID must be string')

    strOECDURL = r'https://stats.oecd.org/SDMX-JSON/data/{dataset}/{indi}.{country}.{frequency}/all?startTime={start}-Q1&endTime={end}-Q4'

    strQ=strOECDURL.format(dataset=strDataSetID,
                           frequency=frequency,
                           country=make_param(countryCode),
                           indi=make_param(indiID),
                           start=startDate,
                           end=endDate)
    _sess = requests.session()
    strJ=cmm.get_json(_sess, strQ)

    dctStruct=[ {'ID':s['id'], 'NAME':s['name'],
                 'VALUE':s['values'][0]['id'], 'DESCR':s['values'][0]['name']} for s in strJ['structure']['attributes']['series'] if s['id']!='REFERENCEPERIOD']

    pdStruct= pd.DataFrame(dctStruct)
    mult=10**int(pdStruct.loc[pdStruct['ID']=='POWERCODE', 'VALUE'].values[0])

    dctt=[d for d in
                 strJ['structure']['dimensions']['observation'][0]['values'] ]

    pdTime = pd.DataFrame( [d for d in
                 strJ['structure']['dimensions']['observation'][0]['values'] ])
    pdTime['id']=pd.to_datetime(pdTime['id'], format='%Y-%m')

    #pdTime=pdTime.sort_values('id')
    #base_observe=pdTime.loc[0, ('id')]

    series = strJ['dataSets'][0]['series']
    pdCountry = pd.DataFrame(get_county_series(strJ['structure']['dimensions']['series']))

    pdf_ser_list = []

    for k, v in pdCountry.iterrows():

        pdf=pd.DataFrame([{'time': pdTime.iloc[int(key)]['id'],
                           'value': val[0],
                           'country':v['id']} for key, val in series['0:{}:0'.format(k)]['observations'].items()])

        pdf['value']=pd.to_numeric(pdf['value'], errors='coerce')
        pdf['value']*=mult
        pdf_ser_list.append(pdf)

    pdfRet=pd.concat(pdf_ser_list)
    pdfRet=pdfRet.sort_values(by=['country', 'time'])

    #print(pdfRet)
    #pdfRet.to_csv('CSCICP03.csv', sep=';')

    pdfRet=pdfRet.groupby(by=['country', pd.Grouper(key='time', freq='Q')])['value'].mean()
    pdfRet=pdfRet.reset_index()

    pdfRet['time_dop']=pdfRet['time'].dt.quarter

    pdfRet['time'] = pdfRet['time'].dt.year
    pdfRet['id'] = pdfRet.apply(lambda x: cmm.get_hash([x['country'].strip(), int(x['time']), int(x['time_dop'])]), axis=1)
    pdfRet['mult']=mult
    pdfRet=DataFrameDATA(pdfRet.set_index('id'))
    pdfRet.name=indiID

    if debug_info:
        if get_countries:
            return pdfRet, strQ, strJ, pdCountry
        else:
            return pdfRet, strQ, strJ, pdCountry
    else:
        if get_countries:
            return pdfRet, pdCountry
        else:
            return pdfRet

def read_imf(strDataSetID='IFS', frequency='Q', countryCode='U2', indiID='NGDP_XDC',
             startDate=1957, endDate=dt.datetime.now().year, debug_info=False):
    '''get data indicators from selected dataset'''

    '''
            dct_indi = imf.get_indi(countryCode=('US', 'BR'), indiID=('NGDP_XDC', 'NCP_XDC'), frequency='A')
            dct_indi = imf.get_indi(countryCode=('US', 'BR'), indiID='NGDP_XDC', frequency='A')
            imf.print_indi(dct_indi)
    '''

    def return_none(mess=None):
        raise ValueError(mess)

    # def add_err_list(keyIndi, lstNoCountry):
    #     if type(lstNoCountry) is str:
    #         lstNoCountry=[lstNoCountry,]
    #
    #         try:
    #             self._no_data_list[keyIndi]+=', '.join(lstNoCountry)
    #         except KeyError:
    #             self._no_data_list.setdefault(keyIndi, ', '.join(lstNoCountry))
    #         #print(self._no_data_list)
    def make_param(param):
        if type(param) is not str:
            try:
                return '+'.join(param)
            except:
                print(param)
        else:
            return param
    def make_key(strindi, strfreq):
        return strindi #'{0}_{1}'.format(strindi, strfreq)

    def make_dataframe(ser):
        try:
            if type(ser['Obs']) is not list:
                ser=[ser,]
                ttt= (ser['Obs'][0]['@OBS_VALUE'], ser['Obs'][0]['@TIME_PERIOD'], ser['@INDICATOR'],
                        ser['@REF_AREA'], ser['@UNIT_MULT'], )
        except (KeyError, TypeError):
                #print(self.print_json(ser))
            return None

        pdf = DataFrameDATA(ser['Obs'], dtype=float).rename(columns={'@OBS_VALUE': lstFields[3],
                                                                            '@TIME_PERIOD': lstFields[1]})
        pdf[lstFields[2]] = ser['@REF_AREA']
        try:
            pdf['value'] = pdf[lstFields[3]] * (10 ** int(ser['@UNIT_MULT'].replace('N', '')))
            pdf['mult'] = ser['@UNIT_MULT']
        except:
            return None

        #pdf['id'] = pdf[[lstFields[2], lstFields[1]]].apply(get_hash, axis=1)
        lstRetFields=lstFields.copy()

        if frequency != 'A':
            pdf[strFiledDop] = pdf[lstFields[1]].apply(lambda x: x.split(frequency)[1]).astype(int)
            pdf[lstFields[1]] = pdf[lstFields[1]].apply(lambda x: x.split('-')[0]).astype(int)
            pdf['id'] = pdf[[lstFields[2], lstFields[1], strFiledDop]].apply(cmm.get_hash, axis=1)
            lstRetFields+=[strFiledDop]
        else:
            pdf[lstFields[1]] = pdf[lstFields[1]].astype(int)
            pdf['id'] = pdf[[lstFields[2], lstFields[1]]].apply(cmm.get_hash, axis=1)

        return pdf[lstRetFields].set_index(lstFields[0])

    strJSON=''
    strQUERY=''

    _sess = requests.session()

    _strIndi = r'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/{DATASET}/{FREQ}.{CONTRY_CODE}.{INDI}.?startPeriod={START_DT}&endPeriod={END_DT}'

    lstFields = ['id', 'time', 'country', 'value', 'mult']
    strFiledDop = 'time_dop'

    strArea=make_param(countryCode)
    if type(indiID) is not str:
        print(indiID, type(indiID))
        raise TypeError("indicator must by string")


    strReq=_strIndi.format(DATASET=strDataSetID, FREQ=frequency,
                                   CONTRY_CODE=strArea, INDI=indiID,
                                   START_DT=startDate, END_DT=endDate)

    try:
        strQUERY=strReq
        #dct_ds = _get_json(strReq)
        dct_ds = cmm.get_json(_sess, strReq)
        strJSON=dct_ds
    except ValueError:
        return return_none(mess='no response')

    try:
        data=dct_ds['CompactData']['DataSet']['Series']
        if type(data) is dict: # only one country
            data=[data,]
    except:
        # no country data
        return return_none(mess='no data')

    #self.print_json(dct_ds)

    lstRes = [make_dataframe(seria) for seria in data]


    try:
        res = DataFrameDATA(pd.concat(lstRes, axis=0))
    except ValueError:
        # not data for indicator. Series have STATUS insteed VALUES
        return return_none(mess='not right data')

    res.name=make_key(data[0]['@INDICATOR'], data[0]['@FREQ'])
    res.not_country = set(countryCode)-set(res['country']) # no country list

    if debug_info:
        return res, strQUERY, strJSON
    else:
        return res


class DataFrameDATA(pd.DataFrame):

    not_country=[]
    name=''


    @property
    def _constructor(self):
        return DataFrameDATA

    def to_sql(self, name, con, flavor='sqlite', schema=None, if_exists='fail', index=True,
               index_label=None, chunksize=100, dtype=None):

        def drop_table(strTName):
            meta = sa.MetaData(bind=con)
            try:
                tbl_ = sa.Table(strTName, meta, autoload=True, autoload_with=con)
                tbl_.drop(con, checkfirst=False)
            except:
                pass

        def create_table(strTName, strIndName):

            def type_to_sqlA(lstName, sqType):
                l = len(lstName)
                return dict(zip(lstName, [sqType] * l))

            dct_trans = type_to_sqlA(self.select_dtypes(include='int').columns.tolist(), sa.Integer)
            dct_trans.update(type_to_sqlA(self.select_dtypes(include='int64').columns.tolist(), sa.Integer))
            dct_trans.update(type_to_sqlA(self.select_dtypes(include='datetime').columns.tolist(), sa.String))
            dct_trans.update(type_to_sqlA(self.select_dtypes(include='float').columns.tolist(), sa.Float))
            dct_trans.update(type_to_sqlA(self.select_dtypes(include='object').columns.tolist(), sa.String))

            lstDBCols=[sa.Column(k, v) for k, v in dct_trans.items()]
            metadata = sa.MetaData(bind=con)

            bname_t = sa.Table(strTName, metadata,
                               sa.Column(strIndName, sa.String, primary_key=True, nullable=False, autoincrement=False),
                               *lstDBCols)
            metadata.create_all()

        def buff_insert(alch_table, insert_prefix, values, buff_size=chunksize):
            for i in cmm.iterate_group(values, buff_size):
                inserter = alch_table.insert(prefixes=insert_prefix, values=i)
                con.execute(inserter)

        if if_exists == 'replace':
            drop_table(name)
            if_exists = 'fail'

        if not con.dialect.has_table(con, name):
            create_table(name, self.index.name)

        meta = sa.MetaData(bind=con)
        tbl_names = sa.Table(name, meta, autoload=True, autoload_with=con)
        vals = self.reset_index().to_dict(orient='records')

        inserter = None

        if flavor == 'mysql':
            if if_exists in ['append', 'ignore']:
                inserter = tbl_names.insert(prefixes=['IGNORE'], values=vals)
            elif if_exists in ['update', 'upsert']:
                ins_state = sa.dialects.mysql.insert(tbl_names).values(vals)
                inserter = ins_state.on_duplicate_key_update(Date=ins_state.inserted.Date)
            elif if_exists == 'fail':
                inserter = tbl_names.insert(values=vals)
            con.execute(inserter)

        if flavor == 'sqlite':
            if if_exists in ['append', 'ignore']:
                # inserter = tbl_names.insert(prefixes=['OR IGNORE'], values=vals)
                buff_insert(tbl_names, ['OR IGNORE'], vals, buff_size=chunksize)
            elif if_exists in ['update', 'upsert']:
                buff_insert(tbl_names, ['OR REPLACE'], vals, buff_size=chunksize)
                # inserter = tbl_names.insert(prefixes=['OR REPLACE'], values=vals)
            elif if_exists == 'fail':
                buff_insert(tbl_names, None, vals, buff_size=chunksize)


def _read_indy():
    with open(os.path.join('Source', 'work_countries.txt') , 'r') as country_f:

        cntry=country_f.read().split('+')
        print('reading {0} countries'.format(len(cntry)))

    coni = sa.create_engine('sqlite+pysqlite:///{}'.format(cmm.db_indicators))
    #pdfIndi=pd.read_sql('select * from INDICATORS where LastUpdateDateA is NULL', coni, index_col='Code')
    pdfIndi = pd.read_sql('select * from INDICATORS', coni, index_col='Code').iloc[:5]

    pdfIndiA=pdfIndi[pdfIndi['A']>=1]
    pdfIndiQ = pdfIndi[pdfIndi['Q'] >= 1]

    for k, v in pdfIndiQ.iterrows():
        try:
            pdf=read_imf(strDataSetID=v['Dataset'], indiID=k, countryCode=cntry[:50], frequency='Q')
            print(pdf.name, pdf.shape, len(pdf.not_country))
        except ValueError as e:
            print(e, k, 0, 50)

    print('ok')

def read_worldbank(symbol='DT.DOD.DECT.CD.GG.AR.US', countries='all', start=1998,
                   end=2019, freq='Q', debug_info=False, get_countries=False):
    pdwb = pddr.WorldBankReader(symbols=symbol, countries=countries, start=start, end=end, freq=freq)

    ret=DataFrameDATA(pdwb.read().reset_index()).rename(columns={symbol:'value', 'year':'time'})
    ret['indi']=symbol

    db_cntr=pdwb.get_countries()[['iso2c', 'name']].rename(columns={'iso2c':'id', 'name':'Country'}).set_index('id')

    #db_cntr.to_csv('WB_countries.csv', sep=';')

    ret['country'] = ret['country'].map(db_cntr.reset_index().set_index('Country')['id'])
    ret['time_dop'] = ret['time'].str.extract('Q(\d+)', expand=False)
    ret['time_dop']=ret['time_dop'].astype(int)
    ret['time'] = ret['time'].str.extract('^(\d+)Q', expand=False)
    ret['time'] = ret['time'].astype(int)

    ret['id']=ret.apply(lambda x: cmm.get_hash([x['country'].strip(), int(x['time']), int(x['time_dop'])]), axis=1)
    ret=ret.set_index('id')

    db_indi=pdwb.get_indicators()

    #db_indi.to_csv('wb_indi.csv', sep=';')

    db_indi=db_indi.loc[db_indi['id']==symbol].rename(columns={'id':'Code', 'name':'Name', 'source':'Dataset'})
    db_indi['Freq']='Q'
    db_indi['Start']=ret['time'].min()
    db_indi['LastUpdateDate']=dt.datetime.now()
    db_indi['LastUpdateCount']=ret.shape[0]
    db_indi['MULT']=0

    db_indi=db_indi[['Code','Dataset', 'Freq', 'LastUpdateCount', 'LastUpdateDate', 'MULT', 'Name', 'Start']]

    if get_countries:
        if debug_info:
            return ret, db_cntr, db_indi, pdwb
        else:
            return ret, db_cntr, db_indi
    else:
        if debug_info:
            return ret, pdwb
        else:
            return ret


if __name__ == "__main__":

    #pd, strQ, strJ=from_imf(frequency='Q', countryCode='RU', indiID='NSDGDP_XDC', debug_info=True)
    #for i, v in pd.iterrows():
    #    print(i == get_hash(v[['country', 'time', 'time_dop']]))
    #print_json(strJ)
    #print(pd)
    #_read_indy()

    # pr=bis_debt_serv_nf()
    # #pr =bis_cbr_pol()
    # pr.read()
    # print(pr.values)
    # print(pr.countries)
    # print(pr.indi)
    #print(read_bis(indiTYPE='CBRPOL'))

    # pdfRet, strQ, strJ, pdCountry=read_oecd(countryCode=['BEL','AUS'], indiID='IRLT',  strDataSetID='MEI_FIN',
    #                                         debug_info=True, get_countries=False)
    #
    # print(pdfRet.loc[pdfRet['country']=='BEL'])
    # print(strQ)
    # cmm.print_json(strJ)

    #print(read_imf(strDataSetID='IFS', countryCode=['RU', 'US', 'ZA'], indiID='ENEER_IX'))
    #ppp=read_bis(indiTYPE='CBRPOL')
    #ppp = read_bis(indiTYPE='BROAD_REAL')

    #print(ppp)
    #ppp.to_csv('cbrpol.csv', sep=';')
    #dtPP, cntrPP, indiPP = read_bis(get_countries=True)
    #print(dtPP.head(10))
    #print(cntrPP.head(10))
    #print(indiPP.head(10))

    # dt, cntr, indi=read_worldbank(get_countries=True)
    # print(dt)
    # print('+'.join(cntr.index.tolist()))

    print('all done')



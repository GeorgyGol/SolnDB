"""work with local databases"""
import COMMON.readers as cmm

import os, os.path
import sqlite3
import pandas as pd
import numpy as np
import datetime as dt

import scipy.io

pd.options.mode.chained_assignment = None

def to_matlab(pdf, filename):
    dct={cn:pdf.loc[pdf['country'] == cn].set_index('country').to_dict('list') for cn in pdf['country'].unique().tolist()}
    scipy.io.savemat(filename, dct)

def flat_dtf(dtf):
    d2m = dtf.reset_index()
    clns = [cln[1] if cln[0] == 'value' else cln[0] for cln in d2m.columns]
    d2m.columns = clns
    return d2m

def sparse_country_frame(pdf, start_year=0, stop_year=dt.datetime.now().year):
    res_cols = [c for c in pdf.columns.tolist() if c != 'index']

    pde=pd.DataFrame(dict(zip(res_cols, [np.nan]*len(res_cols))),
                     index=pd.date_range('1/1/{Y}'.format(Y=start_year), freq='QS', end='1/1/{Y}'.format(Y=stop_year)),
                     columns=pdf.columns.tolist())

    cntr=pdf['country'].unique().tolist()[0]

    dtf_res = pdf[res_cols]
    dtf_res.loc[:, 'dt'] = pd.to_datetime(dtf_res.apply(lambda x: '{Y}-Q{Q}'.format(Y=x['time'], Q=x['time_dop']), axis=1))

    dtf_res=dtf_res.set_index('dt')

    pde = pde.loc[~pde.index.isin(dtf_res.index)]
    if not pde.empty:
        pde.loc[:, 'country']=cntr
        pde.loc[:, 'time']=pde.index.year
        pde.loc[:, 'time_dop'] = pde.index.quarter
        dtf_res=pd.concat([dtf_res, pde])

    return dtf_res.reset_index(drop=True)[res_cols].sort_values(by=['time', 'time_dop'])


def main():
    indi_list_3 = ('NGDP_R_XDC', 'NGDP_R_K_IX', 'NGDP_R_PC_CP_A_PT', 'NGDP_R_PC_PP_PT', 'FIGB_PA', 'FITB_PA', 'BSCICP03',
    'CSCICP03', 'LOLITOAA', 'FIMM_PA', 'FPE_IX', 'FPE_EOP_IX', 'NGDP_R_PC_CP_A_PT', 'PCPI_IX',
    'BCA_BP6_USD', 'NGDP_XDC', 'ENDA_XDC_USD_RATE', 'H_A_M_A_XDC', 'N_A_M_A_XDC', 'Q_P_B_M_XDC_A', 'EER_REAL_2010_100',
    'Q_USD_EXCHANGE_A', 'DT_DOD_DECT_CD_GG_AR_US')

    cntry_list = ('Argentina', 'Brazil', 'Chile', 'Ecuador', 'India', 'Indonesia', 'Kazakhstan', 'Malaysia', 'Mexico',
                  'Peru', 'Philippines', 'Russian Federation', 'South Africa', 'Thailand', 'Turkey')

    cntry_code_list= ('AR', 'BR', 'CL', 'EC', 'IN', 'ID', 'KZ', 'MY', 'MX', 'PE', 'PH', 'RU', 'ZA', 'TH', 'TR')
    #cntry_code_list = ('AR',)

    str_common_data = os.path.join('DATA')

    dbs_paths = {'IMF': os.path.join(str_common_data, 'imf.sqlite3'),
                 'BIS':os.path.join(str_common_data, 'bis.sqlite3'),
                 'OECD':os.path.join(str_common_data, 'oecd.sqlite3'),
                 'WB': os.path.join(str_common_data, 'worldbank.sqlite3')}

    imf_struct_db = os.path.join(str_common_data, 'IMF_STRUCT.sqlite3')

    all_data = cmm.get_needed_data(databases_path=dbs_paths, indicators=indi_list_3,
                                   countries=cntry_code_list).sort_values(by=['INDI', 'country', 'time', 'time_dop'])
    panel=all_data[['country', 'time', 'time_dop', 'INDI', 'value']].set_index(['country', 'time', 'time_dop', 'INDI'])
    #print(panel)

    ppp = flat_dtf(panel.unstack().reset_index())

    ppp=ppp.loc[ppp['time']>1970]

    #print(list(range(ppp['time'].min(), ppp['time'].max()+1)))

    min_year=ppp['time'].min()
    max_year = ppp['time'].max()

    # print(sparse_country_frame(ppp.loc[ppp['country']=='AR'], start_year=min_year, stop_year=max_year))

    result=pd.concat([sparse_country_frame(ppp.loc[ppp['country']==cntr], start_year=min_year, stop_year=max_year)
                     for cntr in ppp['country'].unique()], ignore_index=True)

    print(result)
    # result.to_csv(os.path.join(str_common_data, 'sparse.csv'), sep=';', index=False)

    # ppp.to_csv(os.path.join(str_common_data, 'BR.csv'), sep=';')
    # print(flat_dtf(ppp))

    # to_matlab(flat_dtf(ppp), os.path.join('MATLAB', 'selection1.mat'))  # write to matlab
    print('All done')



if __name__ == "__main__":

    main()

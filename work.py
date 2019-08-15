"""work with local databases"""
import COMMON.readers as cmm
import IMF.IMF_get_data as imf
import BIS.bis_get_data as bis
import OECD.oecd_get_data as oecd

import os, os.path
import sqlite3
import pandas as pd

import scipy.io

def to_matlab(pdf, filename):
    dct={cn:pdf.loc[pdf['country'] == cn].set_index('country').to_dict('list') for cn in pdf['country'].unique().tolist()}
    scipy.io.savemat(filename, dct)

def flat_dtf(dtf):
    d2m = dtf.reset_index()
    clns = [cln[1] if cln[0] == 'value' else cln[0] for cln in d2m.columns]
    d2m.columns = clns
    return d2m

def main():

    indi_list = ('LOLITOAA', 'TXG_D_FOB_IX', 'TMG_D_CIF_IX', 'PXP_IX', 'PMP_IX', 'NGDP_R_XDC', 'PCPI_IX', 'FPOLM_PA', 'FM2_XDC',
                 'EREER_IX', 'FPE_IX', 'FPE_EOP_IX', 'FOSAON_XDC', 'FOSAOP_XDC', 'FILR_PA', 'BGS_BP6_USD',
                 'INDEX_2010_100_N', 'H_A_M_A_XDC', 'N_A_M_A_XDC', 'BSCICP03', 'CSCICP03')

    cntry_list = ('Argentina', 'Brazil', 'Chile', 'Ecuador', 'India', 'Indonesia', 'Kazakhstan', 'Malaysia', 'Mexico',
                  'Peru', 'Philippines', 'Russian Federation', 'South Africa', 'Thailand', 'Turkey')

    cntry_code_list= ('AR', 'BR', 'CL', 'EC', 'IN', 'ID', 'KZ', 'MY', 'MX', 'PE', 'PH', 'RU', 'ZA', 'TH', 'TR')
    #cntry_code_list = ('AR',)

    str_common_data = os.path.join('DATA')

    dbs_paths = {'IMF': os.path.join(str_common_data, 'imf.sqlite3'),
                 'BIS':os.path.join(str_common_data, 'bis.sqlite3'),
                 'OECD':os.path.join(str_common_data, 'oecd.sqlite3')}

    imf_struct_db = os.path.join(str_common_data, 'IMF_STRUCT.sqlite3')

    all_data = cmm.get_needed_data(databases_path=dbs_paths, indicators=indi_list, countries=cntry_code_list).sort_values(by=['INDI', 'country', 'time', 'time_dop'])
    panel=all_data[['country', 'time', 'time_dop', 'INDI', 'value']].set_index(['country', 'time', 'time_dop', 'INDI'])
    #print(panel)
    ppp = panel.unstack()

    # ppp.to_csv(os.path.join(str_common_data, 'BR.csv'), sep=';')
    flat_dtf(ppp).to_csv(os.path.join(str_common_data, 'TEST.csv'), sep=';', index=False)  # write to csv
    print(flat_dtf(ppp))

    to_matlab(flat_dtf(ppp), os.path.join('MATLAB', 'selection1.mat'))  # write to matlab



if __name__ == "__main__":

    main()

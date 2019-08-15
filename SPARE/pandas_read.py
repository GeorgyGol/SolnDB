import pandas as pd
import re
import numpy as np
import datetime as dt
import glob
import os.path

from pandas_datareader import data, wb

def WorldBank():
    ci=data.get_indicators()
    cntr=data.get_countries()

    print(ci)

    #ci.to_csv('induks.csv', sep=';')
    #cntr.to_csv('coutries.csv', sep=';')


strXLSData='рмэз_все волны.xlsx'
lst_strange=['ID_W', 'REDID_H', 'ID_H']
lst_vars=['E13.72A', 'F14.6', 'F14.71', 'F14.72', 'F14.8']

def read_descr(strFile=strXLSData):
    pdfDescr=pd.read_excel(strFile, sheet_name='описание', header=None, names=['code', 'description'])
    pdfDescr['wave_num']=pdfDescr['description'].str.extract('(\d+)', expand=False)

    lst_vars = pdfDescr.loc[pdfDescr['code'].str.contains('\d+'), 'code'].tolist()
    print(lst_vars)
    pdfDescr.set_index('code')
    return pdfDescr

def read_waves(strFile=strXLSData):
    pdfData=pd.read_excel(strFile, sheet_name='данные', header=0)
    return pdfData

def main_update():
    pdf=read_waves()
    id_cols=[c for c in pdf.columns.tolist() if c not in lst_strange+lst_vars]
    all_cols=pdf.columns.tolist()
    pdf['ID_H']=np.nan
    pdf['ID_W']=pdf['ID_W'].astype(int)
    print(id_cols)
    print(pdf.shape)
    cols=pdf.columns.tolist()

    waves = sorted(pdf['ID_W'].unique().tolist())

    pdf_i=pdf.loc[pdf['ID_W']==waves[-1]].reset_index(drop=True)
    pdf_i['ID_H']=pdf_i.index

    for n, cur_i in enumerate(id_cols[:12]):
        print(n+5, cur_i, end=' ')


        p_update = pdf_i.loc[pdf_i[cur_i].notnull(), [cur_i, 'ID_H']].set_index(cur_i)
        pdf=pdf.reset_index().set_index(cur_i)
        mask = (pdf['ID_W'] == n + 5) & pdf.index.notnull()
        pdf.loc[mask].update(p_update)
        print('='*30, end='\n')


    print('All done')
    #print('NOT INSTALL ', pdf.loc[pdf['ID_H'].isnull()].shape)
    #print(pdf)

    #print(pdf[pdf.columns.difference(lst_strange+lst_vars)].shape)


def main():
    pdf = read_waves()
    id_cols = [c for c in pdf.columns.tolist() if c not in lst_strange + lst_vars]
    all_cols = pdf.columns.tolist()

    pdf['ID_H'] = np.nan
    pdf['ID_W'] = pdf['ID_W'].astype(int)
    print(id_cols)
    print(pdf.shape)
    cols = pdf.columns.tolist()

    waves = sorted(pdf['ID_W'].unique().tolist())

    pdf_i = pdf.loc[pdf['ID_W'] == waves[-1]].reset_index(drop=True)
    pdf_i['ID_H'] = pdf_i.index

    for n, cur_i in enumerate(id_cols):
        print(n + 5, cur_i, end=' ')

        p_update = pdf_i.loc[pdf_i[cur_i].notnull(), [cur_i, 'ID_H']]

        mask = (pdf['ID_W'] == n + 5) & pdf[cur_i].notnull()
        ppp=pdf.loc[mask].join(p_update, on=cur_i, rsuffix='_{}'.format(n+5))
        print(ppp.head(5))
        print('=' * 30, end='\n')

    print('All done')

def print_catalog(strDir=r'D:\EMuleIncome\Книги\Data Scientist books'):
    strNetPath = r'\\L26-srv1\Книги'
    def replace_path(strS):
        return strS.replace(strDir, strNetPath)

    files=glob.glob(strDir+r'\*.*')

    for f in sorted(files):
        print(f.replace(strDir, strNetPath))
    with open(os.path.join(strDir, 'catalog.txt'), 'w') as catalog:
        catalog.write('\n'.join(list(map(replace_path, files))))

if __name__ == "__main__":
    #main()
    print_catalog()
    #pdf = read_waves()
    #id_cols = [c for c in pdf.columns.tolist() if c not in lst_strange + lst_vars]

    #print(pdf.loc[pdf[id_cols].duplicated()])

    #ppp=pdf.groupby(by=['ID_W', 'ID_H']).size().reset_index(name='counts')
    #print(ppp.loc[ppp['counts']>1])
    #print(pd.__version__)

    #print(dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S'))
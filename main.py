"""Create and update local databases from WWW-resources"""
import COMMON.readers as cmm
import IMF.IMF_get_data as imf
import BIS.bis_get_data as bis
import OECD.oecd_get_data as oecd
import WB.get_data as wb

import os, os.path
import sqlite3
import pandas as pd

if __name__ == "__main__":
    db_imf_path=os.path.join('DATA', 'imf.sqlite3')
    db_bis_path = os.path.join('DATA', 'bis.sqlite3')
    db_oecd_path = os.path.join('DATA', 'oecd.sqlite3')
    db_wb_path = os.path.join('DATA', 'worldbank.sqlite3')

    str_panel_file=os.path.join('DATA', 'full_data.csv')

    # imf.create_db(name=db_imf_path,
    #                  indi_file=os.path.join('INIT', 'IMF', 'codes_need.csv'),
    #                  country_file=os.path.join('INIT', 'IMF', 'work_countries.txt'))

    # imf.update_db(db_name=db_imf_path, start=2017)

    wb.create_db(name=db_wb_path,
                  indi_file=os.path.join('INIT', 'WB', 'codes_need.csv'),
                  country_file=os.path.join('INIT', 'WB', 'work_countries.txt'))

    # wb.update_db(db_name=db_wb_path, start=2017)

    # bis.create_db(name=db_bis_path,
    #                  indi_file=os.path.join('INIT', 'BIS', 'codes_need.csv'),
    #                  country_file=os.path.join('INIT', 'BIS', 'work_countries.txt'))
    #
    # bis.update_db(db_name=db_bis_path, start=2017)

    # oecd.create_db(name=db_oecd_path,
    #                 indi_file=os.path.join('INIT', 'OECD', 'codes_need.csv'),
    #                 country_file=os.path.join('INIT', 'OECD', 'work_countries.txt'))

    #oecd.update_db(db_name=db_oecd_path, start=2017)

    # fpdtf=cmm.make_full_panel_dtf(strIMF_DB_path=db_imf_path, strBIS_DB_path=db_bis_path,
    #                               strOECD_DB_path=db_oecd_path, strWB_DB_path=db_wb_path)
    #
    # fpdtf.to_csv(str_panel_file, sep=';')

    #con=sqlite3.connect(db_imf_path)
    #pdf=pd.read_sql('SELECT * FROM INDICATORS_FULL', con=con)
    #print(pdf.shape[0])

    print('All done')
import requests
import pandas as pd
import re
import numpy as np
import datetime as dt
import glob
import os.path

strBaseURL=r'https://api.ukdataservice.ac.uk/{version}/{resource}/{dataset}?user_key={key}'
strTestKey='2fea68f66a3c89baf4b712bbd43bd9a7'
strCMASFkey='80153f32adbbc375327d20f556ceb61a'

strVER='v1'
strRES='Datasets'

'''
    user:  GeorgyGolyshev
    login: g.golyshev@forecast.ru
    pass:  Croaton2018!

    App name: CMASF's App
    Key: 80153f32adbbc375327d20f556ceb61a
    Test key: 2fea68f66a3c89baf4b712bbd43bd9a7

    https://ukdataservice.ac.uk/use-data/guides/dataset-guides

'''


def main():
    pass


if __name__ == "__main__":
    main()
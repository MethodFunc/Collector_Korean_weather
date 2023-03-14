import math

import numpy as np
import pandas as pd
import pytz
import requests


class ASOSCollector:
    def __init__(self, api_key, location_code, start_date, end_date):
        self.asos_url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'

        self.params = {
            'serviceKey': api_key,
            'numOfRows': '24',
            'dataType': 'JSON',
            'dataCd': 'ASOS',
            'dateCd': 'HR',
            'startHh': '00',
            'endHh': '23',
            'stnIds': location_code,
            'startDt': start_date,
            'endDt': end_date

        }

    def get_contents(self):
        return requests.get(self.asos_url, params=self.params).json()

    def get_data(self):
        return self.convert_contents_to_dataframe()

    def convert_contents_to_dataframe(self):
        dataframe = pd.DataFrame(self.get_contents()['response']['body']['items']['item'])
        asos_rename_dict = {
            'tm': 'DATETIME',
            'ta': 'TEMPERATURE',
            'td': 'DEW_POINT_TEMPERATURE',
            'ts': 'GROUND_TEMPERATURE',
            'rn': 'PRECIPITATION',
            'ws': 'WIND_SPEED',
            'wd': 'WIND_DIRECTION',
            'hm': 'HUM',
            'pv': 'VAPOR_PRESSURE',
            'pa': 'ATMOSPHERIC_PRESSURE',
            'ps': 'SEA_LEVEL_PRESSURE',
            'ss': 'SUNSHINE',
            'icsr': 'SOLOR_RADIATION',
            'dsnw': 'DRIFTED_SNOW',
            'dc10Tca': 'TOTAL_CLOUD_CORVER_10M',
            'dc10LmcsCa': 'MIDDLE_LOW_CLOUD_CORVER_10M',
            'lcsCh': 'LOW_LEVEL_CEILING_100M',
            'vs': 'VISIBILITY'
        }
        dataframe.rename(columns=asos_rename_dict, inplace=True)
        dataframe = dataframe[list(asos_rename_dict.values())]

        for col in dataframe.columns[1:]:
            try:
                dataframe[col] = dataframe[col].astype(np.float32)
            except ValueError:
                dataframe[col] = dataframe[col].apply(lambda x: math.nan if x == '' else x)
                try:
                    dataframe[col] = dataframe[col].astype(np.float32)
                except ValueError:
                    print(f'2 {col}')

        dataframe['DATETIME'] = pd.to_datetime(dataframe['DATETIME'])
        dataframe['DATETIME'] = dataframe['DATETIME'].apply(lambda x: x.tz_localize('Asia/Seoul').tz_convert(pytz.utc))

        return dataframe


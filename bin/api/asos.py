import math

import numpy as np
import pandas as pd
import pytz
import requests
from enum import Enum


class ASOSCollector:
    class Columns(Enum):
        DATETIME = 'tm'
        TEMPERATURE = 'ta'
        DEW_POINT_TEMPERATURE = 'td'
        GROUND_TEMPERATURE = 'ts'
        RAIN = 'rn'
        WIND_SPEED = 'ws'
        WIND_DIRECTION = 'wd'
        HUMIDITY = 'hm'
        VAPOR_PRESSURE = 'pv'
        ATMOSPHERIC_PRESSURE = 'pa'
        SEA_LEVEL_PRESSURE = 'ps'
        SUNSHINE = 'ss'
        SOLAR_RADIATION = 'icsr'
        DRIFTED_SNOW = 'dsnw'
        TOTAL_CLOUD_COVER_10M = 'dc10Tca'
        MIDDLE_LOW_CLOUD_COVER_10M = 'dc10LmcsCa'
        LOW_LEVEL_CEILING_100M = 'lcsCh'
        VISIBILITY = 'vs'

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
        try:
            content = requests.get(self.asos_url, params=self.params).json()
        except requests.exceptions.ConnectionError as e:
            print(f'ASOS Connect Error: {e}')
        except requests.exceptions.Timeout as e:
            print(f'ASOS Timeout!: {e}')

        return content

    def get_data(self):
        return self.convert_contents_to_dataframe()

    def convert_contents_to_dataframe(self):
        dataframe = pd.DataFrame(self.get_contents()['response']['body']['items']['item'])
        asos_rename_dict = {col.value: col.name for col in self.Columns}

        dataframe.rename(columns=asos_rename_dict, inplace=True)
        dataframe = dataframe[list(self.Columns.__members__.keys())]

        for col in dataframe.columns[1:]:
            try:
                dataframe[col] = dataframe[col].astype(np.float32)
            except ValueError:
                dataframe[col] = dataframe[col].apply(lambda x: math.nan if x == '' else x)
                try:
                    dataframe[col] = dataframe[col].astype(np.float32)
                except ValueError:
                    print(f'Cannot convert {col} column to float32')

        dataframe['DATETIME'] = pd.to_datetime(dataframe['DATETIME'])
        dataframe['DATETIME'] = dataframe['DATETIME'].apply(lambda x: x.tz_localize('Asia/Seoul').tz_convert(pytz.utc))

        return dataframe


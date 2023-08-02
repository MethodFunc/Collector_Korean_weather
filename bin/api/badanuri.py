import datetime
from collections import OrderedDict

import numpy as np
import pandas as pd
import pytz
import requests

DATEFORMAT = '%Y%m%d'


def convert_date(date):
    try:
        record_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        record_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M")

    return record_date


class OceanCollection:
    def __init__(self, api_key, start_date, end_date, location_code: str = 'TW_0079'):
        # 'TW_0079'
        self.params = {
            'ServiceKey': api_key,
            'ObsCode': location_code,
            'ResultType': 'json',
        }
        self.start_date = start_date
        self.end_date = end_date
        self.data = OrderedDict()
        self.data['WIND_SPEED'] = {}
        self.data['WIND_DIRECTION'] = {}
        self.data['TEMP'] = {}
        self.data['HPA'] = {}
        self.data['WAVE_HEIGHT'] = {}

    def get_contents(self, url):
        try:
            content = requests.get(url, params=self.params).json()
        except requests.exceptions.ConnectionError as e:
            print(f'BADANURI Connect Error: {e}')
        except requests.exceptions.Timeout as e:
            print(f'BADANURI Timeout!: {e}')

        return content

    def get_content_value(self, content, id_name):
        ids = {
            'wind': {'WIND_SPEED': 'wind_speed', 'WIND_DIRECTION': 'wind_dir'},
            'air': {'TEMP': 'air_temp'},
            'pres': {'HPA': 'air_pres'},
            'wh': {'WAVE_HEIGHT': 'wave_height'}
        }
        get_columns = ids.get(id_name)

        for post in content['result']['data']:
            record_date = convert_date(post['record_time'])
            for key, values in get_columns.items():
                try:
                    self.data[key].setdefault(record_date, post[values])
                except KeyError:
                    self.data[key].setdefault(record_date, np.NaN)

    def total_data(self):

        wind_url = r'http://www.khoa.go.kr/api/oceangrid/tidalBuWind/search.do?'
        air_url = r'http://www.khoa.go.kr/api/oceangrid/tidalBuAirTemp/search.do?'
        pres_url = r'http://www.khoa.go.kr/api/oceangrid/tidalBuAirPres/search.do?'
        wh_url = r'http://www.khoa.go.kr/api/oceangrid/obsWaveHight/search.do?'
        date_ = datetime.datetime.strptime(self.start_date, DATEFORMAT)

        while True:
            date = date_.strftime(DATEFORMAT)
            self.params.update(Date=date)

            date_ = date_ + datetime.timedelta(days=1)
            wind_content = self.get_contents(wind_url)
            air_content = self.get_contents(air_url)
            pres_content = self.get_contents(pres_url)
            wh_content = self.get_contents(wh_url)
            try:
                self.get_content_value(wind_content, 'wind')
                self.get_content_value(air_content, 'air')
                self.get_content_value(pres_content, 'pres')
                self.get_content_value(wh_content, 'wh')

            except KeyError:
                print(date_)

            if date == self.end_date:
                break

        return self.data

    def get_data(self):
        self.total_data()
        dataframe = pd.DataFrame(self.data)
        dataframe.index.name = 'DATETIME'
        dataframe.reset_index(inplace=True)
        dataframe['DATETIME'] = dataframe['DATETIME'].apply(lambda x: x.tz_localize('Asia/Seoul').tz_convert(pytz.utc))

        return dataframe

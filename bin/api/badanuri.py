import datetime
from collections import OrderedDict

import numpy as np
import pandas as pd
import pytz
import requests


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
        self.data['wind_speed'] = {}
        self.data['wind_direction'] = {}
        self.data['temp'] = {}
        self.data['hpa'] = {}
        self.data['wave_height'] = {}

    def get_contents(self, url):
        response = requests.get(url, params=self.params).json()

        return response

    def get_content_value(self, content, id_name):
        ids = {
            'wind': {'wind_speed': 'wind_speed', 'wind_direction': 'wind_dir'},
            'air': {'temp': 'air_temp'},
            'pres': {'hpa': 'air_pres'},
            'wh': {'wave_height': 'wave_height'}
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
        date_ = datetime.datetime.strptime(self.start_date, "%Y%m%d")

        while True:
            date = date_.strftime("%Y%m%d")
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


if __name__ == '__main__':
    oc = OceanCollection()
    result = oc.total_data(2020, 1, 1, '20200102')
    import pandas as pd

    df = pd.DataFrame(result)
    print(df.shape)
    print(df.isna().sum())
    print(df)
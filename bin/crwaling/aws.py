from collections import OrderedDict
from datetime import timedelta, datetime

import numpy as np
import pytz
import requests
from bs4 import BeautifulSoup as bs

from .tool import kr_en_convert, columns_type

DATAFORMAT = '%Y%m%d'


def aws_exec(url, set_date: datetime):
    response = requests.get(url)
    contents = response.content
    site_code = bs(contents, 'html.parser', from_encoding='cp949')
    table_columns_before = site_code.find('tr', 'name').find_all('td')
    convert_table_columns = kr_en_convert(table_columns_before)
    table = site_code.find_all('tr', 'text')

    value_nan = {
        '\xa0': np.nan,
        '.': np.nan,
        ' ': np.nan
    }

    aws_list = []
    kst = pytz.timezone('Asia/Seoul')
    for table_data in table:
        aws_data = OrderedDict()
        table_in_data = [data.text for data in table_data]
        current_date = set_date.strftime(DATAFORMAT)
        prev_date = (set_date - timedelta(days=1)).strftime(DATAFORMAT)
        for data, col in zip(table_in_data, convert_table_columns):
            if col == 'datetime':
                data = current_date + ' ' + data if data == '00:00' or set_date.hour != 0 else prev_date + ' ' + data
                local_time = kst.localize(datetime.strptime(data, '%Y%m%d %H:%M'))
                value = local_time.astimezone(pytz.utc)
            elif col in ['rain', 'wd1s', 'wd10s']:
                continue
            else:
                value = value_nan.get(data, data)
                value = columns_type(col)(value)

            aws_data[col] = value

        aws_list.append(aws_data)
        del aws_data

    return aws_list

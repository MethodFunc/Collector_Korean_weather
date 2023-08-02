from datetime import timedelta
from enum import Enum
from .connection import MongoConnect
from bin.crwaling.aws import aws_exec
from bin.api.badanuri import OceanCollection
from bin.api.asos import ASOSCollector
from database.postgresql.connection import _create_engine

AWS_DATEFORMAT = '%Y%m%d%H%M'
API_DATEFORMAT = '%Y%m%d'


class StdType(str, Enum):
    AWS = 'aws'
    BADANURI = 'badanuri'
    ASOS = 'asos'


def insert_database(data, std_in, config, log):
    database_uri = config['mongodb']
    std_type = config['std_type']
    try:
        with MongoConnect(database_uri, config[f'{std_type}_database'], f'{std_in}_DATA') as conn:
            conn.insert_item(data)
    except Exception as e:
        log.error(f'Error in url_to_database: {e}')


def url_to_database(set_date, std_nm, std_in, config, log):
    date_string = set_date.strftime(AWS_DATEFORMAT)
    log.info(f'{std_nm} {date_string} collect start')
    url = f'https://www.weather.go.kr/cgi-bin/aws/nph-aws_txt_min_cal_test?{date_string}&0&MINDB_01M&{std_in}&m&M'
    data = aws_exec(url, set_date)
    insert_database(data, std_in, config, log)


# API 데이터 수집 상위 코드
def api_to_database(set_date, std_nm, std_in, config, log):
    date = set_date - timedelta(days=1)
    date = date.strftime(API_DATEFORMAT)

    if config['std_type'] == StdType.BADANURI:
        api_key = config['bada_api']
        oc = OceanCollection(location_code=std_in, api_key=api_key, start_date=date, end_date=date)
        dataframe = oc.get_data()
    elif config['std_type'] == StdType.ASOS:
        api_key = config['asos_api']
        asos = ASOSCollector(location_code=std_in, api_key=api_key, start_date=date, end_date=date)
        dataframe = asos.get_data()
    else:
        log.error('타입을 지원하지 않습니다.')

    log.info(f'{std_nm} {date} collect start')

    data = dataframe.to_dict('records')
    insert_database(data, std_in, config, log)

from datetime import datetime
from enum import Enum
from functools import partial

from joblib import Parallel, delayed

from database.crud import url_to_database, api_to_database


class StdType(str, Enum):
    AWS = 'aws'
    BADANURI = 'badanuri'
    ASOS = 'asos'


# 단일 장소 수집기
def single_collector(func, types_dict):
    std_nm = list(types_dict.keys())[0]
    std_in = list(types_dict.values())[0]
    func(std_nm=std_nm, std_in=std_in)


# 여러 장소 수집기(병렬)
def parallel_collector(func, types_dict):
    with Parallel(n_jobs=-1, backend='multiprocessing') as parallel:
        parallel(delayed(func)(std_nm=std_nm, std_in=std_in) for std_nm, std_in in
                 types_dict.items())


# 수집기 상위 코드
def collector(log, config):
    log.info('start')
    now = datetime.now()
    set_date = datetime(now.year, now.month, now.day, now.hour, minute=0)

    std_type_str = config['std_type']
    std_type_enum = StdType[std_type_str.upper()]
    types_dict = config[f'{std_type_str}_std']

    log.info(std_type_enum)
    log.info(std_type_str)

    # 크롤링을 통한 데이터베이스 적재
    if std_type_enum == StdType.AWS:
        log.info('AWS 크롤링 시작')
        active_func = partial(url_to_database, set_date=set_date, config=config, log=log)

    # API로 이용한 데이터베이스 적재
    elif std_type_enum == StdType.BADANURI or std_type_enum == StdType.ASOS:
        try:
            active_func = partial(api_to_database, set_date=set_date, config=config, log=log)
        except ValueError as e:
            raise log.error(f'타입을 지원하지 않습니다.\n{e}')
    else:
        raise log.error('잘못된 타입입니다.')

    if len(types_dict) == 1:
        single_collector(active_func, types_dict)

    elif len(types_dict) > 1:
        parallel_collector(active_func, types_dict)

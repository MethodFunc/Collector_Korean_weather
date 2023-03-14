import argparse
import logging
import logging.config
import os
from enum import Enum
from functools import partial

import yaml
from apscheduler.schedulers.background import BlockingScheduler

from bin.process import collector


class CollectorType(str, Enum):
    AWS = 'aws'
    ASOS = 'asos'
    BADANURI = 'badanuri'


# 설정 파일 불러오기 (전역)
with open('./settings.yaml', encoding='utf8') as f:
    config = yaml.load(f, yaml.FullLoader)


def load_logger(types):
    log_file = config['logging'].get(types, 'collector.log')
    logging_config_path = './logging.yaml'
    if os.path.exists(logging_config_path):
        with open(logging_config_path, 'rt') as f:
            logging_config = yaml.load(f, Loader=yaml.FullLoader)
            logging_config['handlers']['file']['filename'] = log_file
            logging.config.dictConfig(logging_config)
    else:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(types)

    return logger


def get_collector_type(s):
    try:
        return CollectorType[s.upper()]
    except KeyError:
        raise argparse.ArgumentTypeError(f'Invalid collector type: {s}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='Scheduler Collector',
                                     description='Collector asos aws badanuri with Mongodb')
    parser.add_argument('-t', '--std_type', type=get_collector_type,
                        help='asos, aws, badanuri 중 하나를 인자로 입력하시면 해당 데이터의 수집이 시작됩니다.')

    args = parser.parse_args()
    if not args.std_type:
        parser.error('collector type must be specified')

    config['std_type'] = args.std_type
    log = load_logger(args.std_type)

    # 스케줄러 시작
    schedule = BlockingScheduler(timezone='Asia/Seoul')

    # 중복된 인자 고정
    partial_collector = partial(collector, config=config, log=log)

    if args.std_type in CollectorType.__members__.values():
        log.info(f'{args.std_type} logger start')
        collectors = {
            CollectorType.BADANURI: {'hour': '0', 'minute': '31', 'id': 'badanuri'},
            CollectorType.ASOS: {'minute': '10', 'second': '0', 'id': 'collector_asos'},
            CollectorType.AWS: {'hour': '11', 'minute': '1', 'second': '0', 'id': 'collector_aws'},
        }
        collector = collectors[args.std_type]
        schedule.add_job(partial_collector, 'cron', **collector)
    else:
        log.error(f'{args.std_type} is not support')
    schedule.start()

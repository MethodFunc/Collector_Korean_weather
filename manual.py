import argparse
from collectable import config, get_collector_type, load_logger
from datetime import datetime, timedelta
from database.mongo.crud import AWS_DATEFORMAT
from bin.crwaling.aws import aws_exec
from database.postgresql.crud import process_psql, aws_to_psql
from tqdm import tqdm
from joblib import Parallel, delayed
from functools import partial


def parallel_collector(func, types_dict):
    with Parallel(n_jobs=-1, backend='multiprocessing') as parallel:
        parallel(delayed(func)(std_nm=std_nm, std_in=std_in) for std_nm, std_in in
                 types_dict.items())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='Scheduler Collector',
                                     description='Collector asos aws badanuri with Mongodb')
    parser.add_argument('-t', '--std_type', type=get_collector_type, default='aws',
                        help='asos, aws, badanuri 중 하나를 인자로 입력하시면 해당 데이터의 수집이 시작됩니다.')

    args = parser.parse_args()
    if not args.std_type:
        parser.error('collector type must be specified')

    config['std_type'] = args.std_type
    log = load_logger(args.std_type)

    start_date = datetime(2020, 10, 1)
    end_date = datetime.now()

    daterange = int((end_date - start_date).total_seconds() / 60 / 60)

    for i in tqdm(range(daterange)):
        set_date = start_date + timedelta(hours=i)
        # date_string = set_date.strftime(AWS_DATEFORMAT)
        active_func = partial(aws_to_psql, set_date=set_date, config=config, log=log)
        parallel_collector(active_func, config["aws_std"])

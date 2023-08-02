import sqlalchemy
from sqlalchemy.orm import Session

from bin.crwaling.aws import aws_exec
from database.postgresql import schemas, models, connection
from math import nan
from sqlalchemy import update, insert
from copy import deepcopy


AWS_DATEFORMAT = '%Y%m%d%H%M'


def aws_to_psql(set_date, std_nm, std_in, config, log):
    date_string = set_date.strftime(AWS_DATEFORMAT)
    log.info(f'{std_nm} {date_string} collect start')
    url = f'https://www.weather.go.kr/cgi-bin/aws/nph-aws_txt_min_cal_test?{date_string}&0&MINDB_01M&{std_in}&m&M'
    data = aws_exec(url, set_date)
    process_psql(data, std_in, config)


def process_psql(data, std_in, config):
    engine, session = connection._create_engine(config)
    table_name = f"AWS_{std_in}"

    for value in data[::-1]:
        item = {
            "DATETIME": value.get("DATETIME"),
            "RAIN15": value.get("RAIN_15M", nan),
            "RAIN60": value.get("RAIN_60M", nan),
            "RAIN3H": value.get("RAIN_3H", nan),
            "RAIN6H": value.get("RAIN_6H", nan),
            "RAIN12H": value.get("RAIN_12H", nan),
            "RAIN1D": value.get("RAIN_1D", nan),
            "TEMP": value.get("TEMP", nan),
            "WD1": value.get("WIND_DIRECTION_1M", nan),
            "WS1": value.get("WIND_SPEED_1M", nan),
            "WD10": value.get("WIND_DIRECTION_10M", nan),
            "WS10": value.get("WIND_SPEED_10M", nan),
            "HUMIDITY": value.get("HUM", nan),
            "HPA": value.get("HPA", nan)

        }
        item = schemas.AwsItem(**item)
        try:
            with session() as sess:
                insert_psql_data(engine, sess, table_name, item)
        except Exception as err:
            print(err)


def insert_psql_data(engine, db: Session, name: str, schema: schemas.AwsItem):
    table = models.create_model(name)
    connection.Base.metadata.create_all(bind=engine)

    data = {
        "DATETIME": schema.DATETIME,
        "RAIN15": schema.RAIN15,
        "RAIN60": schema.RAIN60,
        "RAIN3H": schema.RAIN3H,
        "RAIN6H": schema.RAIN6H,
        "RAIN12H": schema.RAIN12H,
        "RAIN1D": schema.RAIN1D,
        "TEMP": schema.TEMP,
        "WD1": schema.WD1,
        "WS1": schema.WS1,
        "WD10": schema.WD10,
        "WS10": schema.WS10,
        "HUMIDITY": schema.HUMIDITY,
        "HPA": schema.HPA
    }

    instance = db.query(table).filter_by(DATETIME=schema.DATETIME).first()
    if instance:
        update_data = deepcopy(data)
        for key, value in data.items():

            if key == "DATETIME":
                continue

            if hasattr(instance.__dict__[key], "hex") & hasattr(data[key], "hex"):
                if instance.__dict__[key].hex() == value.hex():
                    update_data.pop(key)
                    continue

            elif instance.__dict__[key] == value:
                update_data.pop(key)
                continue

        if len(update_data) != 1:
            print(f"{update_data['DATETIME']} data is not equal. update data...")
            update_psql_data(db, name, update_data)

        else:

            return instance

    insert_stmt = insert(table).values(data)
    db.execute(insert_stmt)
    db.commit()


def update_psql_data(db: Session, name: str, data):
    table = models.create_model(name)

    update_stmt = (
        update(table).where(table.DATETIME == data["DATETIME"]).values(data)
    )

    db.execute(update_stmt)
    db.commit()

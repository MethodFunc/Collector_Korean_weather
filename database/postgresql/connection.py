from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from urllib.parse import quote_plus


def _create_engine(config):
    pwd = quote_plus(config["psql_pwd"])
    URL = f"postgresql+psycopg2://{config['psql_user']}:{pwd}@{config['psql_host']}:{config['psql_port']}/{config['psql_db']}"

    engine = create_engine(URL)
    session_local = sessionmaker(autoflush=False, bind=engine)

    return engine, session_local


class Base(DeclarativeBase):
    pass



import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from data_common.config.configurer import get_conf

conf = get_conf()
# TODO move this to config
mysql_connection_string = f"mysql://cdap:cdap@35.199.173.91:3306/etl_{conf.env}"
logging.info(f'mysql_connection_string: \n{mysql_connection_string}')
engine = create_engine(mysql_connection_string, echo=True)
# use session_factory() to get a new Session
_SessionFactory = sessionmaker(bind=engine)
Base = declarative_base()


def session_factory():
    Base.metadata.create_all(engine)
    return _SessionFactory()


@contextmanager
def executor():
    session = session_factory()
    yield session
    session.close()

import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

@pytest.fixture(scope='session')
def aqua_connection_string():
    return os.environ['AQUA_CONNECTION_STRING']

@pytest.fixture(scope='session')
def engine(aqua_connection_string):
    return create_engine(aqua_connection_string, pool_size=5, pool_recycle=3600)

@pytest.fixture(scope='session')
def session(engine):
    with Session(engine) as session:
        yield session

class ValueStorage:
    valid_pull_rev = None
    revision = None
    out = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

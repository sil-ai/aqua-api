import os
import mock
import random
import pytest
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

@pytest.fixture(scope='session')
def aqua_connection_string():
    return os.environ['aqua_connection_string']

@pytest.fixture(scope='session')
def engine(aqua_connection_string):
    return create_engine(aqua_connection_string, pool_size=5, pool_recycle=3600)

@pytest.fixture(scope='session')
def session():
    with Session(engine) as session:
        yield session

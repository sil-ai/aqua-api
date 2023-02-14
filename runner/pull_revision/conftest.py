import os
import pytest
import random
from string import ascii_letters
from db_connect import get_session

@pytest.fixture(scope='session')
def session_object():
    return next(get_session())

@pytest.fixture(scope='session')
def engine(session_object):
    return session_object[0]

@pytest.fixture(scope='session')
def session(session_object):
    return session_object[1]

@pytest.fixture(scope='session')
def aqua_connection_string():
    return os.environ['AQUA_DB']

def get_fake_conn_string(original_string):    
    done = False
    #??? Maybe rework?
    while not done:
        idx_list = random.sample([i for i,__ in enumerate(original_string)],3)
        lst = list(original_string)
        for idx in idx_list:
            lst[idx]=random.choice(ascii_letters)
        fake_string = ''.join(lst)
        if fake_string != original_string:
            done=True
            return fake_string

@pytest.fixture(scope='session')
def fake_num():
    return 3

@pytest.fixture(scope='session')
def fake_strings(aqua_connection_string, fake_num):
    return [get_fake_conn_string(aqua_connection_string) for __ in range(fake_num)]

@pytest.fixture(scope='session')
def fake1(fake_strings):
    return fake_strings[0]

@pytest.fixture(scope='session')
def fake2(fake_strings):
    return fake_strings[1]

@pytest.fixture(scope='session')
def fake3(fake_strings):
    return fake_strings[2]
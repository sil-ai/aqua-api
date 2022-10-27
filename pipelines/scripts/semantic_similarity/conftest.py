import json
import pytest

@pytest.fixture(scope='session')
def valid_paths():
    return {'input': '1_2_merge_chunk42.csv', 'out': '.'}

@pytest.fixture(scope='session')
def json_output():
    return json.load(open('1_2_semsim_chunk42_10_27.json'))

class ValueStorage:
    valid_sem_sim = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

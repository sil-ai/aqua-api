import json
import pytest

@pytest.fixture(scope='session')
def valid_paths():
    return {'input': '2_3_merge_chunk42.csv', 'out': '.'}

@pytest.fixture(scope='session')
def json_output():
    return json.load(open('2_3_semsim_10_13.json'))

class ValueStorage:
    valid_sem_sim = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()
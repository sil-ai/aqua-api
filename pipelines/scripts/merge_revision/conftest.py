import pytest

@pytest.fixture(scope='session')
def valid_paths():
    return {'target': '2_gen_exo_10_10.txt',
           'reference': '3_2022_10_03.txt',
           'out': 'merge_result'}

class ValueStorage:
    valid_mergerevision = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

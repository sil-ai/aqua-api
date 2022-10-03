import pytest

@pytest.fixture(scope='session')
def valid_paths():
    return {'target': 'out', 'reference': 'out', 'out': 'semsim_result'}

class ValueStorage:
    valid_sem_sim = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

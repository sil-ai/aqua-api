import pytest

@pytest.fixture(scope='session')
def valid_paths():
    return {'chunked': '../split_revision/out', 'out': '.'}

class ValueStorage:
    valid_sem_sim = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

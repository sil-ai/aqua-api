import pytest

@pytest.fixture(scope='session')
def valid_paths():
    return {'target': 'target.txt', 'reference': 'reference.txt', 'out': 'align_result'}

class ValueStorage:
    valid_alignrevision = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

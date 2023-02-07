import pytest

class ValueStorage:
    results = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

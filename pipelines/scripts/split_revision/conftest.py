import pytest

class ValueStorage:
    valid_split_rev = None
    list_of_chunked_files = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

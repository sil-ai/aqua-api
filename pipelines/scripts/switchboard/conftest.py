import pytest
from dotenv import load_dotenv
load_dotenv('../pull_revision/.env')

class ValueStorage:
    valid_switchboard = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

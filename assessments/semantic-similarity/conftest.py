import pytest
import os

class ValueStorage:
    results = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()


@pytest.fixture
def base_url():
    return os.getenv("AQUA_URL")

@pytest.fixture
def header():
    return {"api_key": str(os.getenv("TEST_KEY"))}

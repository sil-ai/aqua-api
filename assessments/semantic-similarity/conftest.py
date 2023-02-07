import pytest
import os
import pandas as pd
# from app import SemanticSimilarity



class ValueStorage:
    results = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

@pytest.fixture(scope='session')
def rev1_2():
    return pd.read_pickle('combo.pkl')

@pytest.fixture
def base_url():
    return os.getenv("AQUA_URL")

@pytest.fixture
def header():
    key =  "Bearer" + " " + str(os.getenv("TEST_KEY"))
    return {"Authorization": key}
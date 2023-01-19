import pytest
import pandas as pd
from assess import SemanticSimilarity

@pytest.fixture(scope='session')
def model():
    return SemanticSimilarity().semsim_model

@pytest.fixture(scope='session')
def tokenizer():
    return SemanticSimilarity().semsim_tokenizer

class ValueStorage:
    results = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

@pytest.fixture(scope='session')
def rev1_2():
    return pd.read_pickle('combo.pkl')

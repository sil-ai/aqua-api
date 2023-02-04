import pytest
from app import SemanticSimilarity

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

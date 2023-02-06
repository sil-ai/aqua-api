import pytest
import pickle
from app import SemanticSimilarity

@pytest.fixture(scope='session')
def model():
    return SemanticSimilarity().semsim_model

@pytest.fixture(scope='session')
def tokenizer():
    return SemanticSimilarity().semsim_tokenizer

@pytest.fixture(scope='session')
def revisions():
    return pickle.load(open('./fixtures/revisions_feb_4.pkl','rb'))

@pytest.fixture(scope='session')
def swahili_revision(revisions):
    return revisions[1]

class ValueStorage:
    results = None

@pytest.fixture(scope='session')
def valuestorage():
    return ValueStorage()

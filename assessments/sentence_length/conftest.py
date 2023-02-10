import os
import pytest

@pytest.fixture
def base_url():
    return os.getenv("AQUA_URL")

@pytest.fixture
def header():
    key =  "Bearer" + " " + str(os.getenv("TEST_KEY"))
    return {"Authorization": key}

class AssessmentStorage:
    revision: int = 0
    reference: int = 0
    assessment_id: int = 0

@pytest.fixture(scope='session')
def assessment_storage():
    return AssessmentStorage()

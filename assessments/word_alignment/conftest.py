import os
import pytest

@pytest.fixture
def base_url():
    return os.getenv("API_URL")

@pytest.fixture
def header():
    key =  "Bearer" + " " + str(os.getenv("API_KEY"))
    return {"Authorization": key}
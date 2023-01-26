import os
import pytest

@pytest.fixture
def base_url():
    return os.getenv("AQUA_URL")

@pytest.fixture
def header():
    key =  "Bearer" + " " + str(os.getenv("TEST_KEY"))
    return {"Authorization": key}

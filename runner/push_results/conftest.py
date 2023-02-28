import os
import pytest

@pytest.fixture
def base_url():
    return os.getenv("AQUA_URL")

@pytest.fixture
def header():
    return {"api_key": str(os.getenv("TEST_KEY"))}

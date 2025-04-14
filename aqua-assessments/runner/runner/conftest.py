import os
import requests
import pytest
from dotenv import load_dotenv

load_dotenv('../../.env')

@pytest.fixture
def base_url():
    return os.getenv("AQUA_URL")


@pytest.fixture
def header():
    AQUA_URL = os.getenv("AQUA_URL")
    TEST_USER = os.getenv("TEST_USER")
    TEST_PASSWORD = os.getenv("TEST_PASSWORD")
    base_url = AQUA_URL

    response = requests.post(
            base_url+"/token", data={"username": TEST_USER, "password": TEST_PASSWORD}
    )

    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}

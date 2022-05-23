import app
import pytest
from fastapi.testclient import TestClient

# Create a generator that when called gives
# a mock/ test API client, for our FastAPI app.
@pytest.fixture
def client():

    # Get a mock FastAPI app.
    mock_app = app.create_app()

    # Yield the mock/ test client for the FastAPI
    # app we spun up any time this generator is called.
    with TestClient(mock_app) as client:
        yield client


# Test for the root endpoint
def test_read_main(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}

# TODO
# Test for the List Versions endpoint
def test_list_versions(client):
    response = client.get("/version")
    # assert

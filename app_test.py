from pathlib import Path
import os

import app
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient


def test_key_auth():
    with pytest.raises(HTTPException) as err:
        app.api_key_auth(os.getenv("FAIL_KEY"))
    assert err.value.status_code == 401

    response = app.api_key_auth(os.getenv("TEST_KEY"))
    assert response == True

# Create a generator that when called gives
# a mock/ test API client, for our FastAPI app.
@pytest.fixture
def client():

    def skip_auth():
        return True

    # Get a mock FastAPI app.
    mock_app = app.create_app()
    #print(mock_app.dependency_overrides.keys())
    mock_app.dependency_overrides[app.api_key_auth] = skip_auth

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
    assert response.status_code == 200


def test_upload_bible(client):
    test_revision = {
            "version": 3,
            "published": False
            }

    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}
    response = client.post("/upload_bible", params=test_revision, files=file)
    assert response.status_code == 200


def test_list_revisions(client):
    test_version = {
            "version": "TEST"
            }

    response = client.get("/list_revisions", params=test_version)
    assert response.status_code == 200


def test_get_chapter(client):
    test_chapter = {
            "revision": 3,
            "book": "GEN",
            "chapter": 1
            }

    response = client.get("/get_chapter", params=test_chapter)
    assert response.status_code == 200


def test_get_verse(client):
    test_version = {
            "revision": 3,
            "book": "GEN",
            "chapter": 1,
            "verse": 1
            }

    response = client.get("/get_verse", params=test_version)
    assert response.status_code == 200

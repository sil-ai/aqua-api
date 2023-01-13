from pathlib import Path
import os
from datetime import date
import ast

import app
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import numpy as np

import queries
import bible_loading


headers = {'x-hasura-admin-secret': os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
        )

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


def test_add_version(client):
    test_version = {
            "name": "delete", "isoLanguage": "eng",
            "isoScript": "Latn", "abbreviation": "DEL"
            }

    fail_version = {
            "name": "fail_delete", "isoLanguage": "eng",
            "isoScript": "Latn", "abbreviation": "DEL"
            }

    test_response = client.post("/version", params=test_version)
    fail_response = client.post("/version", params=fail_version)

    assert test_response.status_code == 200
    assert fail_response.status_code == 400


# Test for the List Versions endpoint
def test_list_versions(client):
    response = client.get("/version")

    assert response.status_code == 200


def test_upload_bible(client):
    test_abv_revision = {
            "version_abbreviation": "DEL",
            "published": False
            }
 
    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}
    response_abv = client.post("/revision", params=test_abv_revision, files=file)

    assert response_abv.status_code == 200


def test_list_revisions(client):
    test_version = {
            "version_abbreviation": "DEL"
            }

    fail_version = {
            "version_abbreviation": "FAIL"
            }

    test_response = client.get("/revision", params=test_version)
    fail_response = client.get("/revision", params=fail_version)
    
    assert test_response.status_code == 200
    assert fail_response.status_code == 400


def test_get_chapter(client): 
    version_abv = {
            "version_abbreviation": "DEL"
            }

    version_response = client.get("/revision", params=version_abv)
    version_fixed = ast.literal_eval(version_response.text)

    for version_data in version_fixed:
        if version_data["versionName"] == "delete":
            revision_id = version_data["id"]

    test_chapter = {
            "revision": revision_id,
            "book": "GEN",
            "chapter": 1
            }

    response = client.get("/chapter", params=test_chapter)

    assert response.status_code == 200


def test_get_verse(client): 
    version_abv = {
            "version_abbreviation": "DEL"
            }

    version_response = client.get("/revision", params=version_abv)
    version_fixed = ast.literal_eval(version_response.text)

    for version_data in version_fixed:
        if version_data["versionName"] == "delete":
            revision_id = version_data["id"]

    test_version = {
            "revision": revision_id,
            "book": "GEN",
            "chapter": 1,
            "verse": 1
            }

    response = client.get("/verse", params=test_version)

    assert response.status_code == 200


def test_delete_revision(client):
    
    version_abv = {
           "version_abbreviation": "DEL"
           }

    version_response = client.get("/revision", params=version_abv)
    version_fixed = ast.literal_eval(version_response.text)

    for version_data in version_fixed:
        if version_data["versionName"] == "delete":
            revision_id = version_data["id"]

    delete_revision_data = {
            "revision": revision_id
            }

    delete_response = client.delete("/revision", params=delete_revision_data)

    assert delete_response.status_code == 200


def test_delete_version(client):
    test_delete_version = {
            "version_abbreviation": "DEL"
            }

    test_response = client.delete("/version", params=test_delete_version)

    assert test_response.status_code == 200

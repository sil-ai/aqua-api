from pathlib import Path
import os

import app
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import queries


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


# TODO
# Test for the List Versions endpoint
def test_list_versions(client):
    response = client.get("/list_versions")
    assert response.status_code == 200


def test_add_version(client):
    test_version = {
            "name": "delete", "isoLanguage": "eng",
            "isoScript": "Latn", "abbreviation": "DEL"
            }

    fail_version = {
            "name": "test", "isoLanguage": "eng",
            "isoScript": "Latn", "abbreviation": "TEST"
            }

    test_response = client.get("/add_version", params=test_version)
    fail_response = client.get("/add_version", params=fail_version)

    delete_abv = '"' + test_version["abbreviation"] + '"'
    delete_test_version = queries.delete_bible_version(delete_abv)

    with Client(transport=transport,
            fetch_schema_from_transport=True) as mutation_client:

        mutation = gql(delete_test_version)
        deleted_version = mutation_client.execute(mutation)

    assert test_response.status_code == 200
    assert fail_response.status_code == 400


def test_upload_bible(client):
    test_none_revision = {
            "version_id": None,
            "version_abbreviation": None,
            "published": False
            }

    test_id_revision = {
            "version_id": 3,
            "version_abbreviation": "TEST",
            "published": False
            }

    test_abv_revision = {
            "version_id": None,
            "version_abbreviation": "TEST",
            "published": False}

    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}    
    response_none = client.post("/upload_bible", params=test_none_revision, files=file) 

    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}
    response_id = client.post("/upload_bible", params=test_id_revision, files=file)
    
    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}
    response_abv = client.post("/upload_bible", params=test_abv_revision, files=file)

    revision_id = response_id.json()["Revision ID"]
    revision_abv = response_abv.json()["Revision ID"]

    delete_response_id = queries.delete_bibleRevision_text(revision_id)
    delete_response_abv = queries.delete_bibleRevision_text(revision_abv)

    with Client(transport=transport,
            fetch_schema_from_transport=True) as mutation_client:

        mutation_id = gql(delete_response_id)
        mutation_abv = gql(delete_response_abv)

        revised_id = mutation_client.execute(mutation_id)
        revised_abv = mutation_client.execute(mutation_abv)

    assert response_none.status_code == 400
    assert response_id.status_code == 200
    assert response_abv.status_code == 200


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

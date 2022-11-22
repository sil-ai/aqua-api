from pathlib import Path
import os
from datetime import date

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


# Test for the List Versions endpoint
def test_list_versions(client):
    response = client.get("/version")
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

    test_response = client.post("/version", params=test_version)
    fail_response = client.post("/version", params=fail_version)

    delete_abv = '"' + test_version["abbreviation"] + '"'
    delete_test_version = queries.delete_bible_version(delete_abv)

    with Client(transport=transport,
            fetch_schema_from_transport=True) as mutation_client:

        mutation = gql(delete_test_version)
        deleted_version = mutation_client.execute(mutation)

    assert test_response.status_code == 200
    assert fail_response.status_code == 400


def test_delete_version(client):
    add_version = queries.add_version_query(
            '"test"', '"eng"', '"Latn"', 
            '"DEL"', "null", "null", 
            "null", "false"
            )

    with Client(transport=transport,
            fetch_schema_from_transport=True) as mutation_client:

        mutation = gql(add_version)
        added_version = mutation_client.execute(mutation)

    test_delete_version = {
            "version_abbreviation": "DEL"
            }

    test_response = client.delete("/version", params=test_delete_version)

    assert test_response.status_code == 200


def test_list_revisions(client):
    fail_version = {
            "version_abbreviation": "FAIL"
            }

    test_version = {
            "version_abbreviation": "TEST"
            }

    fail_response = client.get("/revision", params=fail_version)
    test_response = client.get("/revision", params=test_version)

    assert fail_response.status_code == 400
    assert test_response.status_code == 200


def test_upload_bible(client):
    test_abv_revision = {
            "version_abbreviation": "TEST",
            "published": False}
 
    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}
    response_abv = client.post("/revision", params=test_abv_revision, files=file)

    revision_abv = response_abv.json()["Revision ID"]

    delete_verse_response_abv = queries.delete_verses_mutation(revision_abv)
    delete_revision_response_abv = queries.delete_revision_mutation(revision_abv)

    with Client(transport=transport,
            fetch_schema_from_transport=True) as mutation_client:

        verse_mutation_abv = gql(delete_verse_response_abv)
        revision_mutation_abv = gql(delete_revision_response_abv)

        verse_revised_abv = mutation_client.execute(verse_mutation_abv)
        revision_revised_abv = mutation_client.execute(revision_mutation_abv)

    assert response_abv.status_code == 200


def test_delete_revision(client):
    test_upload_file = "fixtures/uploadtest.txt"

    revision_date = '"' + str(date.today()) + '"'
    create_revision_mutation = queries.insert_bible_revision(
            1, revision_date, "false"
            )

    with Client(transport=transport, fetch_schema_from_transport=True) as mutation_client:
        create_revision = gql(create_revision_mutation)
        create_revision_response = mutation_client.execute(create_revision)

    revision_id = create_revision_response["insert_bibleRevision"]["returning"][0]["id"]

    # Parse the input Bible revision data.
    verses = []
    bibleRevision = []

    with open(test_upload_file, "r") as bible_data:
        for line in bible_data:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(revision_id)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_id)

    # Push the revision to the database.
    bible_loading.upload_bible(verses, bibleRevision)

    delete_revision_data = {
            "revision": revision_id
            }

    delete_response = client.delete("/revision", params=delete_revision_data)

    assert delete_response.status_code == 200


def test_get_chapter(client):
    test_chapter = {
            "revision": 3,
            "book": "GEN",
            "chapter": 1
            }

    response = client.get("/chapter", params=test_chapter)
    assert response.status_code == 200


def test_get_verse(client):
    test_version = {
            "revision": 3,
            "book": "GEN",
            "chapter": 1,
            "verse": 1
            }

    response = client.get("/verse", params=test_version)
    assert response.status_code == 200

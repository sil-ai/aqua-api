from pathlib import Path
import os
import ast

import app
import pytest
import fastapi
from fastapi import HTTPException
from fastapi.testclient import TestClient
from gql.transport.requests import RequestsHTTPTransport
from pydantic.error_wrappers import ValidationError

import bible_routes.language_routes as language_routes
import bible_routes.version_routes as version_routes
import bible_routes.revision_routes as revision_routes
import bible_routes.verse_routes as verse_routes
import assessment_routes.assessment_routes as assessments_routes
import review_routes.results_routes as results_routes
from models import VersionIn, RevisionIn, AssessmentIn


version_name = 'App delete test'
version_abbreviation = 'APP-DEL'

headers = {'x-hasura-admin-secret': os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
        )

def test_key_auth():
    with pytest.raises(HTTPException) as err:
        version_routes.api_key_auth(os.getenv("FAIL_KEY"))
    assert err.value.status_code == 401

    response = version_routes.api_key_auth(os.getenv("TEST_KEY"))
    assert response is True


# Create a generator that when called gives
# a mock/ test API client, for our FastAPI app.
@pytest.fixture
def client():

    def skip_auth():
        return True

    # Get a mock FastAPI app.
    mock_app = fastapi.FastAPI()
    app.configure(mock_app)

    #print(mock_app.dependency_overrides.keys())
    mock_app.dependency_overrides[language_routes.api_key_auth] = skip_auth
    mock_app.dependency_overrides[version_routes.api_key_auth] = skip_auth
    mock_app.dependency_overrides[revision_routes.api_key_auth] = skip_auth
    mock_app.dependency_overrides[verse_routes.api_key_auth] = skip_auth
    mock_app.dependency_overrides[assessments_routes.api_key_auth] = skip_auth
    mock_app.dependency_overrides[results_routes.api_key_auth] = skip_auth


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
    test_version = VersionIn(
            name=version_name, 
            isoLanguage="eng", 
            isoScript="Latn",
            abbreviation=version_abbreviation,
        )

    test_response = client.post("/version", params=test_version.dict())
    fail_response = client.post("/version", params=test_version.dict())  # Push the same version a second time, which should give 400

    if test_response.status_code == 400 and test_response.json()['detail'] == "Version abbreviation already in use.":
        print("This version is already in the database")
    else:
        assert test_response.status_code == 200
        assert test_response.json()['name'] == version_name
    
    assert fail_response.status_code == 400


# Test for the List Versions endpoint
def test_list_versions(client):
    response = client.get("/version")

    assert response.status_code == 200


def test_upload_bible(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]

    test_abv_revision = RevisionIn(version_id=version_id)
    test_abv_revision_with_name = RevisionIn(
            version_id=version_id,
            published=False,
            name="App test upload",
                )
 
    test_upload_file = Path("fixtures/uploadtest.txt")
    file = {"file": test_upload_file.open("rb")}
    file_2 = {"file": test_upload_file.open("rb")}

    response_abv = client.post("/revision", params=test_abv_revision.dict(), files=file)
    assert response_abv.status_code == 200

    response_abv = client.post("/revision", params=test_abv_revision_with_name.dict(), files=file_2)

    assert response_abv.status_code == 200


def test_list_revisions(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    
    test_version = {
            "version_id": version_id
            }

    fail_version = {
            "version_id":999999
            }

    test_response = client.get("/revision", params=test_version)
    fail_response = client.get("/revision", params=fail_version)
    all_response = client.get("/revision")
    
    assert test_response.status_code == 200
    assert fail_response.status_code == 400
    assert all_response.status_code == 200


def test_get_languages(client): 
    language_response = client.get("/language")

    assert language_response.status_code == 200


def test_get_scripts(client): 
    script_response = client.get("/script")

    assert script_response.status_code == 200


def test_get_chapter(client): 
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    version_abv = {
            "version_id": version_id
            }

    revision_response = client.get("/revision", params=version_abv)
    revision_response_text = revision_response.text.replace("null", "None").replace('false', 'False')
    revision_fixed = ast.literal_eval(revision_response_text)

    for revision_data in revision_fixed:
        if revision_data["version_id"] == version_id:
            revision_id = revision_data["id"]

    test_chapter = {
            "revision_id": revision_id,
            "book": "GEN",
            "chapter": 1
            }

    response = client.get("/chapter", params=test_chapter)

    assert response.status_code == 200


def test_get_verse(client): 
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    version_abv = {
            "version_id": version_id
            }

    version_response = client.get("/revision", params=version_abv)
    version_response_text = version_response.text.replace("null", "None").replace('false', 'False')
    version_fixed = ast.literal_eval(version_response_text)

    for version_data in version_fixed:
        if version_data["version_id"] == version_id:
            revision_id = version_data["id"]

    test_version = {
            "revision_id": revision_id,
            "book": "GEN",
            "chapter": 1,
            "verse": 1
            }

    response = client.get("/verse", params=test_version)

    assert response.status_code == 200


def test_assessment(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    version_response = client.get("/revision", params=test_version_abv)
    version_response_text = version_response.text.replace("null", "None").replace('false', 'False')
    version_fixed = ast.literal_eval(version_response_text)

    for version_data in version_fixed:
        if version_data["version_id"] == version_id:
            revision_id = version_data["id"]
    print(f'{revision_id=}')
     
    with pytest.raises(ValidationError):
        AssessmentIn(
                revision_id="eleven",
                reference_id=10,
                type="dummy"
        )

    with pytest.raises(ValidationError):
        AssessmentIn(
                revision_id=11,
                reference_id=10,
                type="non-existent assessment"
        )
    
    bad_config_3 = AssessmentIn(
            revision_id=revision_id,
            type="word-alignment"
    )                                   # This should require a reference_id

    good_config = AssessmentIn(
            revision_id=revision_id,
            reference_id=10,
            type="dummy"
    )

    # Try to post bad config
    response = client.post("/assessment", params={**bad_config_3.dict(), 'modal_suffix': 'test'})
    assert response.status_code == 400

    # Post good config
    response = client.post("/assessment", params={**good_config.dict(), 'modal_suffix': 'test'})
    assert response.status_code == 200
    id = response.json()['id']

    # Verify good config id is now in assessments
    response = client.get("/assessment")
    assert response.status_code == 200
    assert id in [assessment['id'] for assessment in response.json()]

    # Remove good config from assessments
    response = client.delete("/assessment", params={'assessment_id': id})
    assert response.status_code == 200

    # Verify good config is no longer in assessments
    response = client.get("/assessment")
    assert response.status_code == 200
    assert id not in [assessment['id'] for assessment in response.json()]


def test_result(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    revision_response = client.get("/revision", params=test_version_abv)
    revision_response_text = revision_response.text.replace("null", "None").replace('false', 'False')
    revision_fixed = ast.literal_eval(revision_response_text)

    for revision_data in revision_fixed:
        if revision_data["version_id"] == version_id:
            revision_id = revision_data["id"]

    good_config = AssessmentIn(
            revision_id=revision_id,
            reference_id=10,
            type="dummy",
    )

    response = client.post("/assessment", params={**good_config.dict(), 'modal_suffix': 'test'})
    assert response.status_code == 200
    assessment_id = response.json()['id']
    
    test_config = {
            "assessment_id": assessment_id
    }

    fail_config = {
            "assessment_id": 0
            }

    test_response = client.get("/result", params=test_config)
    fail_response = client.get("/result", params=fail_config)
    
    assert test_response.status_code == 200
    assert fail_response.status_code == 400
    

def test_delete_revision(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    version_response = client.get("/revision", params=test_version_abv)
    version_response_text = version_response.text.replace("null", "None").replace('false', 'False')
    version_fixed = ast.literal_eval(version_response_text)

    for version_data in version_fixed:
        if version_data["version_id"] == version_id:
            revision_id = version_data["id"]

    delete_revision_data = {
            "id": revision_id,
            }

    fail_revision_data = {
            "id": 0
            }

    test_delete_response = client.delete("/revision", params=delete_revision_data)
    fail_delete_response = client.delete("/revision", params=fail_revision_data)

    assert test_delete_response.status_code == 200
    assert fail_delete_response.status_code == 400


def test_delete_version(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_delete_version = {
            "id": version_id
            }

    fail_delete_version = {
            "id": 999999
            }

    test_response = client.delete("/version", params=test_delete_version)
    fail_response = client.delete("/version", params=fail_delete_version)

    assert test_response.status_code == 200
    assert fail_response.status_code == 400

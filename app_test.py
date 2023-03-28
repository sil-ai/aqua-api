__version__ = 'v1'

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

import bible_routes.v2.language_routes as language_routes_v2
import bible_routes.v1.language_routes as language_routes_v1
import bible_routes.v1.version_routes as version_routes_v1
import bible_routes.v2.version_routes as version_routes_v2
import bible_routes.v1.revision_routes as revision_routes_v1
import bible_routes.v2.revision_routes as revision_routes_v2
import bible_routes.v1.verse_routes as verse_routes_v1
import bible_routes.v2.verse_routes as verse_routes_v2
import assessment_routes.v1.assessment_routes as assessment_routes_v1
import assessment_routes.v2.assessment_routes as assessment_routes_v2
import review_routes.v1.results_routes as results_routes_v1
import review_routes.v2.results_routes as results_routes_v2

from models import VersionIn, RevisionIn, AssessmentIn


version_name = 'App delete test'
version_abbreviation = 'APP-DEL'

headers = {'x-hasura-admin-secret': os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
        url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
        )

version_prefixes = ['v1', 'v2']


def test_key_auth_v1():
    with pytest.raises(HTTPException) as err:
        version_routes_v1.api_key_auth(os.getenv("FAIL_KEY"))
    assert err.value.status_code == 401

    response = version_routes_v1.api_key_auth(os.getenv("TEST_KEY"))
    assert response is True


def test_key_auth_v2():
    with pytest.raises(HTTPException) as err:
        version_routes_v2.api_key_auth(os.getenv("FAIL_KEY"))
    assert err.value.status_code == 401

    response = version_routes_v2.api_key_auth(os.getenv("TEST_KEY"))

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
    mock_app.dependency_overrides[language_routes_v1.api_key_auth] = skip_auth
    mock_app.dependency_overrides[version_routes_v1.api_key_auth] = skip_auth
    mock_app.dependency_overrides[revision_routes_v1.api_key_auth] = skip_auth
    mock_app.dependency_overrides[verse_routes_v1.api_key_auth] = skip_auth
    mock_app.dependency_overrides[assessment_routes_v1.api_key_auth] = skip_auth
    mock_app.dependency_overrides[results_routes_v1.api_key_auth] = skip_auth
    mock_app.dependency_overrides[language_routes_v2.api_key_auth] = skip_auth
    mock_app.dependency_overrides[version_routes_v2.api_key_auth] = skip_auth
    mock_app.dependency_overrides[revision_routes_v2.api_key_auth] = skip_auth
    mock_app.dependency_overrides[verse_routes_v2.api_key_auth] = skip_auth
    mock_app.dependency_overrides[assessment_routes_v2.api_key_auth] = skip_auth
    mock_app.dependency_overrides[results_routes_v2.api_key_auth] = skip_auth



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
    for prefix in version_prefixes:
        test_response = client.post(f"/{prefix}/version", params=test_version.dict())
        assert test_response.status_code == 200
        assert test_response.json()['name'] == version_name


# Test for the List Versions endpoint
def test_list_versions(client):
    for prefix in version_prefixes:
        response = client.get(f"/{prefix}/version")
        assert response.status_code == 200
        assert len(response.json()) > 0


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
    for prefix in version_prefixes:
        with open(test_upload_file, "r") as f:
            file = {"file": f}
            
            response_abv = client.post(f"/{prefix}/revision", params=test_abv_revision.dict(), files=file)
            assert response_abv.status_code == 200
        
        with open(test_upload_file, "r") as f:
            file_2 = {"file": f}

            response_abv = client.post(f"/{prefix}/revision", params=test_abv_revision_with_name.dict(), files=file_2)
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
    for prefix in version_prefixes:
        test_response = client.get(f"/{prefix}/revision", params=test_version)
        fail_response = client.get(f"/{prefix}/revision", params=fail_version)
        all_response = client.get(f"/{prefix}/revision")
    
        assert test_response.status_code == 200
        assert fail_response.status_code == 400
        assert all_response.status_code == 200


def test_get_languages(client):
    for prefix in version_prefixes:
        language_response = client.get(f"/{prefix}/language")


        assert language_response.status_code == 200


def test_get_scripts(client):
    for prefix in version_prefixes:
        script_response = client.get(f"/{prefix}/script")


        assert script_response.status_code == 200


def test_get_chapter(client): 
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    revision_response = client.get("/revision", params=test_version_abv)
    revision_response_text = revision_response.text.replace("null", "None").replace('false', 'False')
    revision_fixed = ast.literal_eval(revision_response_text)

    revision_ids = []
    for version_data in revision_fixed:
        if version_data["version_id"] == version_id:
            revision_ids.append(version_data["id"])

    test_chapter = {
            "revision_id": revision_ids[0],
            "book": "GEN",
            "chapter": 1
            }

    for prefix in version_prefixes:
        response = client.get(f"/{prefix}/chapter", params=test_chapter)

        assert response.status_code == 200


def test_get_verse(client): 
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    revision_response = client.get("/revision", params=test_version_abv)
    revision_response_text = revision_response.text.replace("null", "None").replace('false', 'False')
    revision_fixed = ast.literal_eval(revision_response_text)

    revision_ids = []
    for version_data in revision_fixed:
        if version_data["version_id"] == version_id:
            revision_ids.append(version_data["id"])

    test_version = {
            "revision_id": revision_ids[0],
            "book": "GEN",
            "chapter": 1,
            "verse": 1
            }

    for prefix in version_prefixes:
        response = client.get(f"/{prefix}/verse", params=test_version)

        assert response.status_code == 200


def test_assessment(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    revision_response = client.get("/revision", params=test_version_abv)
    revision_response_text = revision_response.text.replace("null", "None").replace('false', 'False')
    revision_fixed = ast.literal_eval(revision_response_text)

    revision_ids = []
    for version_data in revision_fixed:
        if version_data["version_id"] == version_id:
            revision_ids.append(version_data["id"])
     
    with pytest.raises(ValidationError):
        AssessmentIn(
                revision_id="eleven",
                reference_id=revision_ids[0],
                type="dummy"
        )

    with pytest.raises(ValidationError):
        AssessmentIn(
                revision_id=revision_ids[0],
                reference_id=revision_ids[1],
                type="non-existent assessment"
        )
    
    bad_config_3 = AssessmentIn(
            revision_id=revision_ids[0],
            type="word-alignment"
    )                                   # This should require a reference_id

    good_config = AssessmentIn(
            revision_id=revision_ids[0],
            reference_id=revision_ids[1],
            type="dummy"
    )

    for prefix in version_prefixes:
        # Try to post bad config
        response = client.post(f"/{prefix}/assessment", params=bad_config_3.dict())
        assert response.status_code == 400

        # Post good config
        response = client.post(f"/{prefix}/assessment", params={**good_config.dict(), 'modal_suffix': 'test'})
        assert response.status_code == 200
        id = response.json()['id']

        # Verify good config id is now in assessments
        response = client.get(f"/{prefix}/assessment")
        assert response.status_code == 200
        assert id in [assessment['id'] for assessment in response.json()]

        # Remove good config from assessments
        response = client.delete(f"/{prefix}/assessment", params={'assessment_id': id})
        assert response.status_code == 200

        # Verify good config is no longer in assessments
        response = client.get(f"/{prefix}/assessment")
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

    revision_ids = []
    for version_data in revision_fixed:
        if version_data["version_id"] == version_id:
            revision_ids.append(version_data["id"])

    good_config = AssessmentIn(
            revision_id=revision_ids[0],
            reference_id=revision_ids[1],
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

    test_config_chapter_agg = {
            "assessment_id": assessment_id,
            "aggregate": "chapter",
    }

    test_config_book_agg = {
            "assessment_id": assessment_id,
            "aggregate": "book",
    }

    test_config_text_agg = {
            "assessment_id": assessment_id,
            "aggregate": "text",
    }

    test_config_include_text = {
            "assessment_id": assessment_id,
            "include_text": True,
    }

    test_config_aggregate_and_include_text = {
            "assessment_id": assessment_id,
            "aggregate": "chapter",
            "include_text": True,
    }

    test_config_pagination = {
            "assessment_id": assessment_id,
            "page": 1,
            "page_size": 100,
    }

    for prefix in version_prefixes:
        test_response = client.get(f"/{prefix}/result", params=test_config)
        fail_response = client.get(f"/{prefix}/result", params=fail_config)
        assert test_response.status_code == 200
        assert fail_response.status_code == 404

        if prefix != 'v1':
            test_response_chapter_agg = client.get(f"/{prefix}/result", params=test_config_chapter_agg)
            test_response_book_agg = client.get(f"/{prefix}/result", params=test_config_book_agg)
            test_response_text_agg = client.get(f"/{prefix}/result", params=test_config_text_agg)
            test_response_include_text = client.get(f"/{prefix}/result", params=test_config_include_text)
            test_response_aggregate_and_include_text = client.get(f"/{prefix}/result", params=test_config_aggregate_and_include_text)
            test_response_pagination = client.get(f"/{prefix}/result", params=test_config_pagination)
            
            
            assert test_response_chapter_agg.status_code == 200
            assert test_response_book_agg.status_code == 200
            assert test_response_text_agg.status_code == 200
            assert test_response_include_text.status_code == 200
            assert test_response_aggregate_and_include_text.status_code == 400
            assert test_response_pagination.status_code == 200
        

def test_delete_revision(client):
    response = client.get("/version")
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_version_abv = {
            "version_id": version_id
            }

    version_response = client.get("/revision", params=test_version_abv)
    version_response_text = version_response.text.replace("null", "None").replace('false', 'False')
    version_fixed = ast.literal_eval(version_response_text)

    revision_ids = []
    for version_data in version_fixed:
        if version_data["version_id"] == version_id:
            revision_ids.append(version_data["id"])

    for prefix in version_prefixes:
        delete_revision_data = {
            "id": revision_ids.pop(),
            }

        fail_revision_data = {
            "id": 0
            }
        test_delete_response = client.delete(f"/{prefix}/revision", params=delete_revision_data)
        fail_delete_response = client.delete(f"/{prefix}/revision", params=fail_revision_data)

        assert test_delete_response.status_code == 200
        assert fail_delete_response.status_code == 400


def test_delete_version(client):
    for prefix in version_prefixes:
        response = client.get(f"/{prefix}/version")
        version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
        test_delete_version = {
                "id": version_id
                }

        fail_delete_version = {
                "id": 999999
                }

        test_response = client.delete(f"/{prefix}/version", params=test_delete_version)
        fail_response = client.delete(f"/{prefix}/version", params=fail_delete_version)

        assert test_response.status_code == 200
        assert fail_response.status_code == 400

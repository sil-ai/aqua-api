import json
import os
import time
from pathlib import Path

import modal
import pytest
import requests

import app

version_abbreviation = "NG-DEL"
version_name = "ngrams delete"


@pytest.fixture
def api_response():
    AQUA_URL = os.getenv("AQUA_URL")
    TEST_USER = os.getenv("TEST_USER")
    TEST_PASSWORD = os.getenv("TEST_PASSWORD")

    base_url = AQUA_URL
    response = requests.post(
        f"{base_url}/token", data={"username": TEST_USER, "password": TEST_PASSWORD}
    )

    return response


def test_trivial():
    assert 1 == 1


def test_response(api_response):
    assert api_response.status_code == 200


# Add a version to the database for this test
def test_add_version(api_response):
    base_url = os.getenv("AQUA_URL")
    header = {"Authorization": f"Bearer {api_response.json()['access_token']}"}

    test_version = {
        "name": version_name,
        "iso_language": "swh",
        "iso_script": "Latn",
        "abbreviation": version_abbreviation,
    }

    url = base_url + "/version"
    response = requests.post(url, json=test_version, headers=header)

    if (
        response.status_code == 400
        and response.json()["detail"] == "Version abbreviation already in use."
    ):
        print("This version is already in the database")
    else:
        assert response.json()["name"] == version_name


# Add one or more revisions to the database for this test
@pytest.mark.parametrize("filepath", [Path("fixtures/swh-ONEN.txt")])
def test_add_revision(api_response, filepath: Path):
    base_url = os.getenv("AQUA_URL")
    url = base_url + "/version"
    header = {"Authorization": f"Bearer {api_response.json()['access_token']}"}
    response = requests.get(url, headers=header)

    version_id = [
        version["id"]
        for version in response.json()
        if version["abbreviation"] == version_abbreviation
    ][0]

    test_abv_revision = {"version_id": version_id, "published": False}

    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"

    response_abv = requests.post(
        url, params=test_abv_revision, files=file, headers=header
    )

    assert response_abv.status_code == 200


app = modal.App(
    name="run-ngrams-test",
    image=modal.Image.debian_slim()
    .apt_install("libpq-dev", "gcc")
    .pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pydantic~=1.10.0",
        "pytest~=8.0.0",
        "requests~=2.31.0",
        "sqlalchemy~=1.4.0",
        "asyncpg~=0.27.0",
        "tqdm~=4.66.0",
        "nltk~=3.6.2",
    ),
)

run_ngrams = modal.Function.lookup("ngrams-test", "assess")


# app tests
@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def run_assess_draft(config):
    import os

    AQUA_DB = os.getenv("AQUA_DB")
    results_response = run_ngrams.remote(config, AQUA_DB)
    results = results_response["results"]

    assert len(results) > 2
    assert results[0]["assessment_id"] == 1
    assert len(results[0]["vrefs"]) == 2
    assert results[0]["ngram_size"] == 46


def test_assess_draft(api_response):
    from typing import Literal, Optional

    from pydantic import BaseModel

    base_url = os.getenv("AQUA_URL")
    header = {"Authorization": f"Bearer {api_response.json()['access_token']}"}

    class Assessment(BaseModel):
        id: Optional[int] = None
        revision_id: int
        min_n: Optional[int] = 2
        type: Literal["ngrams"]

    url = base_url + "/version"
    response = requests.get(url, headers=header)

    version_id = [
        version["id"]
        for version in response.json()
        if version["abbreviation"] == version_abbreviation
    ][0]

    url = base_url + "/revision"
    response = requests.get(url, params={"version_id": version_id}, headers=header)
    revision_id = response.json()[0]["id"]
    config = Assessment(id=1, revision_id=revision_id, min_n=40, type="ngrams")

    with app.run():
        run_assess_draft.remote(config.dict())


def test_delete_version(api_response):
    time.sleep(
        2
    )  # Allow the assessments above to finish pulling from the database before deleting!

    base_url = os.getenv("AQUA_URL")
    url = base_url + "/version"
    header = {"Authorization": f"Bearer {api_response.json()['access_token']}"}
    response = requests.get(url, headers=header)

    version_id = [
        version["id"]
        for version in response.json()
        if version["abbreviation"] == version_abbreviation
    ][0]

    test_delete_version = {"id": version_id}

    test_response = requests.delete(url, params=test_delete_version, headers=header)

    assert test_response.status_code == 200


# main
if __name__ == "__main__":
    # get api_response
    AQUA_URL = os.getenv("AQUA_URL")
    TEST_USER = os.getenv("TEST_USER")
    TEST_PASSWORD = os.getenv("TEST_PASSWORD")
    base_url = AQUA_URL

    response = requests.post(
        base_url + "/token", data={"username": TEST_USER, "password": TEST_PASSWORD}
    )

    # print response
    print(json.dumps(response.json(), indent=4))

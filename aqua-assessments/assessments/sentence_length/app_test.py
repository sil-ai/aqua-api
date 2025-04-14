from pathlib import Path
import time

import modal
import pytest
import requests

import app

version_abbreviation = "SL-DEL"
version_name = "sentence length delete"


# Add a version to the database for this test
def test_add_version(base_url, header):
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
def test_add_revision(base_url, header, filepath: Path):
    url = base_url + "/version"
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


# The following functions need a app to provide extra packages
# app = modal.App(
#     name="run-sentence-length-test",
#     image=modal.Image.debian_slim().pip_install(
#         "pandas~=1.5.0",
#         "psycopg2-binary~=2.9.0",
#         "pydantic~=1.10.0",
#         "pytest~=8.0.0",
#         "requests~=2.31.0",
#         "sqlalchemy~=1.4.0",
#         "asyncpg~=0.27.0",
#         "psycopg2~=2.9.0",
#     ),
# )
app = modal.App(
    name="run-sentence-length-test",
    image=modal.Image.debian_slim()
        .apt_install("libpq-dev", "gcc")
        .pip_install(
            "pandas~=1.5.0",
            "psycopg2-binary~=2.9.0",
            "pydantic~=1.10.0",
            "pytest~=8.0.0",
            "requests~=2.31.0",
            "sqlalchemy~=1.4.0",
            "asyncpg~=0.27.0"
        )
)

get_words_per_sentence = modal.Function.lookup("sentence-length-test", "get_words_per_sentence")
get_long_words = modal.Function.lookup("sentence-length-test", "get_long_words")
# run_sentence_length = modal.Function.lookup("sentence-length-test", "assess")

run_sentence_length = modal.Function.lookup("sentence-length-test", "assess")

def test_metrics():
    # Bee Movie intro
    test_text = """
    The bee, of course, flies anyway because bees don't care what humans think is impossible.
    Yellow, black. Yellow, black. Yellow, black. Yellow, black.
    Ooh, black and yellow! Let's shake it up a little.
    Barry! Breakfast is ready!
    Coming!
    Hang on a second.
    Hello?
    """
    with app.run():
        assert round(get_words_per_sentence.remote(test_text), 2) == 5.31
        assert round(get_long_words.remote(test_text), 2) == 2.90

# @app.function(timeout=3600)
# def run_sentence_length_from_app(config, AQUA_DB: str):
#     with app.run():
#         return run_sentence_length.remote(config, AQUA_DB)

@app.function(secrets=[modal.Secret.from_name("aqua-pytest")], timeout=3600)
def run_assess_draft(config):
    import os

    AQUA_DB = os.getenv("AQUA_DB")
    # with modal.enable_output():
    #     with app.run():
    #         results_response = run_sentence_length.remote(config, AQUA_DB)
    results_response = run_sentence_length.remote(config, AQUA_DB)
    # results_response = results_future.get()
    results = results_response["results"]
    assert len(results) == 31098
    assert results[0]["score"] == pytest.approx(23.12, 0.01)
    assert results[31]["score"] == pytest.approx(33.62, 0.01)
    assert results[56]["score"] == pytest.approx(33.57, 0.01)


def test_assess_draft(base_url, header):
    from pydantic import BaseModel
    from typing import Optional, Literal

    class Assessment(BaseModel):
        id: Optional[int] = None
        revision_id: int
        type: Literal["sentence-length"]

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
    config = Assessment(id=1, revision_id=revision_id, type="sentence-length")
    #run_assess_draft(config.dict())

    with app.run():
        run_assess_draft.remote(config.dict())


def test_delete_version(base_url, header):
    time.sleep(
        2
    )  # Allow the assessments above to finish pulling from the database before deleting!

    url = base_url + "/version"
    response = requests.get(url, headers=header)
    version_id = [
        version["id"]
        for version in response.json()
        if version["abbreviation"] == version_abbreviation
    ][0]
    test_delete_version = {"id": version_id}
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200

# @app.function()
# def test_asyncpg_installation():
#     try:
#         import asyncpg
#         return "asyncpg is installed and working"
#     except ModuleNotFoundError:
#         return "asyncpg is not installed"


if __name__ == "__main__":
    import os

    AQUA_URL = os.getenv("AQUA_URL")
    TEST_USER = os.getenv("TEST_USER")
    TEST_PASSWORD = os.getenv("TEST_PASSWORD")
    base_url = AQUA_URL

    response = requests.post(
            base_url+"/token", data={"username": TEST_USER, "password": TEST_PASSWORD}
    )

    token = response.json()["access_token"]
    header = {"Authorization": f"Bearer {token}"}

    test_add_version(base_url, header)
    test_add_revision(base_url, header, Path("fixtures/swh-ONEN.txt"))
    test_assess_draft(base_url, header)
    test_delete_version(base_url, header)

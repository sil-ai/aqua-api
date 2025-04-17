from pathlib import Path
from typing import List

import modal
import pytest
import requests
from pydantic import ValidationError

from models import Result

app = modal.App(
    name="run-push-results-test",
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pytest~=8.0.0",
        "requests~=2.31.0",
        "sqlalchemy~=1.4.0",
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("fixtures/verse_scores.csv"),
            remote_path=Path("/root/verse_scores.csv"),
        )
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("fixtures/alignment_threshold_scores.csv"),
            remote_path=Path("/root/alignment_threshold_scores.csv"),
        )
    ),
)
run_push_results = modal.Function.lookup("push-results-test", "push_results")
run_delete_results = modal.Function.lookup("push-results-test", "delete_results")

version_abbreviation = "PSR-DEL"
version_name = "push results delete"


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


# Add two revisions to the database for this test
@pytest.mark.parametrize(
    "filepath",
    [Path("../../fixtures/greek_lemma_luke.txt"), Path("../../fixtures/ngq-ngq.txt")],
)
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


@app.function()
def push_results(results: list, AQUA_DB: str, table_name: str = "assessment_result"):
    return run_push_results.remote(results, AQUA_DB, table_name=table_name)


@app.function()
def delete_results(ids: List[int], AQUA_DB: str):
    return run_delete_results.remote(ids, AQUA_DB)


@app.function(
    secrets=[modal.Secret.from_name("aqua-pytest"), modal.Secret.from_name("aqua-api")]
)
def push_df_results():
    import os

    import pandas as pd
    import requests

    AQUA_DB = os.getenv("AQUA_DB")
    AQUA_URL = os.getenv("AQUA_URL")
    TEST_USER = os.getenv("TEST_USER")
    TEST_PASSWORD = os.getenv("TEST_PASSWORD")
    base_url = AQUA_URL
    response = requests.post(
        base_url + "/token", data={"username": TEST_USER, "password": TEST_PASSWORD}
    )

    token = response.json()["access_token"]
    header = {"Authorization": f"Bearer {token}"}
    # get version
    url = AQUA_URL + "/version"
    response = requests.get(url, headers=header)
    version_id = [
        version["id"]
        for version in response.json()
        if version["abbreviation"] == version_abbreviation
    ][0]

    # get revision
    url = AQUA_URL + "/revision"
    response = requests.get(url, headers=header, params={"version_id": version_id})
    revision_id = response.json()[0]["id"]
    reference_id = response.json()[1]["id"]

    verse_scores = pd.read_csv("/root/verse_scores.csv")
    alignment_threshold_scores = pd.read_csv("/root/alignment_threshold_scores.csv")

    # Create an assessment
    url = AQUA_URL + "/assessment"
    response = requests.post(
        url,
        params={
            "revision_id": revision_id,
            "reference_id": reference_id,
            "type": "word-alignment",
        },
        headers=header,
    )

    assessment_id = response.json()[0][
        "id"
    ]  # here it returned a list of just 1 assessment, but we have to indicate the first one anyway

    num_rows = 10
    results = []

    for i, row in verse_scores.iloc[:num_rows, :].iterrows():
        result = {
            # 'id': i,
            "assessment_id": assessment_id,
            "vref": row["vref"],
            "score": row["score"],
            "flag": False,
        }
        results.append(result)

    # Results(results=results)

    # Push the results to the DB.
    AQUA_DB = os.getenv("AQUA_DB")
    response, ids = push_results.remote(results, AQUA_DB)

    assert response == 200
    assert len(set(ids)) == num_rows

    response, _ = delete_results.remote(ids, AQUA_DB)
    assert response == 200

    num_rows = 10
    results = []

    for i, row in alignment_threshold_scores.iloc[:num_rows, :].iterrows():
        result = {
            "assessment_id": assessment_id,
            "vref": row["vref"],
            "score": row["score"],
            "flag": False,
            "source": row["source"],
            "target": row["target"],
        }
        results.append(result)

    # Results(results=results)

    # Push the results to the DB.
    response, ids = push_results.remote(
        results, AQUA_DB, table_name="alignment_threshold_scores"
    )

    assert response == 200
    assert len(set(ids)) == num_rows

    response, _ = delete_results.remote(ids, AQUA_DB)
    assert response == 200


def test_push_df_rows():
    with app.run():
        push_df_results.remote()


def test_push_wrong_data_type():
    with pytest.raises(ValidationError):
        Result(assessment_id=1, vref=2, score="abc123", flag=False)


def test_delete_version(base_url, header):
    url = base_url + "/version"
    response = requests.get(url, headers=header)
    version_id = [
        version["id"]
        for version in response.json()
        if version["abbreviation"] == version_abbreviation
    ][0]
    params = {"id": version_id}
    url = base_url + "/version"
    test_response = requests.delete(url, params=params, headers=header)
    assert test_response.status_code == 200

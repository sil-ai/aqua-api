from pydantic import ValidationError
from typing import List
from pathlib import Path

import modal
import pytest

from models import Result, Results

stub = modal.Stub(
    name="push_results_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
        "pytest",
    )
    .copy(
        mount=modal.Mount(
            local_file=Path("fixtures/verse_scores.csv"), remote_dir=Path("/root")
        )
    ),
)
stub.run_push_results = modal.Function.from_name("push-results-test", "push_results")
stub.run_delete_results = modal.Function.from_name("push-results-test", "delete_results")

version_abbreviation = 'PSR-DEL'
version_name = 'push results delete'

# Add a version to the database for this test
def test_add_version(base_url, header):
    import requests
    test_version = {
            "name": version_name, "isoLanguage": "swh",
            "isoScript": "Latn", "abbreviation": version_abbreviation
            }
    url = base_url + '/version'
    response = requests.post(url, params=test_version, headers=header)
    if response.status_code == 400 and response.json()['detail'] == "Version abbreviation already in use.":
        print("This version is already in the database")
    else:
        assert response.json()['name'] == version_name


# Add two revisions to the database for this test
@pytest.mark.parametrize("filepath", [Path("../../fixtures/greek_lemma_luke.txt"), Path("../../fixtures/ngq-ngq.txt")])
def test_add_revision(base_url, header, filepath: Path):
    import requests
    url = base_url + "/version"
    response = requests.get(url, headers=header)
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_abv_revision = {
            "version_id": version_id,
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200



@stub.function
def push_results(results: Results, AQUA_DB: str):
    return modal.container_app.run_push_results.call(results, AQUA_DB)


@stub.function
def delete_results(ids: List[int], AQUA_DB: str):
    return modal.container_app.run_delete_results.call(ids, AQUA_DB)


@stub.function(secrets=[modal.Secret.from_name("aqua-pytest"), modal.Secret.from_name("aqua-api")])
def push_df_results():
    import pandas as pd
    import requests
    import os

    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    AQUA_URL = os.getenv(f"AQUA_URL_{database_id.replace('-', '_')}")
    AQUA_API_KEY = os.getenv(f"AQUA_API_KEY_{database_id.replace('-', '_')}")
    key =  "Bearer" + " " + AQUA_API_KEY
    header = {"Authorization": key}
    
    #get version
    url = AQUA_URL + "/version"
    response = requests.get(url, headers=header)
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    
    #get revision
    url = AQUA_URL + "/revision"
    response = requests.get(url, headers=header, params={'version_id': version_id})
    revision_id = response.json()[0]['id']
    reference_id = response.json()[1]['id']

    df = pd.read_csv("/root/verse_scores.csv")
    num_rows = 10
    results = []

    # Create an assessment
    url = AQUA_URL + "/assessment"
    response = requests.post(
        url,
        params={
            "revision_id": revision_id,
            "reference_id": reference_id,
            "type": "dummy"
        },
        headers=header,
    )
    assessment_id = response.json()['id']

    for _, row in df.iloc[:num_rows, :].iterrows():
        result = {
            'assessment_id': assessment_id,
            'vref': row["vref"],
            'score': row["total_score"],
            'flag': False,
        }
        results.append(result)

    Results(results=results)

    # Push the results to the DB.
    AQUA_DB = os.getenv("AQUA_DB")
    response, ids = push_results.call(results, AQUA_DB)
    
    assert response == 200
    assert len(set(ids)) == num_rows

    response, _ = delete_results.call(ids, AQUA_DB)
    assert response == 200


def test_push_df_rows():
    with stub.run():
        push_df_results.call()


def test_push_wrong_data_type():
    with pytest.raises(ValidationError):
        Result(
            assessment_id=1,
            vref=2,
            score="abc123",
            flag=False,
        )

def test_delete_version(base_url, header):
    import requests
    url = base_url + "/version"
    response = requests.get(url, headers=header)
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    test_delete_version = {
            "id": version_id
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200
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
stub.run_push_results = modal.Function.from_name("push_results_test", "push_results")
stub.run_delete_results = modal.Function.from_name(
    "push_results_test", "delete_results"
)

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
    response = requests.post(url, json=test_version, headers=header)
    if response.status_code == 400 and response.json()['detail'] == "Version abbreviation already in use.":
        print("This version is already in the database")
    else:
        assert response.json()['name'] == version_name


# Add two revisions to the database for this test
@pytest.mark.parametrize("filepath", [Path("../../fixtures/greek_lemma_luke.txt"), Path("../../fixtures/ngq-ngq.txt")])
def test_add_revision(base_url, header, filepath: Path):
    import requests
    test_abv_revision = {
            "version_abbreviation": version_abbreviation,
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200



@stub.function
def push_results(results: Results):
    return modal.container_app.run_push_results.call(results)


@stub.function
def delete_results(ids: List[int]):
    return modal.container_app.run_delete_results.call(ids)


@stub.function(secret=modal.Secret.from_name("aqua-api"))
def push_df_rows(base_url, header):
    import pandas as pd
    import requests

    df = pd.read_csv("/root/verse_scores.csv")
    num_rows = 10
    results = []

    # Create an assessment
    url = base_url + "/revision"
    response = requests.get(url, headers=header, params={'version_abbreviation': version_abbreviation})

    reference = response.json()[0]['id']
    revision = response.json()[1]['id']
    
    response = requests.post(
        f"{base_url}/assessment",
        json={
            "revision": revision,
            "reference": reference,
            "type": "dummy"
        },
        headers=header,
    )

    assessment_id = response.json()['data']['id']

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
    response, ids = push_results.call(results)
    print(response)
    assert response == 200
    print(ids)
    assert len(set(ids)) == num_rows

    response, _ = delete_results.call(ids)
    assert response == 200


def test_push_df_rows(base_url, header):
    with stub.run():
        push_df_rows.call(base_url, header)


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
    test_delete_version = {
            "version_abbreviation": version_abbreviation
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200
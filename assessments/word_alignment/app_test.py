import modal
import requests
from pathlib import Path
import json
import time
import pytest
import pickle

import word_alignment_steps.prepare_data as prepare_data


stub = modal.Stub(
    name="run_word_alignment_test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
        "asyncio",
        "sqlalchemy",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
        "pytest",
    ).copy(
        mount=modal.Mount(
            local_dir=Path("fixtures/"), remote_dir=Path("/")
        ),
        remote_path='/root/fixtures'
    ).copy(
        mount=modal.Mount(
            local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/")
        ),
        remote_path='/root/fixtures'
    ),
)
stub.run_word_alignment = modal.Function.from_name("word_alignment_test", "word_alignment")


@stub.function(mounts=[
    *modal.create_package_mounts(["word_alignment_steps.prepare_data"]),
    modal.Mount(local_dir="./", remote_dir="/"),
])
def run_prepare_data():
    for file in Path('/root/fixtures').iterdir():
        print(file.name)
    with open('/root/fixtures/hebrew_lemma_mini.txt') as f:
        src_data = f.readlines()

    with open('/root/fixtures/en-NASB_mini.txt') as f:
        trg_data = f.readlines()

    vref_filepath = Path('/root/fixtures/vref.txt')

    src_tokenized_df = pickle.loads(prepare_data.create_tokens(src_data, vref_filepath))
    trg_tokenized_df = pickle.loads(prepare_data.create_tokens(trg_data, vref_filepath))
    combined_df = src_tokenized_df.join(
            trg_tokenized_df.drop(["vref"], axis=1).rename(
                columns={"src_tokenized": "trg_tokenized", "src_list": "trg_list"}
            ),
            how="inner",
        )
    
    combined_df_pkl = pickle.dumps(combined_df)
    condensed_df = pickle.loads(prepare_data.condense_df(combined_df_pkl))
    assert condensed_df.shape[0] > 10
    assert condensed_df.shape[0] < 50
    assert 'vref' in condensed_df.columns
    assert 'src' in condensed_df.columns
    assert 'trg' in condensed_df.columns


def test_prepare_data():
    with stub.run():
        run_prepare_data.call()


def test_add_version(base_url, header):
    test_version = {
            "name": "word alignment delete", "isoLanguage": "eng",
            "isoScript": "Latn", "abbreviation": "WA-DEL"
            }
    url = base_url + '/version'
    new_version = requests.post(url, params=test_version, headers=header)
    assert new_version.json()['name'] == 'word alignment delete'


@pytest.mark.parametrize("filepath", [Path("../../fixtures/test_bible.txt"), Path("../../fixtures/uploadtest.txt")])
def test_add_revision(base_url, header, filepath: Path):
    test_abv_revision = {
            "version_abbreviation": "WA-DEL",
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200


def test_runner(base_url, header):
    webhook_url = "https://sil-ai--runner-test-assessment-runner.modal.run"
    api_url = base_url + "/revision"
    response = requests.get(api_url, headers=header, params={'version_abbreviation': 'WA-DEL'})
    revision_id = response.json()[0]['id']
    reference_id = response.json()[1]['id']
    config = {
        "assessment":999999,    #This will silently fail when pushing to the database, since it doesn't exist
        "assessment_type":"word_alignment",
        "configuration":{
        "revision": revision_id,
        "reference": reference_id
        }
    }
    json_file = json.dumps(config)
    response = requests.post(webhook_url, files={"file": json_file})

    assert response.status_code == 200


@stub.function(timeout=3600)
def get_results(assessment_id, configuration, push_to_db: bool=True):
    ids = modal.container_app.run_word_alignment.call(assessment_id, configuration, push_to_db=push_to_db)
    return ids


def test_assess_draft(base_url, header):
    with stub.run():
        # Use the two revisions of the "WA-DEL" version as revision and reference
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': 'WA-DEL'})
        revision_id = response.json()[0]['id']
        reference_id = response.json()[1]['id']

        config = {'revision': revision_id, 'reference': reference_id}

        #Run word alignment from reference to revision, but don't push it to the database
        response, _ = get_results.call(assessment_id=999999, configuration=config, push_to_db=False)

        assert response == 200


def test_delete_version(base_url, header):
    time.sleep(10)  # Allow the assessments above to finish pulling from the database before deleting!
    test_delete_version = {
            "version_abbreviation": "WA-DEL"
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    print(test_response.json())
    assert test_response.status_code == 200
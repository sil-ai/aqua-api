import modal
import requests
from pathlib import Path
import time
import pytest
import pickle
import os

import word_alignment_steps.prepare_data as prepare_data
from app import Assessment

version_abbreviation = 'WA-DEL'
version_name = 'word alignment delete'

stub = modal.Stub(
    name="run-word-alignment-test",
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


@stub.function(mounts=[
    *modal.create_package_mounts(["word_alignment_steps.prepare_data"]),
    modal.Mount(local_dir="./", remote_dir="/"),
])
def run_prepare_data():
    
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



# Add a version to the database for this test
def test_add_version(base_url, header):
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



@pytest.mark.parametrize("filepath", [Path("../../fixtures/test_bible.txt"), Path("../../fixtures/uploadtest.txt")])
def test_add_revision(base_url, header, filepath: Path):
    test_abv_revision = {
            "version_abbreviation": version_abbreviation,
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200


def test_runner(base_url, header):
    webhook_url = "https://sil-ai--runner-assessment-runner.modal.run"
    api_url = base_url + "/revision"
    response = requests.get(api_url, headers=header, params={'version_abbreviation': version_abbreviation})

    revision_id = response.json()[0]['id']
    reference_id = response.json()[1]['id']
    
    config = Assessment(
        assessment = 999999,    #This will silently fail when pushing to the database, since it doesn't exist
        type = "word-alignment",
        revision = revision_id,
        reference = reference_id,
    )
    
    response = requests.post(webhook_url, json=config.dict())
    assert response.status_code == 200


stub.run_word_alignment = modal.Function.from_name("word-alignment-test", "assess")

@stub.function(timeout=3600)
def get_results(assessment_config: Assessment, push_to_db: bool=True):
    response = modal.container_app.run_word_alignment(assessment_config=assessment_config, push_to_db=push_to_db)
    return response


def test_assess_draft(base_url, header):
    with stub.run():
        # Use the two revisions of the version_abbreviation version as revision and reference
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': version_abbreviation})

        revision_id = response.json()[0]['id']
        reference_id = response.json()[1]['id']
        
        config = Assessment(
                assessment=999999, 
                revision=revision_id, 
                reference=reference_id, 
                type='word-alignment'
                )

        #Run word alignment from reference to revision, but don't push it to the database
        response = get_results.call(assessment_config=config, push_to_db=False)

        assert response['status'] == 'finished (not pushed to database)'


RESULTS_DIR = Path("/results")
word_alignment_results_volume = modal.SharedVolume().persist("word_alignment_results")

stub.get_results = modal.Function.from_name("word-alignment-test", "get_results")

@stub.function(
    shared_volumes={RESULTS_DIR: word_alignment_results_volume},
    secret=modal.Secret.from_name("aqua-db"),
    )
def check_word_alignment_results(assessment_config: Assessment):
    top_source_scores_df = modal.container_app.get_results.call(assessment_config.revision, assessment_config.reference)
    assert top_source_scores_df.shape[0] > 10
    assert "source" in top_source_scores_df.columns


def test_check_word_alignment_results(base_url, header):
    with stub.run():
        # Use the two revisions of the version_abbreviation version as revision and reference
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': version_abbreviation})

        revision_id = response.json()[0]['id']
        reference_id = response.json()[1]['id']
        
        config = Assessment(
                assessment=999999, 
                revision=revision_id, 
                reference=reference_id, 
                type='word-alignment'
                )

        #Check that the results are in the shared volume
        check_word_alignment_results.call(assessment_config=config)


def test_delete_version(base_url, header):
    time.sleep(2)  # Allow the assessments above to finish pulling from the database before deleting!
    test_delete_version = {
            "version_abbreviation": version_abbreviation
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200

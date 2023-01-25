import modal
import requests
from pathlib import Path
import json
import os
import time
import pytest

import pandas as pd

import word_alignment_steps.prepare_data as prepare_data


def test_prepare_data():
    with open('fixtures/hebrew_lemma_mini.txt') as f:
        src_data = f.readlines()

    with open('fixtures/en-NASB_mini.txt') as f:
        trg_data = f.readlines()

    vref_filepath = Path('../../fixtures/vref.txt')

    src_tokenized_df = prepare_data.create_tokens(src_data, vref_filepath)
    trg_tokenized_df = prepare_data.create_tokens(trg_data, vref_filepath)
    combined_df = src_tokenized_df.join(
            trg_tokenized_df.drop(["vref"], axis=1).rename(
                columns={"src_tokenized": "trg_tokenized", "src_list": "trg_list"}
            ),
            how="inner",
        )
    condensed_df = prepare_data.condense_df(combined_df)
    assert condensed_df.shape[0] > 10
    assert condensed_df.shape[0] < 50
    assert 'vref' in condensed_df.columns
    assert 'src' in condensed_df.columns
    assert 'trg' in condensed_df.columns


def test_add_version(base_url, header):
    test_version = {
            "name": "delete", "isoLanguage": "eng",
            "isoScript": "Latn", "abbreviation": "DEL"
            }
    url = base_url + 'version'
    new_version = requests.post(url, params=test_version, headers=header)
    assert new_version.json()['name'] == 'delete'
    
    return new_version.json()['id']


@pytest.mark.parametrize("filepath", [Path("../../fixtures/test_bible.txt"), Path("../../fixtures/uploadtest.txt")])
def test_add_revision(base_url, header, filepath: Path):
    test_abv_revision = {
            "version_abbreviation": "DEL",
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200

    return response_abv.json()['Revision ID']




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
    ),
)
stub.run_word_alignment = modal.Function.from_name("word_alignment_test", "word_alignment")

def test_runner(base_url, header):
    webhook_url = "https://sil-ai--runner-test-assessment-runner.modal.run/"
    api_url = base_url + "revision"
    response = requests.get(api_url, headers=header, params={'version_abbreviation': 'DEL'})
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
def get_results(assessment_id, configuration):
    ids = modal.container_app.run_word_alignment.call(assessment_id, configuration)
    return ids


def test_assess_draft(base_url, header):
    with stub.run():
        # Use the two revisions of the "DEL" version as revision and reference
        url = base_url + "revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': 'DEL'})
        revision_id = response.json()[0]['id']
        reference_id = response.json()[1]['id']

        config = {'revision': revision_id, 'reference': reference_id}

        #Run word alignment from reference to revision and push it to the assessment in the database
        response, ids = get_results.call(assessment_id=999999, configuration=config)  #This will silently fail when pushing to the database, since the assessment id doesn't exist

        assert response == 200
        assert len(set(ids)) == 3


time.sleep(180) # The assessments continue after the runner has returned a response and this script
            # has finished. This sleep gives time for the assessment to pull the revision before it's deleted.

# def test_add_assessment(base_url, header):
#     url = base_url + "revision"
#     response = requests.get(url, headers=header, params={'version_abbreviation': 'DEL'})
#     revision_id = response.json()[0]['id']

#     config = {
#             "revision": revision_id,
#             "type": "word_alignment",
#             "reference": reference_id
#             }
#     config_json = json.dumps(config)
#     url = base_url + "assessment"
#     response = requests.post(url, files={'file': config_json}, headers=header)
#     # assert response.status_code == 200
#     id = response.json()['data']['id']

#     return id


# def delete_assessment(base_url, header):
#     url = base_url + "assessment"
#     response = requests.get(url, headers=header, params={'version_abbreviation': 'DEL'})
#     revision_id = response.json()[0]['id']

#     response = requests.get(url, headers=header)
#     assessments = response.json()['assessments']

#     assessments_to_delete = [assessment for assessment in assessments if assessment['revision'] == revision_id]

#     url = base_url + "assessment"
#     for assessment in assessments_to_delete:
#         response = requests.delete(url, params={'assessment_id': assessment['id']}, headers=header)
#         assert response.status_code == 200


def test_delete_version(base_url, header):
    test_delete_version = {
            "version_abbreviation": "DEL"
            }
    url = base_url + "version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200


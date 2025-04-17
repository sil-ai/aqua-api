import os
from pathlib import Path

import modal
import pytest
import requests

from app import Assessment

volume = modal.NetworkFileSystem.from_name("pytorch-model-vol", create_if_missing=True)
CACHE_PATH = "/root/model_cache"


app = modal.App(
    name="run-semantic-similarity-test",
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas~=1.5.0",
        "pytest~=8.0.0",
        "PyYAML~=6.0.0",
        "sqlalchemy~=1.4.0",
        "torch~=2.1.0",
        "transformers~=4.34.0",
        "asyncpg~=0.27.0",
    )
    .copy_mount(
        modal.Mount.from_local_file(
            local_path="./fixtures/swahili_revision.pkl",
            remote_path="/root/fixtures/swahili_revision.pkl",
        )
    )
    .copy_mount(
        modal.Mount.from_local_file(
            local_path="./fixtures/swahili_drafts.yml",
            remote_path="/root/fixtures/swahili_drafts.yml",
        )
    ),
)

assess = modal.Function.lookup("semantic-similarity-test", "assess")
get_sim_scores = modal.Function.lookup("semantic-similarity-test", "get_sim_scores")


@app.function(timeout=3600)
def get_assessment(config, AQUA_DB: str, offset: int = -1):
    return assess.remote(config, AQUA_DB)


# @app.function(timeout=3600)
def get_sim_scores_from_app(rev_sents, ref_sents):
    return list(get_sim_scores.map(zip(rev_sents, ref_sents)))


version_abbreviation = "SS-DEL"
version_name = "semantic similarity delete"


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
def test_add_revision(base_url, header, valuestorage, filepath: Path):
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
    valuestorage.revisions.append(response_abv.json()["id"])

    assert response_abv.status_code == 200


@app.function(timeout=3600, secrets=[modal.Secret.from_name("aqua-pytest")])
def assessment_object(draft_id, ref_id, expected):
    AQUA_DB = os.getenv("AQUA_DB")
    config = Assessment(
        id=1, revision_id=draft_id, reference_id=ref_id, type="semantic-similarity"
    )
    results_response = get_assessment.remote(config, AQUA_DB)
    results = results_response["results"]
    # test for the right type of results
    assert isinstance(results, list)
    # test for the expected length of results
    assert len(results) == expected
    return results


# tests the assessment object
def test_assessment_object(base_url, header, valuestorage):
    with app.run():
        url = base_url + "/version"
        response = requests.get(url, headers=header)
        version_id = [
            version["id"]
            for version in response.json()
            if version["abbreviation"] == version_abbreviation
        ][0]
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={"version_id": version_id})
        reference = valuestorage.revisions[0]
        revision = valuestorage.revisions[1]
        expected = 1143  # Length of verses in common between the two fixture revisions (basically the book of Luke)
        # results = modal.Lookup("semantic-similarity-test", "assessment_object").remote(
        results = assessment_object.remote(revision, reference, expected)
        valuestorage.results = results


def prediction_tester(expected, score):
    try:
        print(score)
        assert score == pytest.approx(expected, 0.01)
    except TypeError:
        raise ValueError("No result values")


@pytest.mark.parametrize(
    "idx,expected", [(0, 0.124), (1, 0.0233)], ids=["LUK 1:1", "LUK 1:2"]
)
# test sem_sim predictions
def test_predictions(idx, expected, request, valuestorage):
    with app.run():
        try:
            score = valuestorage.results[idx]["score"]
            prediction_tester(expected, score)
        except TypeError:
            raise AssertionError("No result values")


@app.function(timeout=600, retries=3)
def get_swahili_verses(verse_offset, variance):
    import pandas as pd
    import yaml

    drafts = yaml.safe_load(open("/root/fixtures/swahili_drafts.yml"))["drafts"]
    draft_verse = drafts[f"{verse_offset}-{variance}"]
    swahili_revision = pd.read_pickle("/root/fixtures/swahili_revision.pkl")
    verse = swahili_revision.iloc[verse_offset].text
    return verse, draft_verse


@pytest.mark.parametrize(
    "verse_offset, variance, expected",
    [
        (12, 5, 0.9391425251960754),
        (42, 10, 0.6759120225906372),
        (1042, 20, 0.19469811022281647),
        (4242, 30, 0.4287506639957428),
    ],
    ids=["NEH 10:21 5%", "GEN 26:21 10%", "GEN 32:22 20%", "Num 16:9 30%"],
)
def test_swahili_revision(verse_offset, variance, expected, request):
    with app.run():
        verse, draft_verse = get_swahili_verses.remote(verse_offset, variance)
        print(verse)
        print(draft_verse)
        result = get_sim_scores_from_app([verse], [draft_verse])[0][0]
        assert result == pytest.approx(expected, 0.01)


def test_delete_version(base_url, header):
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

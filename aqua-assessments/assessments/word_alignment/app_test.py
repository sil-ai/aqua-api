import pickle
import time
from pathlib import Path

import modal
import pytest
import word_alignment_steps.prepare_data as prepare_data

version_abbreviation = "WA-DEL"
version_name = "word alignment delete"

app = modal.App(
    name="run-word-alignment-test",
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "pytest~=8.0.0",
        "sil-machine~=0.9.0",
        "sil-thot~=3.4.0",
        "sqlalchemy~=1.4.0",
    )
    .copy_mount(
        mount=modal.Mount.from_local_dir(
            local_path=Path("fixtures/"), remote_path=Path("/root/fixtures")
        )
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("../../fixtures/vref.txt"),
            remote_path=Path("/root/fixtures/vref.txt"),
        )
    ),
)


@app.function(
    mounts=[
        modal.Mount.from_local_python_packages("word_alignment_steps.prepare_data"),
        modal.Mount.from_local_dir(local_path="./", remote_path="/"),
    ]
)
def run_prepare_data():
    with open("/root/fixtures/hebrew_lemma_mini.txt") as f:
        src_data = f.readlines()

    with open("/root/fixtures/en-NASB_mini.txt") as f:
        trg_data = f.readlines()

    vref_filepath = Path("/root/fixtures/vref.txt")

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
    assert "vref" in condensed_df.columns
    assert "src" in condensed_df.columns
    assert "trg" in condensed_df.columns


def test_prepare_data():
    with app.run():
        run_prepare_data.remote()


# Add a version to the database for this test
def test_add_version(base_url, header, assessment_storage):
    import requests

    test_version = {
        "name": version_name,
        "iso_language": "swh",
        "iso_script": "Latn",
        "abbreviation": version_abbreviation,
    }
    print(base_url)
    url = base_url + "/version"
    response = requests.post(url, json=test_version, headers=header)
    if (
        response.status_code == 400
        and response.json()["detail"] == "Version abbreviation already in use."
    ):
        print("This version is already in the database")
    else:
        assert response.json()["name"] == version_name

    assessment_storage.version_id = response.json()["id"]


@pytest.mark.parametrize(
    "filepath, name",
    [
        (Path("../../fixtures/test_bible.txt"), "revision"),
        (Path("../../fixtures/uploadtest.txt"), "reference"),
    ],
)
def test_add_revision(base_url, header, assessment_storage, filepath: Path, name: str):
    import requests

    version_id = assessment_storage.version_id
    test_abv_revision = {"version_id": version_id, "published": False, "name": name}

    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(
        url, params=test_abv_revision, files=file, headers=header
    )

    assert response_abv.status_code == 200

    assessment_storage.revisions.append(response_abv.json()["id"])


run_word_alignment = modal.Function.lookup("word-alignment-test", "assess")


@app.function(timeout=3600, secrets=[modal.Secret.from_name("aqua-pytest")])
def get_results(assessment_config, return_all_results: bool = False):
    import os

    AQUA_DB = os.getenv("AQUA_DB")
    results_response = run_word_alignment.remote(
        assessment_config, AQUA_DB, return_all_results=return_all_results
    )
    return results_response


def test_assess_draft(base_url, header, assessment_storage):
    with app.run():
        # Use the two revisions of the version_abbreviation version as revision and reference
        from typing import Literal, Optional

        from pydantic import BaseModel

        class Assessment(BaseModel):
            id: Optional[int] = None
            revision_id: int
            reference_id: int
            type: Literal["word-alignment"]

        revision_id = assessment_storage.revisions[0]
        reference_id = assessment_storage.revisions[1]
        print(f"{revision_id=}\n{reference_id=}")
        config = Assessment(
            id=1,
            revision_id=revision_id,
            reference_id=reference_id,
            type="word-alignment",
        )

        # Run word alignment from reference to revision, but don't push it to the database
        results_response = get_results.remote(assessment_config=config.dict())
        results = results_response["results"]
        assert len(results) == 3
        assert (
            results[0]["score"] == pytest.approx(0.629, 0.001)
            and results[1]["score"] == pytest.approx(0.738, 0.001)
            and results[2]["score"] == pytest.approx(0.758, 0.001)
        )

        # results_response = get_results.remote(
        #     assessment_config=config.dict(), return_all_results=True
        # )


get_word_alignment_results = modal.Function.lookup("save-results-test", "get_results")


@app.function(secrets=[modal.Secret.from_name("aqua-pytest")])
def check_word_alignment_results(assessment_config):
    import os

    AQUA_DB = os.getenv("AQUA_DB")
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    top_source_scores_df = get_word_alignment_results.remote(
        assessment_config["revision_id"],
        assessment_config["reference_id"],
        database_id,
        source_type="source",
    )
    assert "source" in top_source_scores_df.columns
    assert "total_score" in top_source_scores_df.columns
    assert (
        top_source_scores_df.loc[0, "total_score"] == pytest.approx(0.6736, 0.001)
        and top_source_scores_df.loc[5, "total_score"] == pytest.approx(0.7890, 0.001)
        and top_source_scores_df.loc[10, "total_score"] == pytest.approx(0.6524, 0.001)
    )

    top_target_scores_df = get_word_alignment_results.remote(
        assessment_config["revision_id"],
        assessment_config["reference_id"],
        database_id,
        source_type="target",
    )
    assert "target" in top_target_scores_df.columns
    assert "total_score" in top_target_scores_df.columns
    assert (
        top_target_scores_df.loc[0, "total_score"] == pytest.approx(0.8061, 0.001)
        and top_target_scores_df.loc[5, "total_score"] == pytest.approx(0.5317, 0.001)
        and top_target_scores_df.loc[10, "total_score"] == pytest.approx(0.7999, 0.001)
    )


def test_check_word_alignment_results(assessment_storage):
    with app.run():
        from typing import Literal, Optional

        from pydantic import BaseModel

        class Assessment(BaseModel):
            id: Optional[int] = None
            revision_id: int
            reference_id: int
            type: Literal["word-alignment"]

        # Use the two revisions of the version_abbreviation version as revision and reference
        revision_id = assessment_storage.revisions[0]
        reference_id = assessment_storage.revisions[1]
        config = Assessment(
            id=1,
            revision_id=revision_id,
            reference_id=reference_id,
            type="word-alignment",
        )

        # Check that the results are in the shared volume
        check_word_alignment_results.remote(assessment_config=config.dict())


def test_delete_version(base_url, header, assessment_storage):
    import requests

    time.sleep(
        2
    )  # Allow the assessments above to finish pulling from the database before deleting!

    version_id = assessment_storage.version_id
    test_delete_version = {"id": version_id}
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200


if __name__ == "__main__":
    import os

    import requests
    from conftest import AssessmentStorage

    AQUA_URL = os.getenv("AQUA_URL")
    TEST_USER = os.getenv("TEST_USER")
    TEST_PASSWORD = os.getenv("TEST_PASSWORD")
    base_url = AQUA_URL

    response = requests.post(
        base_url + "/token", data={"username": TEST_USER, "password": TEST_PASSWORD}
    )

    token = response.json()["access_token"]
    header = {"Authorization": f"Bearer {token}"}
    assessment_storage = AssessmentStorage()
    test_add_version(base_url, header)
    test_add_revision(base_url, header, Path("../../fixtures/test_bible.txt"))
    test_add_revision(base_url, header, Path("../../fixtures/uploadtest.txt"))
    test_assess_draft(base_url, header, assessment_storage)
    test_check_word_alignment_results(assessment_storage)
    test_delete_version(base_url, header)

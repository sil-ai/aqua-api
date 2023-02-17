import modal
import requests
from pathlib import Path
import pytest
import time

import app

version_abbreviation = 'SL-DEL'
version_name = 'sentence length delete'

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


# Add one or more revisions to the database for this test
@pytest.mark.parametrize("filepath", [Path('fixtures/swh-ONEN.txt')])
def test_add_revision(base_url, header, filepath: Path):
    test_abv_revision = {
            "version_abbreviation": version_abbreviation,
            "published": False
            }
 
    file = {"file": filepath.open("rb")}
    url = base_url + "/revision"
    response_abv = requests.post(url, params=test_abv_revision, files=file, headers=header)

    assert response_abv.status_code == 200


#The following functions need a stub to provide extra packages
stub = modal.Stub(
    name="run_sentence_length_test",
    image=modal.Image.debian_slim().pip_install(
        'pydantic',
        'pytest',
        'pandas',
        'requests',
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
    ),
)

stub.run_sentence_length = modal.Function.from_name("sentence-length-test", "assess")


@stub.function(mounts=[
    *modal.create_package_mounts(["app"]),
    modal.Mount(local_dir="./", remote_dir="/"),
])
def run_test_metrics():
    #Bee Movie intro
    test_text = """
    The bee, of course, flies anyway because bees don't care what humans think is impossible.
    Yellow, black. Yellow, black. Yellow, black. Yellow, black.
    Ooh, black and yellow! Let's shake it up a little.
    Barry! Breakfast is ready!
    Coming!
    Hang on a second.
    Hello?
    """

    assert app.get_words_per_sentence(test_text) == 8.625
    assert round(app.get_long_words(test_text), 2) == 2.90
    assert app.get_lix_score(test_text) == 11.52


def test_metrics():
    with stub.run():
        run_test_metrics.call()

    
@stub.function(secret=modal.Secret.from_name('aqua-pytest'))
def run_assess_draft(config):
    import os
    AQUA_DB = os.getenv("AQUA_DB")
    results = modal.container_app.run_sentence_length.call(config, AQUA_DB)
    assert len(results) == 41899
    assert results[0]['score'] == pytest.approx(23.12, 0.01)
    assert results[31]['score'] == pytest.approx(33.62, 0.01)
    assert results[56]['score'] == pytest.approx(37.44, 0.01)


def test_assess_draft(base_url, header):
    url = base_url + "/revision"
    response = requests.get(url, params={'version_abbreviation': version_abbreviation}, headers=header)
    revision = response.json()[0]['id']
    from app import Assessment
    config = Assessment(
        revision = revision,
        type = 'sentence-length',
    )
    with stub.run():
        run_assess_draft.call(config)    


def test_delete_version(base_url, header):
    time.sleep(2)  # Allow the assessments above to finish pulling from the database before deleting!
    test_delete_version = {
            "version_abbreviation": version_abbreviation
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200


if __name__ == "__main__":
    import os
    key =  "Bearer" + " " + str(os.getenv("TEST_KEY"))
    header = {"Authorization": key}
    base_url = os.getenv("AQUA_URL")

    test_add_version(base_url, header)
    test_add_revision(base_url, header, Path('fixtures/swh-ONEN.txt'))
    test_assess_draft(base_url, header)
    test_delete_version(base_url, header)
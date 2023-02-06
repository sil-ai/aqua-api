from pathlib import Path

import pytest
import modal

from app import Assessment


stub = modal.Stub(
    name="run-missing-words-test",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
        "asyncio",
        "sqlalchemy",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
        "pytest",
    )
)

stub.run_missing_words = modal.Function.from_name("missing-words-test", "assess")

@stub.function(timeout=3600)
def get_missing_words(assessment_config: Assessment, push_to_db: bool=True):
    response = modal.container_app.run_missing_words.call(assessment_config, push_to_db=push_to_db)
    return response


version_abbreviation = 'MW-DEL'
version_name = 'missing words delete'


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


def test_get_missing_words(base_url, header):
    with stub.run():
        # Use the two revisions of the version_abbreviation version as revision and reference
        import requests
        url = base_url + "/revision"
        response = requests.get(url, headers=header, params={'version_abbreviation': version_abbreviation})

        reference = response.json()[0]['id']
        revision = response.json()[1]['id']

        
        config = Assessment(
                assessment=999999, 
                revision=revision, 
                reference=reference, 
                type='missing-words'
                )

        #Run word alignment from reference to revision, but don't push it to the database
        push_to_db = False
        response = get_missing_words.call(assessment_config=config, push_to_db=push_to_db)
        assert response['status'] == 'finished' if push_to_db else 'finished (not pushed to database)'



def test_delete_version(base_url, header):
    import requests
    test_delete_version = {
            "version_abbreviation": version_abbreviation
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200

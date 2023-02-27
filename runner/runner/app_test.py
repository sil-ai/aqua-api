import pytest
from pathlib import Path

version_abbreviation = 'RN-DEL'
version_name = 'runner runner delete'


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


def test_runner(base_url, header):
    import requests
    url = base_url + "/version"
    response = requests.get(url, headers=header)
    version_id = [version["id"] for version in response.json() if version["abbreviation"] == version_abbreviation][0]
    url = base_url + "/revision"
    response = requests.get(url, headers=header, params={'version_id': version_id})

    reference_id = response.json()[0]['id']
    revision_id = response.json()[1]['id']
    
    config = {
        "type":"dummy",
        "reference_id": reference_id,
        "revision_id": revision_id,
    }
    url = "https://sil-ai--runner-test-assessment-runner.modal.run/"
    response = requests.post(url, json=config)

    assert response.status_code == 200


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

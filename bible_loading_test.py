import os
import pytest
from pathlib import Path
import requests
import time

import pandas as pd
import numpy as np
import sqlalchemy as db

import bible_loading

version_abbreviation = 'BL-DEL'
version_name = 'bible loading delete'

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
@pytest.mark.parametrize("filepath", [Path("fixtures/greek_lemma_luke.txt")])
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


def test_get_revision(base_url, header, revision_storage):
    # Use the two revisions of the version_abbreviation version as revision and reference
    api_url = base_url + "/revision"
    response = requests.get(api_url, headers=header, params={'version_abbreviation': version_abbreviation})

    revision_storage.revision = response.json()[0]['id']


def test_text_dataframe(revision_storage): 
    verses = []
    bibleRevision = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(np.nan)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_storage.revision)

    verseText = bible_loading.text_dataframe(verses, bibleRevision)

    test_data = {
            "locations": [
                "GEN 1:1", "GEN 1:2", "GEN 1:3", 
                "GEN 1:22", "GEN 1:23", "GEN 1:26", 
                "GEN 5:3", "GEN 5:4"
                ], 
            "text": [
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit,", 
                "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", 
                "Ut enim ad minim veniam,",
                "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
                "Duis aute irure dolor in reprehenderit in voluptate,",
                "velit esse cillum dolore eu fugiat nulla pariatur.",
                "Excepteur sint occaecat cupidatat non proident,",
                "sunt in culpa qui officia deserunt mollit anim id est laborum."
                ]
            }

    status = 0
    for _, row in verseText.iterrows():
        if row["verseReference"] in test_data["locations"]:
            location = test_data["locations"].index(row["verseReference"])
            if row["text"] in test_data["text"][location]:
                status += 1
                if status == 8:
                    success = True
            else:
                success = False
                break

    assert success is True


def test_text_loading(revision_storage):
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    
    verse_dict = {
        "text": ["TEST"], 
        "bibleRevision": [revision_storage.revision], 
        "verseReference": ["GEN 1:1"]
        }

    verseText = pd.DataFrame(verse_dict)

    text_load = bible_loading.text_loading(verseText, db_engine)
    assert text_load is True

    #TODO - Do an explicit SQL query to check that the data was uploaded.
    # and then another assert.


def test_upload_bible(revision_storage):
    verses = []
    bibleRevision = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(np.nan)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_storage.revision)

    bible_upload = bible_loading.upload_bible(verses, bibleRevision)

    assert bible_upload is True


def test_delete_version(base_url, header):
    time.sleep(2)  # Allow the assessments above to finish pulling from the database before deleting!
    test_delete_version = {
            "version_abbreviation": version_abbreviation
            }
    url = base_url + "/version"
    test_response = requests.delete(url, params=test_delete_version, headers=header)
    assert test_response.status_code == 200

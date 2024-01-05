import os
import psycopg2
import sqlalchemy as db
import pandas as pd
from datetime import date
import pytest
import bible_loading
import queries
import numpy as np


@pytest.fixture(scope="module")
def db_setup_teardown():
    setup = setup_database()
    yield setup
    teardown_database()

def setup_database():
    """Set up the database for testing."""
    connection_string = os.getenv("AQUA_DB")
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            iso_language_query = queries.add_iso_language()
            iso_script_query = queries.add_iso_script()
            version_query = queries.add_version_query()

            cursor.execute(iso_language_query, ("eng", "english"))
            cursor.execute(iso_script_query, ("Latn", "latin"))
            cursor.execute(version_query, ("loading_test", "eng", "Latn", "BLTEST", None, None, None, False))

            fetch_version_query = queries.fetch_bible_version_by_abbreviation()
            cursor.execute(fetch_version_query, ("BLTEST",))
            fetch_version_data = cursor.fetchone()
            version_id = fetch_version_data[0]

            revision_date = str(date.today())
            revision_query = queries.insert_bible_revision()
            cursor.execute(revision_query, (version_id, revision_date, None, False, None, None, True))
            revision_response = cursor.fetchone()
            revision_id = revision_response[0]

    return revision_id

def teardown_database():
    """Tear down the database after testing."""
    connection_string = os.getenv("AQUA_DB")
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            # Delete all data from the tables
            cursor.execute("DELETE FROM bible_revision;")
            cursor.execute("DELETE FROM bible_version;")
            cursor.execute("DELETE FROM iso_language;")
            cursor.execute("DELETE FROM iso_script;")
            # Add any other tables as needed


def test_text_dataframe(db_setup_teardown): 
    verses = []
    bible_revision = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line.strip():
                verses.append(line.strip())
                bible_revision.append(db_setup_teardown)
            else:
                verses.append(np.nan)
                bible_revision.append(np.nan)

    verse_text = bible_loading.text_dataframe(verses, bible_revision)

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

    success = True
    for _, row in verse_text.iterrows():
        if row["verse_reference"] in test_data["locations"]:
            location = test_data["locations"].index(row["verse_reference"])
            if row["text"] != test_data["text"][location]:
                success = False
                break

    assert success is True


def test_text_loading(db_setup_teardown):
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    
    verse_dict = {
        "text": ["TEST"], 
        "bible_revision": [db_setup_teardown], 
        "verse_reference": ["GEN 1:1"]
    }

    verse_text = pd.DataFrame(verse_dict)

    text_load = bible_loading.text_loading(verse_text, db_engine)
    assert text_load is True

    # TODO - Do an explicit SQL query to check that the data was uploaded.

def test_upload_bible(db_setup_teardown): 
    verses = []
    bible_revision = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line.strip():
                verses.append(line.strip())
                bible_revision.append(db_setup_teardown)
            else:
                verses.append(np.nan)
                bible_revision.append(np.nan)
        
    bible_upload = bible_loading.upload_bible(verses, bible_revision, os.getenv("AQUA_DB"))

    # Verify upload and cleanup
    with psycopg2.connect(os.getenv("AQUA_DB")) as conn:
        with conn.cursor() as cursor:
            fetch_version_query = queries.fetch_bible_version_by_abbreviation()
            cursor.execute(fetch_version_query, ("BLTEST",))
            fetch_response = cursor.fetchone()
            version_id = fetch_response[0]

            delete_version_mutation = queries.delete_bible_version()
            cursor.execute(delete_version_mutation, (version_id,))
            delete_response = cursor.fetchone()
            delete_check = delete_response[0]

    assert bible_upload is True
    assert delete_check == "loading_test"

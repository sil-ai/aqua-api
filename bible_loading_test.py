import os
import psycopg2
import sqlalchemy as db
import pandas as pd
from datetime import date
import pytest
import bible_loading
import queries
import numpy as np
from sqlalchemy import create_engine


@pytest.fixture(scope="function")
def db_setup_teardown():
    setup = setup_database()
    yield setup
    teardown_database()

def setup_database():
    """Set up the database for testing."""
    connection_string = os.getenv("AQUA_DB")
    
    
    engine = create_engine(connection_string)
    book_ref_df = pd.read_csv('fixtures/book_reference.txt', sep='\t')
    book_ref_df.to_sql('book_reference', con=engine, if_exists='append', index=False)

    chapter_ref_df = pd.read_csv('fixtures/chapter_reference.txt', sep='\t')
    chapter_ref_df.to_sql('chapter_reference', con=engine, if_exists='append', index=False)

    verse_ref_df = pd.read_csv('fixtures/verse_reference.txt', sep='\t')
    verse_ref_df.to_sql('verse_reference', con=engine, if_exists='append', index=False)

    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            iso_language_query = queries.add_iso_language()
            iso_script_query = queries.add_iso_script()
            version_query = queries.add_version_query()
            book_reference_query = queries.add_book_reference()
            chapter_reference_query = queries.add_chapter_reference()
            verse_reference_query = queries.add_verse_reference()


            cursor.execute(iso_language_query, ("eng", "english"))
            cursor.execute(iso_script_query, ("Latn", "latin"))
            cursor.execute(version_query, ("loading_test", "eng", "Latn", "BLTEST", None, None, None, False))
            # cursor.execute(book_reference_query, ("GEN", "Genesis", 1))
            # cursor.execute(chapter_reference_query,("GEN 1", 1, "GEN"))
            # cursor.execute(verse_reference_query, ("GEN 1:1", 1, "GEN 1", "GEN"))


            fetch_version_query = queries.fetch_bible_version_by_abbreviation()
            cursor.execute(fetch_version_query, ("BLTEST",))
            fetch_version_data = cursor.fetchone()
            version_id = fetch_version_data[0]

            revision_date = str(date.today())
            revision_query = queries.insert_bible_revision()
            cursor.execute(revision_query, (revision_date, version_id, False, None, None, True))
            revision_response = cursor.fetchone()
            revision_id = revision_response[0]

    return revision_id

def teardown_database():
    """Tear down the database after testing."""
    connection_string = os.getenv("AQUA_DB")
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            # Corrected DELETE statements
            cursor.execute("DELETE FROM verse_text;")
            cursor.execute("DELETE FROM bible_revision;")
            cursor.execute("DELETE FROM bible_version;")
            cursor.execute("DELETE FROM iso_script;")
            cursor.execute("DELETE FROM iso_language;")
            cursor.execute("DELETE FROM verse_reference;")
            cursor.execute("DELETE FROM chapter_reference;")
            cursor.execute("DELETE FROM book_reference;")

def test_text_dataframe(db_setup_teardown): 
    revision_id = db_setup_teardown  # This is the value returned by setup_database
    verses = []
    bible_revision_id = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line.strip():
                verses.append(line.strip())
                bible_revision_id.append(revision_id)
            else:
                verses.append(np.nan)
                bible_revision_id.append(np.nan)

    verse_text = bible_loading.text_dataframe(verses, bible_revision_id)

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
    revision_id = db_setup_teardown  # This is the value returned by setup_database
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    
    verse_dict = {
        "text": ["TEST"], 
        "bible_revision_id": [revision_id], 
        "verse_reference": ["GEN 1:1"]
    }

    verse_text = pd.DataFrame(verse_dict)

    text_load = bible_loading.text_loading(verse_text, db_engine)
    assert text_load is True

    # TODO - Do an explicit SQL query to check that the data was uploaded.

def test_upload_bible(db_setup_teardown): 
    verses = []
    bible_revision_id = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line.strip():
                verses.append(line.strip())
                bible_revision_id.append(db_setup_teardown)
            else:
                verses.append(np.nan)
                bible_revision_id.append(np.nan)
        
    bible_upload = bible_loading.upload_bible(verses, bible_revision_id)

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

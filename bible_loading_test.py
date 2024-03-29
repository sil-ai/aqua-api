import os
import re

import pandas as pd
import numpy as np
import sqlalchemy as db
import psycopg2
from datetime import date

import bible_loading
import queries


conn_list = (re.sub("/|:|@", " ", os.getenv("AQUA_DB")).split())
connection = psycopg2.connect(
        host=conn_list[3],
        database=conn_list[4],
        user=conn_list[1],
        password=conn_list[2],
        sslmode="require"
        )

cursor = connection.cursor()

version_query = queries.add_version_query()

fetch_version_query = queries.fetch_bible_version_by_abbreviation()
 
cursor.execute(version_query, (
    "loading_test", "eng", "Latn", "BLTEST",
    None, None, None, False
    )
)
cursor.close()


cursor = connection.cursor()

cursor.execute(fetch_version_query, ("BLTEST",))
fetch_version_data = cursor.fetchone()
version_id = fetch_version_data[0]

revision_date = str(date.today())
revision_query = queries.insert_bible_revision()
        
cursor.execute(revision_query, (
    version_id, None,
    revision_date, False, None, True
    ))

connection.commit()
revision_response = cursor.fetchone()
revision_id = revision_response[0]    


def test_text_dataframe(): 
    verses = []
    bibleRevision = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(np.nan)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_id)

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
        if row["versereference"] in test_data["locations"]:
            location = test_data["locations"].index(row["versereference"])
            if row["text"] in test_data["text"][location]:
                status += 1
                if status == 8:
                    success = True
            else:
                success = False
                break

    assert success is True


def test_text_loading():
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    
    verse_dict = {
        "text": ["TEST"], 
        "biblerevision": [revision_id], 
        "versereference": ["GEN 1:1"]
        }

    verseText = pd.DataFrame(verse_dict)

    text_load = bible_loading.text_loading(verseText, db_engine)
    assert text_load is True

    #TODO - Do an explicit SQL query to check that the data was uploaded.
    # and then another assert.


def test_upload_bible(): 
    verses = []
    bibleRevision = []
    with open("fixtures/test_bible.txt", "r") as f:
        for line in f:
            if line == "\n" or line == "" or line == " ":
                verses.append(np.nan)
                bibleRevision.append(np.nan)
            else:
                verses.append(line.replace("\n", ""))
                bibleRevision.append(revision_id)
        
    bible_upload = bible_loading.upload_bible(verses, bibleRevision)
    fetch_version_query = queries.fetch_bible_version_by_abbreviation()
    cursor.execute(fetch_version_query, ("BLTEST",))
    fetch_response = cursor.fetchone()
    version_id = fetch_response[0]
    delete_version_mutation = queries.delete_bible_version()
    cursor.execute(delete_version_mutation, (version_id,))
    delete_response = cursor.fetchone()
    delete_check = delete_response[0]
    
    connection.commit()
    cursor.close()
    connection.close()

    assert bible_upload is True
    assert delete_check == "loading_test"

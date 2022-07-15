import os

import pandas as pd
import numpy as np
import pytest
from pytest import fixture
import sqlalchemy as db

import bible_loading


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
                bibleRevision.append(3)

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
    for index, row in verseText.iterrows():
        if row["verseReference"] in test_data["locations"]:
            location = test_data["locations"].index(row["verseReference"])
            if row["text"] in test_data["text"][location]:
                status += 1
                if status == 8:
                    success = True
            else:
                success = False
                break

    assert True == success


def test_text_loading():
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    
    verse_dict = {
        "text": ["TEST"], 
        "bibleRevision": [3], 
        "verseReference": ["GEN 1:1"]
        }

    verseText = pd.DataFrame(verse_dict)

    text_load = bible_loading.text_loading(verseText, db_engine)

    assert text_load == True


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
                bibleRevision.append(3)

    bible_upload = bible_loading.upload_bible(verses, bibleRevision)

    assert bible_upload == True

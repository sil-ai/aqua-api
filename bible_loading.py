import os
import sqlalchemy as db
import pandas as pd
from database.models import VerseText


# Parse the revision verses into a dataframe.
def text_dataframe(verses, bible_revision):
    my_col = ["book", "chapter", "verse"]
    vref = pd.read_csv("fixtures/vref.txt", sep=" |:", names=my_col, engine="python")

    vref["text"] = verses
    vref["revision_id"] = bible_revision

    vref = vref.dropna()

    verse_id = []
    for index, row in vref.iterrows():
        ids = (
                row["book"] + " " +
                str(row["chapter"]) + ":" +
                str(row["verse"])
                )

        verse_id.append(ids)

    vref["verse_reference"] = verse_id
    # verse_text = vref.drop(columns=["book", "chapter", "verse"])

    # return verse_text
    return vref


# Direct upload to the SQL database.
async def text_loading(verse_text, db):
    for index, row in verse_text.iterrows():
        verse = VerseText(**row.to_dict())
        db.add(verse)
    await db.commit()
    return True

async def upload_bible(verses, bible_revision, db):
    # initialize SQL engine
    verse_text = text_dataframe(verses, bible_revision)
    await text_loading(verse_text, db)

    return True
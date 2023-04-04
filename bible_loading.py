import os
import sqlalchemy as db
import pandas as pd


# Parse the revision verses into a dataframe.
def text_dataframe(verses, bibleRevision):
    my_col = ["book", "chapter", "verse"]
    vref = pd.read_csv("fixtures/vref.txt", sep=" |:", names=my_col, engine="python")

    vref["text"] = verses
    vref["biblerevision"] = bibleRevision

    vref = vref.dropna()

    verse_id = []
    for index, row in vref.iterrows():
        ids = (
                row["book"] + " " +
                str(row["chapter"]) + ":" +
                str(row["verse"])
                )

        verse_id.append(ids)

    vref["versereference"] = verse_id
    verseText = vref.drop(columns=["book", "chapter", "verse"])

    return verseText


# Direct upload to the SQL database.
def text_loading(verseText, db_engine):
    
    #TODO - what happens when the upload fails?
    # Do we need to have a negative test for trying
    # to upload bad data.
    verseText.to_sql(
            "verseText",
            db_engine,
            index=False,
            if_exists="append",
            chunksize=200
            )
    return True


def upload_bible(verses, bibleRevision):
    # initialize SQL engine
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    verseText = text_dataframe(verses, bibleRevision)
    text_loading(verseText, db_engine)

    return True

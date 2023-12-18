import os
import sqlalchemy as db
import pandas as pd


# Parse the revision verses into a dataframe.
def text_dataframe(verses, bible_revision):
    my_col = ["book", "chapter", "verse"]
    vref = pd.read_csv("fixtures/vref.txt", sep=" |:", names=my_col, engine="python")

    vref["text"] = verses
    vref["bible_revision"] = bible_revision

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
    verse_text = vref.drop(columns=["book", "chapter", "verse"])

    return verse_text


# Direct upload to the SQL database.
def text_loading(verse_text, db_engine):
    
    #TODO - what happens when the upload fails?
    # Do we need to have a negative test for trying
    # to upload bad data.
    verse_text.to_sql(
            "verse_text",
            db_engine,
            index=False,
            if_exists="append",
            chunksize=200
            )
    return True


def upload_bible(verses, bible_revision):
    # initialize SQL engine
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    verse_text = text_dataframe(verses, bible_revision)
    text_loading(verse_text, db_engine)

    return True

from typing import Optional, Dict
import os
import json

import sqlalchemy as db
import pandas as pd

def upload_bible(vref_file, verses, bibleRevision):
    def text_dataframe(vref_file, verses, bibleRevision):
        my_col = ["book", "chapter", "verse"]
        vref = pd.read_csv(vref_file, sep=" |:", names=my_col, engine="python")

        vref["text"] = verses
        vref["bibleRevision"] = bibleRevision

        vref = vref.dropna()

        verse_id = []
        for index, row in vref.iterrows():
            ids = (
                row["book"] + " " +
                str(row["chapter"]) + ":" +
                str(row["verse"])
                )

            verse_id.append(ids)

        vref["verseReference"] = verse_id

        verseText = vref.drop(columns=["book", "chapter", "verse"])

        return verseText


    def text_loading(verseText, db_engine):
        verseText.to_sql(
                "verseText",
                db_engine,
                index=False,
                if_exists="append",
                chunksize=200
                )
        return


    # initialize SQL engine
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    verseText = text_dataframe(vref_file, verses, bibleRevision)
    text_loading(verseText, db_engine)

    return

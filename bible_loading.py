import os
import sqlalchemy as db
import pandas as pd
from database.models import VerseText
import asyncio
import aiofiles
from io import StringIO


# Parse the revision verses into a dataframe.
async def async_text_dataframe(verses, bible_revision):
    my_col = ["book", "chapter", "verse"]
    content = ""

    async with aiofiles.open("fixtures/vref.txt", mode='r') as file:
        content = await file.read()

    def process_data(content):
        data = StringIO(content)
        vref = pd.read_csv(data, sep=" |:", names=my_col, engine='python')
        vref["text"] = verses
        vref["revision_id"] = bible_revision
        vref = vref.dropna()
        verse_id = [f"{row['book']} {row['chapter']}:{row['verse']}" for _, row in vref.iterrows()]
        vref["verse_reference"] = verse_id
        return vref

    loop = asyncio.get_running_loop()
    vref = await loop.run_in_executor(None, process_data, content)
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
    verse_text = await async_text_dataframe(verses, bible_revision)
    await text_loading(verse_text, db)

    return True
from sqlalchemy.sql import insert
import pandas as pd
from database.models import VerseText
import asyncio
import aiofiles
from io import StringIO


# Parse the revision verses into a dataframe.
async def async_text_dataframe(verses, bible_revision):
    my_col = ["book", "chapter", "verse"]
    content = ""

    async with aiofiles.open("fixtures/vref.txt", mode="r") as file:
        content = await file.read()

    def process_data(content):
        data = StringIO(content)
        vref = pd.read_csv(data, sep=" |:", names=my_col, engine="python")
        vref["text"] = verses
        vref["revision_id"] = bible_revision
        vref = vref.dropna()
        vref["verse_reference"] = (
            vref["book"]
            + " "
            + vref["chapter"].astype(str)
            + ":"
            + vref["verse"].astype(str)
        )
        return vref

    loop = asyncio.get_running_loop()
    vref = await loop.run_in_executor(None, process_data, content)
    return vref


async def text_loading(verse_text, db):
    batch_size = 1000
    for start in range(0, len(verse_text), batch_size):
        batch = verse_text[start : start + batch_size]
        await db.execute(insert(VerseText), batch.to_dict(orient="records"))
        await db.commit()


async def upload_bible(verses, bible_revision, db):
    # initialize SQL engine
    verse_text = await async_text_dataframe(verses, bible_revision)
    await text_loading(verse_text, db)

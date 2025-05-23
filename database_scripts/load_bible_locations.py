import json
import os
from typing import Dict, Optional

import pandas as pd
import sqlalchemy as db

vref_filename = "../fixtures/vref.txt"
my_col = ["book", "chapter", "verse"]
vref = pd.read_csv(vref_filename, sep=" |:", names=my_col, engine="python")

with open("../fixtures/bible_books.json", "r") as f:
    books_json = json.load(f)


def create_upsert_method(
    meta: db.MetaData, extra_update_fields: Optional[Dict[str, str]]
):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """

    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = db.Table(table.name, meta, autoload=True)

        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]

        # create insert statement using postgresql dialect.
        # For other dialects, please refer to https://docs.sqlalchemy.org/en/14/dialects/
        insert_stmt = db.dialects.postgresql.insert(sql_table, values_to_insert)

        # create update statement for excluded fields on conflict
        update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded}
        # update_stmt = {"name": "excluded.name"}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)

        # create upsert statement.
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns,  # index elements are primary keys of a table
            set_=update_stmt,  # the SET part of an INSERT statement
        )

        # execute upsert statement
        conn.execute(upsert_stmt)

    return method


def dataframe_creation():
    books = {"abbreviation": [], "name": [], "number": []}
    chapters = {"fullChapterId": [], "number": [], "book_reference": []}
    verses = {"fullVerseId": [], "number": [], "book": [], "chapt": []}

    chapter_dict = {}

    for index, row in vref.iterrows():
        if row["book"] not in books["abbreviation"]:
            books["abbreviation"].append(row["book"])
            books["name"].append(books_json[row["book"]]["name"])
            books["number"].append(books_json[row["book"]]["number"])

        if row["book"] in chapter_dict.keys():
            chapter_dict[row["book"]].append(row["chapter"])
        elif row["book"] not in chapter_dict.keys():
            chapter_dict[row["book"]] = [row["chapter"]]

        verseId = row["book"] + " " + str(row["chapter"]) + ":" + str(row["verse"])

        verses["fullVerseId"].append(verseId)
        verses["number"].append(row["verse"])
        verses["book"].append(row["book"])
        verses["chapt"].append(row["chapter"])

    for book in chapter_dict.keys():
        new_chapter = list(set(chapter_dict[book]))
        for chapter in new_chapter:
            chaptId = book + " " + str(chapter)

            chapters["number"].append(chapter)
            chapters["book_reference"].append(book)
            chapters["fullChapterId"].append(chaptId)

    book_df = pd.DataFrame(books)
    chapter_df = pd.DataFrame(chapters)
    verse_df = pd.DataFrame(verses)

    return book_df, chapter_df, verse_df


def load_books(book_df, upsert_method, db_engine):
    book_df.to_sql(
        "book_reference",
        db_engine,
        index=False,
        if_exists="append",
        chunksize=200,
        method=upsert_method,
    )
    return


def load_chapters(chapter_df, upsert_method, db_engine):
    chapter_df.to_sql(
        "chapter_reference",
        db_engine,
        index=False,
        if_exists="append",
        chunksize=200,
        method=upsert_method,
    )

    chapter_sql = pd.read_sql_table("chapter_reference", db_engine)

    return chapter_sql


def load_verses(verse_df, upsert_method, db_engine, chapter_sql):
    chapter_list = []
    for index, row in verse_df.iterrows():
        book = row["book"]
        chapter = row["chapt"]
        ids = chapter_sql.loc[
            (chapter_sql["book_reference"] == book)
            & (chapter_sql["number"] == chapter),
            "fullChapterId",
        ].values[0]
        chapter_list.append(ids)

    verse_df["chapter"] = chapter_list

    verse_df = verse_df.drop(columns=["book", "chapt"])

    verse_df.to_sql(
        "verse_reference",
        db_engine,
        index=False,
        if_exists="append",
        chunksize=200,
        method=upsert_method,
    )
    return


def main():
    # initialize SQL engine
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    meta = db.MetaData(db_engine)
    upsert_method = create_upsert_method(meta, None)

    # Create dataframes
    df_book, df_chapter, df_verse = dataframe_creation()

    # Load books
    load_books(df_book, upsert_method, db_engine)
    chapter_sql = load_chapters(df_chapter, upsert_method, db_engine)
    load_verses(df_verse, upsert_method, db_engine, chapter_sql)


if __name__ == "__main__":
    main()

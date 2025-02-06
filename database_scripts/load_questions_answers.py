import json
import os
from typing import Dict, Optional

import pandas as pd
import sqlalchemy as db


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
    dir = "../fixtures/comprehension_questions"
    lang_df = {
        "start_verse": [],
        "end_verse": [],
        "language": [],
        "question": [],
        "answer": [],
    }

    for files in os.listdir(dir):
        language = files[0:3]

        with open(os.path.join(dir, files), "r") as f:
            data = json.load(f)

            for line in data:
                for qas in line["qas"]:
                    question = qas["question"]
                    id = (qas["id"][33::]).split("/")[0]
                    id_split = id.replace("c", " ")
                    verse_id = id_split.replace("v", ":")

                    for answers in qas["answers"]:
                        answer = answers["text"]
                        lang_df["question"].append(question)
                        lang_df["answer"].append(answer)
                        lang_df["language"].append(language)

                        if "-" not in verse_id:
                            start_ver = verse_id
                            lang_df["start_verse"].append(start_ver)
                            lang_df["end_verse"].append(start_ver)
                        elif "-" in verse_id:
                            ver = verse_id.split("-")
                            start_ver = ver[0]
                            end = start_ver.split(":")
                            end_ver = end[0] + ":" + ver[1]
                            lang_df["start_verse"].append(start_ver)
                            lang_df["end_verse"].append(end_ver)

    qa_df = pd.DataFrame(lang_df)

    return qa_df


def load_qas(qa_df, upsert_method, db_engine):
    qa_df.to_sql(
        "question",
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
    df_qa = dataframe_creation()

    # Load books
    load_qas(df_qa, upsert_method, db_engine)


if __name__ == "__main__":
    main()

import shutil
import os
import urllib.request
import zipfile
from typing import Optional, Dict

import sqlalchemy as db
import pandas as pd


# ISO639-3 code tables
iso_code_link = "https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3_Code_Tables_20220311.zip"
iso_code_zipfile = "iso-639-3_Code_Tables_20220311.zip"
iso_code_filename = "iso-639-3_Code_Tables_20220311/iso-639-3_20220311.tab"

# ISO 15924 script table
iso_script_link = "https://raw.githubusercontent.com/interscript/iso-15924/master/iso_15924.txt"
iso_script_filename = "iso_15924.txt"

# urllib setup
opener = urllib.request.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0')]
urllib.request.install_opener(opener)


def create_upsert_method(meta: db.MetaData, extra_update_fields: Optional[Dict[str, str]]):
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
        #update_stmt = {"name": "excluded.name"}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)
        
        # create upsert statement. 
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns, # index elements are primary keys of a table
            set_=update_stmt # the SET part of an INSERT statement
        )
        
        # execute upsert statement
        conn.execute(upsert_stmt)

    return method


def download_iso_langs():
    urllib.request.urlretrieve(iso_code_link, iso_code_zipfile)
    with zipfile.ZipFile(iso_code_zipfile, 'r') as zip_ref:
        zip_ref.extractall('.')
    df = pd.read_csv(iso_code_filename, sep='\t')
    files = os.listdir('.')
    for item in files:
        if item.endswith(".zip"):
            os.remove(os.path.join('.', item))
        elif "iso-639-3" in item:
            shutil.rmtree(os.path.join('.', item))
        else:
            pass
    df = df[['Id', 'Ref_Name']]
    df.rename(columns={'Id': 'iso639', 'Ref_Name': 'name'}, inplace=True)
    df.dropna(inplace=True)
    return df


def load_iso_langs(langs, upsert_method, db_engine):
    langs.to_sql(
            "iso_language",
            db_engine,
            index=False,
            if_exists="append",
            chunksize=200, 
            method=upsert_method
    )
    return


def download_iso_scripts():
    urllib.request.urlretrieve(iso_script_link, iso_script_filename)
    df = pd.read_csv(iso_script_filename, sep=';', skiprows=7, 
            names=["iso15924", "Number", "name", "French Name", "PVA", "Unicode Version", "Date"])
    os.remove(iso_script_filename)
    df = df[['iso15924', 'name']]
    df.dropna(inplace=True)
    return df


def load_iso_scripts(scripts, upsert_method, db_engine):
    scripts.to_sql(
            "iso_script",
            db_engine,
            index=False,
            if_exists="append",
            chunksize=200, 
            method=upsert_method
    )
    return


def main():

    # initialize SQL engine
    db_engine = db.create_engine(os.getenv("AQUA_DB"))
    meta = db.MetaData(db_engine)
    upsert_method = create_upsert_method(meta, None)

    # Download the ISO language codes
    langs = download_iso_langs()

    # Load ISO languages
    load_iso_langs(langs, upsert_method, db_engine)

    # Download the ISO script codes
    scripts = download_iso_scripts()

    # Load ISO Scripts
    load_iso_scripts(scripts, upsert_method, db_engine)


if __name__ == "__main__":
    main()

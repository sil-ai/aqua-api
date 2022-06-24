class bible_text(verses, version, date, published):
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


    def text_dataframe(vref_file, verses):
        my_col = ["book", "chapter", "verse"]
        vref = pd.read_csv(vref.file, sep=" |:", names=my_col, engine="python")

        vref["text"] = verses

        verse_id = []
        for index, row in vref.iterrows():
            ids = (
                row["book"] + " " +
                row["chapter"] + ":" +
                row["verse"]
                )

        vref["verseReference"] = verse_id

        verseText = vref.drop(columns=["book", "chapter", "verse"])

        return verseText


    def text_loading(verseText, upsert_method, db_engine):
        verseText.to_sql(
                "verseText",
                db_engine,
                index=False,
                if_exists="append",
                chunksize=200, 
                method=upsert_method
                )
        return


    def text_uploading(verses):
        # initialize SQL engine
        db_engine = db.create_engine(os.getenv("AQUA_DB"))
        meta = db.MetaData(db_engine)
        upsert_method = create_upsert_method(meta, None)

        verseText = text_dataframe("vref.txt", verses)

        return


    def revision_mutation(version, date, published):
        bible_revise = """
            mutation MyMutation {
              insert_bibleRevision(objects: {bibleVersion: {}, date: {}, published: {}}) {
                returning {
                  id
                }
              }
            }
            """.format(version, date, published)
        
        return bible_revise

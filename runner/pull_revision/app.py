import os
from pathlib import Path
import logging

import modal

logging.basicConfig(level=logging.DEBUG)


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(
    name="pull_revision" + suffix,
    image=modal.Image.debian_slim()
    .pip_install(
        "numpy==1.24.1",
        "pandas==1.4.3",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
    )
    .copy(mount=modal.Mount(local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root")))
    .copy(mount=modal.Mount(
        local_file='db_connect.py',
        remote_dir='/root'
        )
    )
)

class RecordNotFoundError(Exception):
    def __init__(self, message):
        self.message = message


class PullRevision:
    def __init__(self):
        self.vref = self.prepare_vref()

    @staticmethod
    def prepare_vref():
        try:
            return open("/root/vref.txt").read().splitlines()
        except FileNotFoundError as err:
            raise FileNotFoundError(err) from err

    @staticmethod
    def is_duplicated(refs):
        return len(refs) != len(set(refs))

    @stub.function(secret=modal.Secret.from_name("aqua-db"))
    def pull_revision(self, revision_id):
        from db_connect import get_session, VerseText
        import pandas as pd

        __, session = next(get_session())
        logging.info("Loading verses from Revision %s...", revision_id)
        revision_verses = pd.read_sql(
            session.query(VerseText)
            .filter(VerseText.bibleRevision == revision_id)
            .statement,
            session.bind,
        )
        if not revision_verses.empty:
            # checks that the version doesn't have duplicated verse references
            if not self.is_duplicated(revision_verses.verseReference):
                # loads the verses as part of the PullRevision object
                return revision_verses.set_index("id", drop=True)
            else:
                logging.info("Duplicated verses in Revision %s", revision_id)
                return None
        else:
            logging.info("No verses for Revision %s", revision_id)
            raise RecordNotFoundError(
                f"Revision {revision_id} was not found in the database."
            )

    @stub.function
    def prepare_output(self, revision_text):
        import pandas as pd
        import numpy as np

        # outer merges the vref list on the revision verses
        vref = pd.Series(self.vref, name="verseReference")
        all_verses = pd.merge(
            revision_text, vref, on="verseReference", how="outer"
        )
        # customed sort index
        vref_sort_index = dict(zip(self.vref, range(len(self.vref))))
        # map the sort order
        all_verses["sort_order"] = all_verses["verseReference"].map(vref_sort_index)
        # sort all verses based on vref custom sort
        all_verses.sort_values("sort_order", inplace=True)
        all_verses_text = all_verses["text"].replace(np.nan, "", regex=True)
        return all_verses_text.to_list()

    @stub.function
    def output_revision(self, revision_text):
        print(revision_text)
        if not revision_text.empty:
            output_text = self.prepare_output.call(revision_text)
            return output_text
        else:
            logging.info("Revision text is empty. Nothing printed.")
            return []

@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def pull_revision(revision_id: int) -> bytes:
    pr = PullRevision()
    revision_text = pr.pull_revision.call(revision_id)
    revision_bytes = pr.output_revision.call(revision_text)
    return revision_bytes

if __name__ == '__main__':
    with stub.run():
        rev1 = pull_revision.call(1)
        import pickle
        pickle.dump(rev1,open('rev1.pkl','wb'))

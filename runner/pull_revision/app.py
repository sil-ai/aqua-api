import os
from pathlib import Path
import logging

import modal
import pandas as pd
import numpy as np

from db_connect import get_session, VerseText


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
    .copy(mount=modal.Mount(local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root"))),
)


class RecordNotFoundError(Exception):
    def __init__(self, message):
        self.message = message


class PullRevision:
    def __init__(self, revision_id: int):
        self.revision_id = revision_id
        self.revision_text = pd.DataFrame()
        self.vref = self.prepare_vref()

    @staticmethod
    def prepare_vref():
        try:
            return pd.Series(
                open("/root/vref.txt").read().splitlines(), name="verseReference"
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(err) from err

    @staticmethod
    def is_duplicated(refs):
        return len(refs) != len(set(refs))

    def pull_revision(self):
        __, session = next(get_session())
        logging.info("Loading verses from Revision %s...", self.revision_id)
        revision_verses = pd.read_sql(
            session.query(VerseText)
            .filter(VerseText.bibleRevision == self.revision_id)
            .statement,
            session.bind,
        )
        if not revision_verses.empty:
            # checks that the version doesn't have duplicated verse references
            if not self.is_duplicated(revision_verses.verseReference):
                # loads the verses as part of the PullRevision object
                self.revision_text = revision_verses.set_index("id", drop=True)
            else:
                logging.info("Duplicated verses in Revision %s", self.revision_id)
        else:
            logging.info("No verses for Revision %s", self.revision_id)
            raise RecordNotFoundError(
                f"Revision {self.revision_id} was not found in the database."
            )

    def prepare_output(self):
        # outer merges the vref list on the revision verses
        all_verses = pd.merge(
            self.revision_text, self.vref, on="verseReference", how="outer"
        )
        # customed sort index
        vref_sort_index = dict(zip(self.vref, range(len(self.vref))))
        # map the sort order
        all_verses["sort_order"] = all_verses["verseReference"].map(vref_sort_index)
        # sort all verses based on vref custom sort
        all_verses.sort_values("sort_order", inplace=True)
        all_verses_text = all_verses["text"].replace(np.nan, "", regex=True)
        return all_verses_text.to_list()

    def output_revision(self):
        if not self.revision_text.empty:
            output_text = self.prepare_output()
            return output_text

        else:
            logging.info("Revision text is empty. Nothing printed.")
            return []


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def pull_revision(revision_id: int) -> bytes:
    pr = PullRevision(revision_id)
    pr.pull_revision()
    revision_bytes = pr.output_revision()

    return revision_bytes
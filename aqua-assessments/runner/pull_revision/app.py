import logging
import os
from pathlib import Path

import modal
from db_connect import get_session

logging.basicConfig(level=logging.INFO)


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"


app = modal.App(
    name="pull-revision" + suffix,
    image=modal.Image.debian_slim()
    .apt_install("libpq-dev", "gcc")
    .pip_install(
        "numpy~=1.26.0", "pandas~=1.5.0", "psycopg2-binary~=2.9.0", "sqlalchemy~=1.4.0"
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("../../fixtures/vref.txt"),
            remote_path=Path("/root/vref.txt"),
        )
    ),
)


class RecordNotFoundError(Exception):
    def __init__(self, message):
        self.message = message


class DuplicateVersesError(Exception):
    def __init__(self, message):
        self.message = message


class PullRevision:
    def __init__(self, revision_id: int, AQUA_DB: str):
        import pandas as pd

        self.revision_id = revision_id
        self.revision_text = pd.DataFrame()
        self.vref = self.prepare_vref()
        self.AQUA_DB = AQUA_DB

    @staticmethod
    def prepare_vref():
        import pandas as pd

        try:
            return pd.Series(
                open("/root/vref.txt").read().splitlines(), name="verse_reference"
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(err) from err

    @staticmethod
    def is_duplicated(refs):
        return len(refs) != len(set(refs))

    def get_verses(self):
        import pandas as pd
        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()

        class VerseText(Base):
            __tablename__ = "verse_text"
            # TODO: check what the original field lengths are
            id = Column(Integer, primary_key=True)
            # transfers all but one Apocryphal verse ESG 10:3
            text = Column(String(550, "utf8_unicode_ci"), nullable=False)
            revision_id = Column(Integer, nullable=False, index=True)
            # transfers across all reference strings
            verse_reference = Column(
                String(15, "utf8_unicode_ci"), nullable=False, index=True
            )

        __, session = next(get_session(self.AQUA_DB))
        logging.info("Loading verses from Revision %s...", self.revision_id)
        revision_verses = pd.read_sql(
            session.query(VerseText)
            .filter(VerseText.revision_id == self.revision_id)
            .statement,
            session.bind,
        )

        print(f"Verses loaded from Revision {self.revision_id}.")
        print(f"{revision_verses.head()=}")
        return revision_verses

    def pull_revision(self):
        revision_verses = self.get_verses()
        if not revision_verses.empty:
            # checks that the version doesn't have duplicated verse references
            if "versereference" in revision_verses.columns:
                revision_verses.rename(
                    columns={"versereference": "verse_reference"}, inplace=True
                )

            print("checking-")
            print(revision_verses.columns)
            if not self.is_duplicated(revision_verses.verse_reference):
                # loads the verses as part of the PullRevision object
                self.revision_text = revision_verses.set_index("id", drop=True)
            else:
                logging.info("Duplicated verses in Revision %s", self.revision_id)
                raise DuplicateVersesError(
                    f"Revision {self.revision_id} has duplicate verses"
                )
        else:
            logging.info("No verses for Revision %s", self.revision_id)
            raise RecordNotFoundError(
                f"Revision {self.revision_id} was not found in the database."
            )

    def prepare_output(self):
        import numpy as np
        import pandas as pd

        # outer merges the vref list on the revision verses
        all_verses = pd.merge(
            self.revision_text, self.vref, on="verse_reference", how="outer"
        )
        # customed sort index
        vref_sort_index = dict(zip(self.vref, range(len(self.vref))))
        # map the sort order
        all_verses["sort_order"] = all_verses["verse_reference"].map(vref_sort_index)
        # sort all verses based on vref custom sort
        all_verses.sort_values("sort_order", inplace=True)
        all_verses_text = all_verses["text"].replace(np.nan, "", regex=True)
        return all_verses_text.to_list()

    def output_revision(self):
        if not self.revision_text.empty:
            output_text = self.prepare_output()
            return output_text

        else:
            print("Revision text is empty. Nothing printed.")
            return []


@app.function(timeout=600)
def pull_revision(revision_id: int, AQUA_DB: str) -> bytes:
    pr = PullRevision(revision_id, AQUA_DB)
    print(f"Pulling revision {revision_id}...")
    pr.pull_revision()
    revision_bytes = pr.output_revision()

    return revision_bytes

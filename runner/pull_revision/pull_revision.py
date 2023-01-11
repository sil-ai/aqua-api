import os
from pathlib import Path
import logging

import modal
import numpy as np
import pandas as pd
from gql.transport.requests import RequestsHTTPTransport
from gql import Client, gql

import queries
from db_connect import get_session, VerseText

logging.basicConfig(level=logging.DEBUG)

local_data_dir = Path("./")
remote_data_dir = Path("/")
# Configure connection to the GRAPHQL endpoint
headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
transport = RequestsHTTPTransport(
    url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
)


stub = modal.Stub(
    name="pull_revision",
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "gql==3.3.0",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
    ),
)


class PullRevision:
    def __init__(self, revision_id: int, version_abbr: str):
        self.revision_id = revision_id
        self.revision_text = pd.DataFrame()
        self.vref = self.prepare_vref()

    @staticmethod
    def prepare_vref():
        try:
            return pd.Series(
                open("/vref.txt").read().splitlines(), name="verseReference"
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
        else:
            logging.info("Revision text is empty. Nothing printed.")
            return []
        return output_text


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("my-aws-secret-api"),
    mounts=[
        modal.Mount(local_dir=local_data_dir, remote_dir=remote_data_dir)
    ],  # This is needed to get vref.txt
)
def pull_revision(revision_id: int) -> bytes:
    get_version_id = queries.fetch_bible_version_from_revision(revision_id)
    with Client(transport=transport, fetch_schema_from_transport=True) as client:
        query = gql(get_version_id)
        result = client.execute(query)
        version_name = result["bibleVersion"][0]["abbreviation"]
    try:
        pr = PullRevision(revision_id, version_name)
        pr.pull_revision()
        revision_bytes = pr.output_revision()
    except (ValueError, OSError, KeyError, AttributeError, FileNotFoundError) as err:
        logging.error(err)
    return revision_bytes

import numpy as np
from datetime import datetime
import logging
import argparse
import os
from pathlib import Path

# TODO: make a decision about logging API-wide from Issue 40
logging.basicConfig(level=logging.DEBUG)

import pandas as pd
import modal
import boto3


from db_connect import get_session, VerseText

stub = modal.Stub(
    name="pull_rev",
    image=modal.Image.debian_slim().pip_install(
        "machine==0.0.1",
        "pandas==1.4.3",
        # "pytest==7.1.2",
        "sil-machine[thot]>=0.8.3",
        "diskcache",
        "boto3",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",  # ==2.8.6
    ),
)
local_data_dir = Path("../../../assessments/word_alignment/data")
remote_data_dir = Path("/data/")


class PullRevision:
    # @stub.function(timeout=600, secret=modal.Secret.from_name("my-aws-secret-api"))
    def __init__(self, revision_id: int, version_name: str):
        # gets the args of the form --revision 3 --out /path/to/output/file
        # args = self.get_args()
        # if not (args.revision and args.out):
        #     raise ValueError('Missing Revision Id or output path')
        # initializes the instance variables
        self.revision_id = revision_id
        self.version_name = version_name
        self.revision_text = pd.DataFrame()
        self.vref = self.prepare_vref()
        self.out = Path("./")

    @staticmethod
    def prepare_vref():
        # ??? maybe consolidate the name to one variable?
        try:
            return pd.Series(
                open("/data/vref.txt").read().splitlines(), name="verseReference"
            )
        except FileNotFoundError as err:
            raise FileNotFoundError(err) from err

    # @staticmethod
    # def get_args():
    #     #initializes a command line argument parser
    #     parser = argparse.ArgumentParser(description='Pull and output verses from a revision')
    #     parser.add_argument('-r','--revision', type=int, help='Revision ID', required=True)
    #     parser.add_argument('-o','--out', type=str, help='Output path', required=True)
    #     #gets the arguments - will fail if they are of the wrong type
    #     try:
    #         return parser.parse_args()
    #     except SystemExit as sys_exit:
    #         if sys_exit.code == 2:
    #             raise ValueError('Argument error') from sys_exit
    #         else:
    #             raise ValueError(sys_exit.code) from sys_exit

    @staticmethod
    def is_duplicated(refs):
        return len(refs) != len(set(refs))

    # @stub.function(secret=modal.Secret.from_name("my-aws-secret"))
    def pull_revision(self):
        # with postgres connection gets the verses from the verseText table
        # ??? Think about dividing get_session into get_engine and get_session
        __, session = next(get_session())
        logging.info("Loading verses from Revision %s...", self.revision_id)
        # builds a dataframe of verses from the revision in self.revision_id
        revision_verses = pd.read_sql(
            session.query(VerseText)
            .filter(VerseText.bibleRevision == self.revision_id)
            .statement,
            session.bind,
        )
        # ??? Maybe rework as a try/except block? Seems convoluted
        if not revision_verses.empty:
            # checks that the version doesn't have duplicated verse references
            if not self.is_duplicated(revision_verses.verseReference):
                # loads the verses as part of the PullRevision object
                self.revision_text = revision_verses.set_index("id", drop=True)
            else:
                logging.info("Duplicated verses in Revision %s", self.revision_id)
        else:
            logging.info("No verses for Revision %s", self.revision_id)
        return self

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

    # @stub.function(timeout=600, secret=modal.Secret.from_name("my-aws-secret-api"))
    def output_revision(self, s3_outpath):
        # Check whether the specified path exists or not
        print("Outputting the revision")
        isExist = os.path.exists(self.out)
        if not isExist:
            os.makedirs(self.out)
        # date = datetime.now().strftime("%Y_%m_%d")
        # saves the output as a txt file with revision_id and unix date
        if not self.revision_text.empty:
            output_text = self.prepare_output()
            filename = f"{self.version_name}_{self.revision_id}.txt"
            filepath = self.out / filename
            with open(filepath, "w") as outfile:
                for verse_text in output_text:
                    outfile.write(f"{verse_text}\n")
            self.upload_texts(filepath, s3_outpath)
            logging.info(
                "Revision %s saved to file %s in location %s",
                self.revision_id,
                filename,
                self.out,
            )
            return filename
        else:
            logging.info("Revision text is empty. Nothing printed.")

    # @stub.function(timeout=600, secret=modal.Secret.from_name("my-aws-secret-api"))
    def upload_texts(self, filepath: Path, s3_outpath: str):
        s3 = boto3.client("s3")
        s3.upload_file(filepath, "aqua-word-alignment", s3_outpath)


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("my-aws-secret-api"),
    mounts=[modal.Mount(local_dir=local_data_dir, remote_dir=remote_data_dir)],
)
def run_pull_rev(revision_id, version_name, s3_outpath):
    try:
        pr = PullRevision(revision_id, version_name)
        pr.pull_revision()
        pr.output_revision(s3_outpath)
    except (ValueError, OSError, KeyError, AttributeError, FileNotFoundError) as err:
        logging.error(err)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull and output verses from a revision"
    )
    parser.add_argument("-r", "--revision", type=int, help="Revision ID", required=True)
    parser.add_argument("-o", "--out", type=str, help="Output path", required=True)
    args = parser.parse_args()
    run_pull_rev(args.revision)

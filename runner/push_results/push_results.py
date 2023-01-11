import logging
import os

import pandas as pd
import modal

from db_connect import get_session


# Manage suffix on modal endpoint if testing.
suffix = ''
if os.environ.get('MODAL_TEST') == 'TRUE':
    suffix = '_test'


stub = modal.Stub(
    name="push_results" + suffix,
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "gql==3.3.0",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
    ),
)


class PushResults:

    def __init__(self, revision_id: int, assessment_id: int, results: pd.DataFrame):
        self.revision_id = revision_id
        self.assessment_id = assessment_id
        self.results = results

    def push_results(self):
        engine, session = next(get_session())
        logging.info(f'Pushing results from assessment {self.assessment_id} for revision {self.revision_id}')
        self.results.to_sql('assessmentResult', con=engine, if_exists='replace', index=False)

def pull_revision(revision_id: int, assessment_id: int, results: pd.DataFrame):
    try:
        pr = PushResults(revision_id, assessment_id, results)
        pr.push_results()
    except (ValueError, OSError, KeyError, AttributeError, FileNotFoundError) as err:
        logging.error(err)

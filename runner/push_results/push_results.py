import logging
import os

import pandas as pd
import modal
from sqlalchemy.exc import IntegrityError

from db_connect import get_session
from models import AssessmentResult, Results


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


class PushResults():

    def __init__(self, results: Results):
        self.results = results.results

    def __del__(self):
        self.session.close()


    def insert(self):
        self.engine, self.session = next(get_session())

        try:
            #sanity check that the assessment id is new
            # assert self.assessment_is_new(self.results.assessment_id), f"Result with assessment id {self.results.assessment_id} exists"
            for result in self.results:
                self.insert_item(result)
            self.session.commit()
            return 200, 'OK'
        except (IntegrityError, AssertionError) as err:
            self.session.rollback()
            return 500, err

    def assessment_is_new(self, assess_id):
        stmt = 'select * from "assessmentResult";'
        return len(list(filter(lambda item:item[1]==assess_id, self.session.execute(stmt)))) == 0
        #self.session.query(AssessmentResult).filter_by(assessment=self.assess_id).first() is None

    def insert_item(self, result):
        ar = AssessmentResult(
                    assessment = result.assessment_id,
                    vref = result.vref,
                    score = result.score,
                    flag = False,

        )
        self.session.add(ar)



        # self.results.to_sql('assessmentResult', con=engine, if_exists='replace', index=False)
@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("my-aws-secret-api"),
)
def push_results(results: Results):
    try:
        pr = PushResults(results)
        pr.insert()
    except (ValueError, OSError, KeyError, AttributeError, FileNotFoundError) as err:
        logging.error(err)

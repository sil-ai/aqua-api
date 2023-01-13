import os

import modal
from sqlalchemy.exc import IntegrityError

from db_connect import get_session
from models import AssessmentResult, Results


# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"


stub = modal.Stub(
    name="push_results" + suffix,
    image=modal.Image.debian_slim().pip_install(
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
    ),
)


class PushResults:
    def __init__(self, results: Results):
        self.results = results.results

    def __del__(self):
        self.session.close()

    def insert(self):
        self.engine, self.session = next(get_session())

        try:
            for result in self.results:
                self.insert_item(result)
            self.session.commit()
            return 200, "OK"
        except (IntegrityError, AssertionError) as err:
            self.session.rollback()
            return 500, err

    def insert_item(self, result):
        ar = AssessmentResult(
            assessment=result.assessment_id,
            vref=result.vref,
            score=result.score,
            flag=False,
        )
        self.session.add(ar)


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def push_results(results: Results):
    pr = PushResults(results)
    response = pr.insert()
    return response

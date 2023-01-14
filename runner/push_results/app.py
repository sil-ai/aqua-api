import os
from typing import List

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
    def __init__(self):
        self.engine, self.session = next(get_session())

    def __del__(self):
        self.session.close()

    def insert(self, results: Results):
        self.results = results
        self.create_bulk_results()

        try:
            ids = self.bulk_insert_items()
            self.session.commit()

            return 200, ids
        except (IntegrityError, AssertionError) as err:
            self.session.rollback()
            return 500, err

    def create_bulk_results(self):
        self.assessment_results = []
        for result in self.results.results:
            ar = AssessmentResult(
                assessment=result.assessment_id,
                vref=result.vref,
                score=result.score,
                flag=False,
            )
            self.assessment_results.append(ar)

    def bulk_insert_items(self):
        self.session.bulk_save_objects(self.assessment_results, return_defaults=True)
        self.session.flush()
        ids = [ar.id for ar in self.assessment_results]

        return ids

    def delete(self, ids: List[int]):
        self.session.query(AssessmentResult).filter(
            AssessmentResult.id.in_(ids)
        ).delete(synchronize_session="fetch")
        self.session.commit()


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def push_results(results: Results):
    pr = PushResults()
    response, ids = pr.insert(results)
    return response, ids


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def delete_results(ids: List[int]):
    pr = PushResults()
    pr.delete(ids)
    return 200, "OK"

import os
from typing import List

import modal

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
    ).copy(modal.Mount(
            local_file="models.py",
            remote_dir="/root"
            )
    ).copy(modal.Mount(
            local_file="db_connect.py",
            remote_dir="/root"
        )
    ),
    secret=modal.Secret.from_name('aqua-db')
)

class PushResults:
    def __init__(self):
        pass

    @stub.function(secret=modal.Secret.from_name('aqua-db'))
    def insert(self, results):
        from sqlalchemy.exc import IntegrityError
        from db_connect import get_session
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy import Column, Integer, String, Numeric, Boolean
        __, session = next(get_session())

        Base = declarative_base()

        class AssessmentResult(Base):
            __tablename__ = "assessmentResult"
            id = Column(Integer, primary_key=True)
            assessment = Column(Integer, nullable=False)
            score = Column(Numeric, nullable=True)
            flag = Column(Boolean, default=False)
            note = Column(String(1024, "utf8_unicode_ci"), nullable=True)

        bulk_results = [AssessmentResult(assessment = result.assessment_id,
                                         score=result.score,
                                         note=result.vref)
                             for result in results]
        ids = [ar.id for ar in bulk_results]
        try:
            session.bulk_save_objects(bulk_results, return_defaults=True)
        
            session.commit()
            session.close()
            return 200, ids
        except (IntegrityError, AssertionError) as err:
            session.rollback()
            session.close()
            return 500, err

    @stub.function(secret=modal.Secret.from_name('aqua-db'))
    def delete(self, ids: List[int]):
        from db_connect import get_session
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy import Column, Integer, String, Numeric, Boolean
        __, session = get_session()

        Base = declarative_base()

        class AssessmentResult(Base):
            __tablename__ = "assessmentResult"
            id = Column(Integer, primary_key=True)
            assessment = Column(Integer, nullable=False)
            score = Column(Numeric, nullable=True)
            flag = Column(Boolean, default=False)
            note = Column(String(1024, "utf8_unicode_ci"), nullable=True)

        session.query(AssessmentResult).filter(
            AssessmentResult.id.in_(ids)
        ).delete(synchronize_session="fetch")
        session.commit()

@stub.function(
    timeout=600,
)
def push_results(results):
    pr = PushResults()
    #response, ids = pr.insert(results)
    #return response, ids
    return pr.insert.call(results)

@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def delete_results(ids: List[int]):
    pr = PushResults()
    pr.delete(ids)
    return 200, "OK"

if __name__ == '__main__':
    import pickle
    results = pickle.load(open('../../assessments/semantic-similarity/fixtures/results_jan_20.pkl','rb'))
    with stub.run():
        status = push_results.call(results.results[:10])

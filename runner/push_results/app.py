import os
from typing import List

import modal

from db_connect import get_session
from models import Result, Results


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
        from sqlalchemy.exc import IntegrityError

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
        from sqlalchemy.orm import declarative_base
        Base = declarative_base()

        class AssessmentResult(Base):
            from sqlalchemy import Column, Integer, Text, Boolean, Float, ForeignKey


            __tablename__ = "assessmentResult"
            id = Column(Integer, primary_key=True)  # autoincrements by default
            assessment = Column(Integer, ForeignKey("assessment.id"), nullable=False)
            vref = Column(
                Text, ForeignKey("verseReference.fullVerseId")
            )  # vref format 'Gen 1:1'
            score = Column(Float)
            flag = Column(Boolean, default=False)
            note = Column(Text)

            def __repr__(self):
                return (
                    f"Assessment Result({self.id}) -> {self.assessment}/{self.vref}\n"
                    f"score={self.score} flag={self.flag}, note={self.note}"
                )

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
        from sqlalchemy.orm import declarative_base
        Base = declarative_base()

        class AssessmentResult(Base):
            from sqlalchemy import Column, Integer, Text, Boolean, Float, ForeignKey


            __tablename__ = "assessmentResult"
            id = Column(Integer, primary_key=True)  # autoincrements by default
            assessment = Column(Integer, ForeignKey("assessment.id"), nullable=False)
            vref = Column(
                Text, ForeignKey("verseReference.fullVerseId")
            )  # vref format 'Gen 1:1'
            score = Column(Float)
            flag = Column(Boolean, default=False)
            note = Column(Text)

            def __repr__(self):
                return (
                    f"Assessment Result({self.id}) -> {self.assessment}/{self.vref}\n"
                    f"score={self.score} flag={self.flag}, note={self.note}"
                )

        self.session.query(AssessmentResult).filter(
            AssessmentResult.id.in_(ids)
        ).delete(synchronize_session="fetch")
        self.session.commit()


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def push_results(results: List):
    results_obj = []
    for result in results:
        result_obj = Result(**result)
        results_obj.append(result_obj)
    results_obj = Results(results=results_obj)
    pr = PushResults()
    response, ids = pr.insert(results_obj)
    return response, ids


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def delete_results(ids: List[int]):
    pr = PushResults()
    pr.delete(ids)
    return 200, "OK"

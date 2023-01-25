import os
from typing import List
import modal

from db_connect import get_session
from models import Result, Results

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from key_fetch import get_secret

# Use Token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    # run api key fetch function requiring 
    # input of AWS credentials   
    api_keys = get_secret(
            os.getenv("KEY_VAULT"),
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )
    if api_key not in api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    return True

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
        "boto3==1.26.56",
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
        from sqlalchemy import Column, Integer, Text, Boolean, Float, ForeignKey, DateTime

        class AssessmentResult(Base):
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
        
        class Assessment(Base):
            __tablename__ = "assessment"
            id = Column(Integer, primary_key=True)  # autoincrements by default
            revision = Column(Integer)
            reference = Column(Integer, ForeignKey("bibleRevision.id"))
            type = Column(Text)
            finished = Column(Boolean)
            time_inserted = Column(DateTime)

            def __repr__(self):
                return (
                    f"Assessment({self.id}) - {self.type} "
                    f"revision={self.revision} reference={self.reference}, finished={self.finished}"
                )
        
        class VerseReference(Base):
            __tablename__ = "verseReference"
            fullVerseId = Column(Text, primary_key=True)
            number = Column(Integer)
            chapter = Column(Text, ForeignKey("chapterReference.fullChapterId"))


        class ChapterReference(Base):
            __tablename__ = "chapterReference"
            fullChapterId = Column(Text, primary_key=True)
            number = Column(Integer)
            bookReference = Column(Text, ForeignKey("bookReference.abbreviation"))


        class BookReference(Base):
            __tablename__ = "bookReference"
            abbreviation = Column(Text, primary_key=True)
            name = Column(Text)
            number = Column(Integer)

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
    secrets=[modal.Secret.from_name("aqua-db"),modal.Secret.from_name("my-aws-secret")],
    mounts=modal.create_package_mounts(['key_fetch']),
    dependencies=[Depends(api_key_auth)],
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
    secrets=[modal.Secret.from_name("aqua-db"),modal.Secret.from_name("my-aws-secret")],
    mounts=modal.create_package_mounts(['key_fetch']),
    dependencies=[Depends(api_key_auth)],
)
def delete_results(ids: List[int]):
    pr = PushResults()
    pr.delete(ids)
    return 200, "OK"

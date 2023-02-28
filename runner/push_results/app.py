import os
from typing import List, Optional
import modal
from pydantic import BaseModel

from db_connect import get_session
from models import Result

# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "-test"


stub = modal.Stub(
    name="push-results" + suffix,
    image=modal.Image.debian_slim().pip_install(
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
        "psycopg2-binary",
        "pandas==1.4.3"
    ),
)


class PushResults:
    def __init__(self, AQUA_DB: str):
        self.engine, self.session = next(get_session(AQUA_DB))

    def __del__(self):
        self.session.close()

    def insert_results(self, results: List[Result]):
        from sqlalchemy.exc import IntegrityError

        self.results = results
        self.create_bulk_results()

        try:
            ids = self.bulk_insert_results()
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
            source = Column(Text)
            target = Column(Text)
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
        for result in self.results:
            ar = AssessmentResult(
                assessment=result.assessment_id,
                vref=result.vref,
                source=result.source,
                target=result.target,
                score=result.score,
                flag=result.flag,
                note=result.note,
            )
            self.assessment_results.append(ar)

    def bulk_insert_results(self):
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
)
def push_results(results: List, AQUA_DB: str):
    class Result(BaseModel):
        id: Optional[int] = None
        assessment_id: int
        vref: str
        source: Optional[str] = None
        target: Optional[str] = None
        score: float
        flag: bool = False
        note: Optional[str] = None
    
    results_list = []
    for result in results:
        result_obj = Result(**result)
        results_list.append(result_obj)
    pr = PushResults(AQUA_DB)
    print(results_list[:20])
    response, ids = pr.insert_results(results_list)
    return response, ids



@stub.function(
    timeout=600,
)
def delete_results(ids: List[int], AQUA_DB: str):
    pr = PushResults(AQUA_DB)
    pr.delete(ids)
    return 200, "OK"

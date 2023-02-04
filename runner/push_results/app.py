import os
from typing import List, Optional
import modal
from pydantic import BaseModel

from db_connect import get_session
from models import Result, Results, MissingWord, MissingWords

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
        "pandas==1.4.3"
    ),
)


class PushResults:
    def __init__(self):
        self.engine, self.session = next(get_session())

    def __del__(self):
        self.session.close()

    def insert_results(self, results: Results):
        from sqlalchemy.exc import IntegrityError

        self.results = results
        self.create_bulk_results()

        try:
            ids = self.bulk_insert_results(self.assessment_results)
            self.session.commit()
            return 200, ids
        
        except (IntegrityError, AssertionError) as err:
            self.session.rollback()
            return 500, err
    
    def insert_missing_words(self, missing_words: MissingWords):
        from sqlalchemy.exc import IntegrityError

        self.missing_words = missing_words
        self.create_bulk_missing_words()

        try:
            ids = self.bulk_insert_results(self.assessment_missing_words)
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
    
    def create_bulk_missing_words(self):
        from sqlalchemy.orm import declarative_base
        Base = declarative_base()
        from sqlalchemy import Column, Integer, Text, Boolean, Float, ForeignKey, DateTime

        class MissingWord(Base):
            __tablename__ = "assessmentMissingWords"
            id = Column(Integer, primary_key=True)  # autoincrements by default
            assessment = Column(Integer, nullable=False)
            vref = Column(Text)
            source = Column(Text)
            score = Column(Float)
            flag = Column(Boolean, default=False)
            note = Column(Text)

            def __repr__(self):
                return (
                    f"Assessment Result({self.id}) -> {self.assessment}/{self.vref}\n"
                    f"score={self.score} flag={self.flag}, note={self.note}"
                )

        self.assessment_missing_words = []
        for missing_word in self.missing_words.missing_words:
            mw = MissingWord(
                assessment=missing_word.assessment_id,
                vref=missing_word.vref,
                source=missing_word.source,
                score=missing_word.score,
                flag=False,
            )
            self.assessment_missing_words.append(mw)

    def bulk_insert_results(self, results):
        self.session.bulk_save_objects(results, return_defaults=True)
        self.session.flush()
        ids = [ar.id for ar in results]

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
    response, ids = pr.insert_results(results_obj)
    return response, ids


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def push_missing_words(missing_words: List):
    missing_words_obj = []
    for missing_word in missing_words:
        missing_word_obj = MissingWord(**missing_word)
        missing_words_obj.append(missing_word_obj)
    missing_words_obj = MissingWords(missing_words=missing_words_obj)
    pr = PushResults()
    response, ids = pr.insert_missing_words(missing_words_obj)
    return response, ids


@stub.function(
    timeout=600,
    secret=modal.Secret.from_name("aqua-db"),
)
def delete_results(ids: List[int]):
    pr = PushResults()
    pr.delete(ids)
    return 200, "OK"

from sqlalchemy import Column, Integer, Text, Boolean, Float, ForeignKey, DateTime, Enum
from sqlalchemy.orm import declarative_base
from pydantic import BaseModel
from typing import List, Optional


Base = declarative_base()


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
            f"Assessment Result({self.id}) -> {self.assessment}/{self.ref}\n"
            f"score={self.score} flag={self.flag}, note={self.note}"
        )


class Assessment(Base):

    __tablename__ = "assessment"
    id = Column(Integer, primary_key=True)  # autoincrements by default
    revision = Column(Integer)
    reference = Column(Integer, ForeignKey("bibleRevision.id"))
    type = Column(Text)
    finished = Column(Boolean)
    time_finished = Column(DateTime)

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


# Results model to record in the DB.
class Result(BaseModel):
    assessment_id: int
    vref: str
    score: float
    flag: bool = False
    note: Optional[str] = None


# Results is a list of results to push to the DB
class Results(BaseModel):
    results: List[Result]

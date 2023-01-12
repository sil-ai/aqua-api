from sqlalchemy import Column, Integer, Text, Boolean,\
                       Float, ForeignKey, DateTime, Enum
from sqlalchemy.orm import declarative_base
from pydantic import BaseModel
from typing import List, Optional


Base = declarative_base()

class AssessmentResult(Base):

    __tablename__ = "assessmentResult"
    id = Column(Integer, primary_key=True)#autoincrements by default
    assessment = Column(Integer, ForeignKey('assessment.id'),
                 nullable=False)
    # vref = Column(Text, ForeignKey('verseReference.fullVerseId')) #vref format 'Gen 1:1'
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
    id = Column(Integer, primary_key=True)#autoincrements by default
    revision = Column(Integer, ForeignKey('verseText.bibleRevision'))
    reference = Column(Integer, ForeignKey('verseText.bibleRevision'))
    type = Column(Text)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    status = Column(Enum, ForeignKey('assessment_status.status'))

    def __repr__(self):
        return (
            f"Assessment({self.id}) - {self.type} "
            f"revision={self.revision} reference={self.reference}, finished={self.finished}"
        )


# Results model to record in the DB.
class Result(BaseModel):
    assessment_id: int
    vref: str
    score: float
    flag: bool
    note: Optional[str] = None


# Results is a list of results to push to the DB
class Results(BaseModel):
    results: List[Result]
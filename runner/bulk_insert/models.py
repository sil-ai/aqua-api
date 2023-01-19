from sqlalchemy import Column, Integer, Text, Boolean,\
                       Float, ForeignKey, DateTime, Enum
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class AssessmentResult(Base):

    __tablename__ = "assessmentResult"
    id = Column(Integer, primary_key=True)#autoincrements by default
    assessment = Column(Integer, ForeignKey('assessment.id'),
                 nullable=False)
    ref = Column(Text) #vref format 'Gen 1:1'
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

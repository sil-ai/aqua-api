from sqlalchemy import Column, Integer, Text, Boolean, Float, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class AssessmentResult(Base):

    __tablename__ = "assessmentResult"
    id = Column(Integer, primary_key=True)
    #TODO: fix foreign key
    assessment = Column(Integer)#, ForeignKey('assessment_table.id'), nullable=False)
    #TODO: add ref to AssessmentResult table
    ref = Column(Text)
    score = Column(Float)
    flag = Column(Boolean, default=False)
    note = Column(Text, default='')

    def __repr__(self):
        return (
            f"Assessment Result({self.id}) -> {self.ref}/{self.assess}\n"
            f"score={self.score} flag={self.flag}, note={self.note}"
        )

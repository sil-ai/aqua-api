import os
from sqlalchemy import Column, Integer, String, Numeric, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()


class AssessmentResult(Base):
    __tablename__ = "assessmentResult"
    id = Column(Integer, primary_key=True)
    assessment = Column(Integer, nullable=False)
    score = Column(Numeric, nullable=True)
    flag = Column(Boolean, default=False)
    note = Column(String(1024, "utf8_unicode_ci"), nullable=True)


def get_session():
    # ??? Should this yield only session?
    # Need engine in testing but maybe I can pull out of session?
    try:
        engine = create_engine(os.environ["AQUA_DB"], pool_size=5, pool_recycle=3600)
    except KeyError as err:
        raise KeyError(f"Missing environmental variable {err}") from err
    with Session(engine) as session:
        yield engine, session

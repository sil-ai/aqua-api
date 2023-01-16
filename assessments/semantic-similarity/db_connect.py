import os
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()

class VerseText(Base):
    __tablename__ = "verseText"
    #TODO: check what the original field lengths are
    id = Column(Integer, primary_key=True)
    #transfers all but one Apocryphal verse ESG 10:3
    text = Column(
        String(550, "utf8_unicode_ci"), nullable=False)
    bibleRevision = Column(Integer, nullable=False, index=True)
    #transfers across all reference strings
    verseReference = Column(String(15, "utf8_unicode_ci"),nullable=False,index=True)

def get_session():
    #??? Should this yield only session?
    #Need engine in testing but maybe I can pull out of session?
    try:
        engine = create_engine(os.environ['AQUA_DB'], pool_size=5, pool_recycle=3600)
    except KeyError as err:
        raise KeyError(f'Missing environmental variable {err}') from err
    with Session(engine) as session:
        yield engine,session
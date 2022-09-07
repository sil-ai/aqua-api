import os
from dotenv import load_dotenv
load_dotenv()
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
    engine = create_engine(os.environ['aqua_connection_string'], pool_size=5, pool_recycle=3600)
    with Session(engine) as session:
        yield engine,session

if __name__ == '__main__':
    engine,session = next(get_session())
    session.query(VerseText)
import enum
from sqlalchemy import Column, Integer, Text,\
                       ForeignKey, DateTime, Enum, VARCHAR
from sqlalchemy.orm import declarative_base
from datetime import datetime, timezone

Base = declarative_base()

class VerseText(Base):

    __tablename__ = "verseText"
    id = Column(Integer, primary_key=True)
    text = Column(Text)
    bibleRevision = Column(Integer)
    verseReference = Column(Text)

    def __repr__(self):
        return (
            f"VerseText({self.id} - Revision {self.bibleRevision})\n"
            f"{self.text} - {self.verseReference}"
        )

class StatusEnum(enum.Enum):
    #TODO: maybe change this
    RUNNING = 'running'
    QUEUED = 'queued'
    FAILED = 'failed'
    COMPLETE = 'complete'

class Assessment(Base):

    __tablename__ = "assessment"
    id = Column(Integer, primary_key=True)#autoincrements by default
    revision = Column(Integer, ForeignKey(VerseText.bibleRevision))
    reference = Column(Integer, ForeignKey(VerseText.bibleRevision))
    type = Column(Text)
    start_time = Column(DateTime, default= datetime.now(timezone.utc))
    end_time = Column(DateTime)
    status = Column(Enum(StatusEnum, values_callable=lambda obj: [e.value for e in obj]),
                   nullable=False,
                   default = StatusEnum.RUNNING.value,
                   server_default = StatusEnum.RUNNING.value
                   )
    job_id = Column(VARCHAR, nullable=False)

    def __repr__(self):
        return (
            f"Assessment({self.id}) - {self.type} "
            f"revision={self.revision} reference={self.reference}, status={self.status}"
        )
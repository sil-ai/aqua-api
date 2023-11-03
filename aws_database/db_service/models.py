from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Numeric, DateTime, Text, Date, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP, JSONB

Base = declarative_base()

class AssessmentResult(Base):
    __tablename__ = 'assessmentResult'
    
    id = Column(Integer, primary_key=True, index=True)
    assessment_id = Column(Integer, ForeignKey('assessment.id'), nullable=False)
    score = Column(Numeric)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text, ForeignKey('verseReference.fullverseid'))
    source = Column(Text)
    target = Column(JSONB)
    hide = Column(Boolean)
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)

    assessment = relationship("Assessment", back_populates="assessment_results")

class Assessment(Base):
    __tablename__ = 'assessment'
    
    id = Column(Integer, primary_key=True, index=True)
    revision = Column(Integer, ForeignKey('assessment.revision'))
    reference = Column(Integer, ForeignKey('assessment.reference'))
    type = Column(Text)
    status = Column(Text)
    requested_time = Column(TIMESTAMP)
    start_time = Column(TIMESTAMP(timezone=True))
    end_time = Column(TIMESTAMP(timezone=True))

    assessment_results = relationship("AssessmentResult", back_populates="assessment")

class BibleRevision(Base):
    __tablename__ = 'bibleRevision'
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date)
    bibleversion_id = Column(Integer, ForeignKey('bibleVersion.id'), nullable=False)
    published = Column(Boolean)
    name = Column(Text)
    backtranslation_id = Column(Integer, ForeignKey('bibleVersion.id'))
    machinetranslation = Column(Boolean)

class BibleVersion(Base):
    __tablename__ = 'bibleVersion'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text)
    isolanguage_id = Column(Text, ForeignKey('isoLanguage.iso639'))
    isoscript_id = Column(Text, ForeignKey('isoScript.iso15924'))
    abbreviation = Column(Text)
    rights = Column(Text)
    forwardtranslation = Column(Integer)
    backtranslation = Column(Integer)
    machinetranslation = Column(Boolean)

class BookReference(Base):
    __tablename__ = 'bookReference'
    
    abbreviation = Column(Text, primary_key=True, index=True)
    name = Column(Text)
    number = Column(Integer)

class ChapterReference(Base):
    __tablename__ = 'chapterReference'
    
    fullchapterid = Column(Text, primary_key=True, index=True)
    number = Column(Integer)
    bookreference = Column(Text, ForeignKey('bookReference.abbreviation'), nullable=False)
class IsoLanguage(Base):
    __tablename__ = 'isoLanguage'
    
    id = Column(Integer, primary_key=True, index=True)
    iso639 = Column(Text, unique=True)
    name = Column(Text)

class IsoScript(Base):
    __tablename__ = 'isoScript'
    
    id = Column(Integer, primary_key=True, index=True)
    iso15924 = Column(Text, unique=True)
    name = Column(Text)



class VerseReference(Base):
    __tablename__ = 'verseReference'
    
    fullverseid = Column(Text, primary_key=True, index=True)
    number = Column(Integer)
    chapter = Column(Text, ForeignKey('chapterReference.fullchapterid'))
    bookreference = Column(Text, ForeignKey('bookReference.abbreviation'))

class VerseText(Base):
    __tablename__ = 'verseText'
    
    id = Column(Integer, primary_key=True, index=True)
    text = Column(Text)
    biblerevision_id = Column(Integer, ForeignKey('bibleRevision.id'))
    versereference = Column(Text, ForeignKey('verseReference.fullverseid'))
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)

class AlignmentThresholdScores(Base):
    __tablename__ = 'alignmentThresholdScores'
    
    id = Column(Integer, primary_key=True, index=True)
    assessment_id = Column(Integer, ForeignKey('assessment.id'), nullable=False)
    score = Column(DOUBLE_PRECISION)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)

class AlignmentTopSourceScores(Base):
    __tablename__ = 'alignmentTopSourceScores'
    
    id = Column(Integer, primary_key=True, index=True)
    assessment_id = Column(Integer, ForeignKey('assessment.id'), nullable=False)
    score = Column(DOUBLE_PRECISION)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(Text)


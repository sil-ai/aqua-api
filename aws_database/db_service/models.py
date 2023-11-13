from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Text, DateTime, Numeric, create_engine, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import func

Base = declarative_base()

class AlignmentThresholdScores(Base):
    __tablename__ = 'alignmentThresholdScores'
    
    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey('assessments.id'))
    score = Column(Numeric)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)

class AlignmentTopSourceScores(Base):
    __tablename__ = 'alignmentTopSourceScores'
    
    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey('assessments.id'))
    score = Column(Numeric)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)

class Assessment(Base):
    __tablename__ = 'assessments'
    
    id = Column(Integer, primary_key=True)
    revision_id = Column(Integer, ForeignKey('bibleRevisions.id'))
    reference_id = Column(Integer, ForeignKey('bibleRevisions.id'))
    type = Column(Text)
    status = Column(Text)
    requested_time = Column(TIMESTAMP, default=func.now())
    start_time = Column(TIMESTAMP(timezone=True))
    end_time = Column(TIMESTAMP(timezone=True))

class AssessmentResult(Base):
    __tablename__ = 'assessmentResults'
    
    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey('assessments.id'))
    score = Column(Numeric)
    flag = Column(Boolean, default=False)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(JSONB)
    hide = Column(Boolean, default=False)
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)
class BibleRevision(Base):
    __tablename__ = 'bibleRevisions'

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    bible_version_id = Column(Integer, ForeignKey('bibleVersions.id'))
    published = Column(Boolean)
    name = Column(Text)
    back_translation_id = Column(Integer, ForeignKey('bibleRevisions.id'))
    machine_translation = Column(Boolean, default=False)

class BibleVersion(Base):
    __tablename__ = 'bibleVersions'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    iso_language = Column(String(3), ForeignKey('isoLanguages.iso639'))
    iso_script = Column(String(4), ForeignKey('isoScripts.iso15924'))
    abbreviation = Column(Text)
    rights = Column(Text)
    forward_translation_id = Column(Integer)
    back_translation_id = Column(Integer)
    machine_translation = Column(Boolean)

class BookReference(Base):
    __tablename__ = 'bookReferences'

    abbreviation = Column(Text, primary_key=True)
    name = Column(Text)
    number = Column(Integer)

class ChapterReference(Base):
    __tablename__ = 'chapterReferences'

    full_chapter_id = Column(Text, primary_key=True)
    number = Column(Integer)
    book_reference = Column(Text, ForeignKey('bookReferences.abbreviation'))

class IsoLanguage(Base):
    __tablename__ = 'isoLanguages'
    
    iso639 = Column(String(3), primary_key=True, unique=True)
    name = Column(Text)

class IsoScript(Base):
    __tablename__ = 'isoScripts'
    
    iso15924 = Column(String(4), primary_key=True, unique=True)
    name = Column(Text)

class VerseReference(Base):
    __tablename__ = 'verseReferences'

    full_verse_id = Column(Text, primary_key=True, unique=True)
    number = Column(Integer)
    chapter = Column(Text, ForeignKey('chapterReferences.full_chapter_id'))
    book_reference = Column(Text, ForeignKey('bookReferences.abbreviation'))

class VerseText(Base):
    __tablename__ = 'verseTexts'

    id = Column(Integer, primary_key=True)
    text = Column(Text)
    bible_revision_id = Column(Integer, ForeignKey('bibleRevisions.id'))
    verse_reference = Column(Text, ForeignKey('verseReferences.full_verse_id'))
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)


    # Relationship definitions (if needed)
    bible_revision = relationship("BibleRevision", backref=backref("verse_texts", cascade="all, delete-orphan"))
    verse_reference = relationship("VerseReference", backref="verse_texts")
    

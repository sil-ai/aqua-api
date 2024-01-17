from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    ForeignKey,
    Text,
    DateTime,
    Numeric,
    TIMESTAMP,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import func

Base = declarative_base()


class AlignmentThresholdScores(Base):
    __tablename__ = "alignment_threshold_scores"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    score = Column(Numeric)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)


class alignment_top_source_scores(Base):
    __tablename__ = "alignment_top_source_scores"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    score = Column(Numeric)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)


class Assessment(Base):
    __tablename__ = "assessment"

    id = Column(Integer, primary_key=True)
    revision_id = Column(Integer, ForeignKey("bible_revision.id"))
    reference_id = Column(Integer, ForeignKey("bible_revision.id"))
    type = Column(Text)
    status = Column(Text)
    requested_time = Column(TIMESTAMP, default=func.now())
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    assessment_version = Column(String, default="1")
    deleted = Column(Boolean, default=False)
    deletedAt = Column(TIMESTAMP, default=None)

    group_id = Column(Integer, ForeignKey('groups.id'))
    group = relationship("Group")


class assessment_result(Base):
    __tablename__ = "assessment_result"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
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

    group_id = Column(Integer, ForeignKey('groups.id'))
    group = relationship("Group")



class BibleRevision(Base):
    __tablename__ = "bible_revision"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    bible_version_id = Column(Integer, ForeignKey("bible_version.id"))
    published = Column(Boolean)
    name = Column(Text)
    back_translation_id = Column(Integer, ForeignKey("bible_version.id"))
    machine_translation = Column(Boolean, default=False)
    deleted = Column(Boolean, default=False)
    deletedAt = Column(TIMESTAMP, default=None)

    group_id = Column(Integer, ForeignKey('groups.id'))
    group = relationship("Group")


class BibleVersion(Base):
    __tablename__ = "bible_version"

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    iso_language = Column(String(3), ForeignKey("iso_language.iso639"))
    iso_script = Column(String(4), ForeignKey("iso_script.iso15924"))
    abbreviation = Column(Text)
    rights = Column(Text)
    forward_translation_id = Column(Integer)
    back_translation_id = Column(Integer)
    machine_translation = Column(Boolean)
    deleted = Column(Boolean, default=False)
    deletedAt = Column(TIMESTAMP, default=None)

    group_id = Column(Integer, ForeignKey('groups.id'))
    group = relationship("Group")


class book_reference(Base):
    __tablename__ = "book_reference"

    abbreviation = Column(Text, primary_key=True)
    name = Column(Text)
    number = Column(Integer)


class chapter_reference(Base):
    __tablename__ = "chapter_reference"

    full_chapter_id = Column(Text, primary_key=True)
    number = Column(Integer)
    book_reference = Column(Text, ForeignKey("book_reference.abbreviation"))


class IsoLanguage(Base):
    __tablename__ = "iso_language"

    iso639 = Column(String(3), primary_key=True, unique=True)
    name = Column(Text)


class IsoScript(Base):
    __tablename__ = "iso_script"

    iso15924 = Column(String(4), primary_key=True, unique=True)
    name = Column(Text)


class verse_reference(Base):
    __tablename__ = "verse_reference"

    full_verse_id = Column(Text, primary_key=True, unique=True)
    number = Column(Integer)
    chapter = Column(Text, ForeignKey("chapter_reference.full_chapter_id"))
    book_reference = Column(Text, ForeignKey("book_reference.abbreviation"))


class VerseText(Base):
    __tablename__ = "verse_text"

    id = Column(Integer, primary_key=True)
    text = Column(Text)
    bible_revision_id = Column(Integer, ForeignKey("bible_revision.id"))
    verse_reference = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)

    # Relationship definitions (if needed)
    bible_revision = relationship(
        "bible_revision", backref=backref("verse_texts", cascade="all, delete-orphan")
    )
    # verse_reference = relationship("verse_reference", backref="verse_texts")
class UserDB(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(50), unique=True, nullable=False)
    hashed_password = Column(String(100), nullable=False)
    is_admin = Column(Boolean, default=False)
    # Relationship with UserGroups
    groups = relationship("UserGroup", back_populates="user")


class Group(Base):
    __tablename__ = 'groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    # Relationship with UserGroups
    users = relationship("UserGroup", back_populates="group")


class UserGroup(Base):
    __tablename__ = 'user_groups'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    group_id = Column(Integer, ForeignKey('groups.id'), nullable=False)
    # Relationships
    user = relationship("UserDB", back_populates="groups")
    group = relationship("Group", back_populates="users")

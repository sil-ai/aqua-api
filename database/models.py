from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import VECTOR, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class AlignmentThresholdScores(Base):
    __tablename__ = "alignment_threshold_scores"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    score = Column(Numeric)
    flag = Column(Boolean)
    note = Column(Text)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)


class AlignmentTopSourceScores(Base):
    __tablename__ = "alignment_top_source_scores"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), index=True)
    score = Column(Numeric)
    flag = Column(Boolean)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    note = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean)
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)

    book_score_idx = Index("book_score_idx", book, score)


class NgramsTable(Base):
    __tablename__ = "ngrams_table"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    ngram = Column(Text)
    ngram_size = Column(Integer)

    vrefs = relationship("NgramVrefTable", back_populates="ngram")


class NgramVrefTable(Base):
    __tablename__ = "ngram_vref_table"

    id = Column(Integer, primary_key=True)
    ngram_id = Column(Integer, ForeignKey("ngrams_table.id"))
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))

    ngram = relationship("NgramsTable", back_populates="vrefs")


class TfidfPcaVector(Base):
    __tablename__ = "tfidf_pca_vector"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), index=True)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"), index=True)
    vector = Column(VECTOR(300))  # Dense vector of fixed length


class TextProportionsTable(Base):
    __tablename__ = "text_proportions_table"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id")), index=True
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    word_proportions = Column(Numeric)
    char_proportions = Column(Numeric)
    word_proportions_z = Column(Numeric)
    char_proportions_z = Column(Numeric)


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
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=True, default=None)

    results = relationship(
        "AssessmentResult", cascade="all, delete", back_populates="assessment"
    )
    revision = relationship(
        "BibleRevision",
        foreign_keys=[revision_id],
        back_populates="assessments_as_revision",
    )
    reference = relationship(
        "BibleRevision",
        foreign_keys=[reference_id],
        back_populates="assessments_as_reference",
    )


class AssessmentResult(Base):
    __tablename__ = "assessment_result"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    score = Column(Numeric)
    flag = Column(Boolean, default=False)
    note = Column(Text)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    source = Column(Text)
    target = Column(JSONB)
    hide = Column(Boolean, default=False)
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)

    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    assessment = relationship("Assessment", back_populates="results")

    __table_args__ = (
        Index(
            "idx_assessment_result_main",
            "assessment_id",
            "book",
            "chapter",
            "verse",
            "id",
        ),
        Index("idx_assessment_id", "assessment_id"),
        Index("idx_book_chapter_verse", "book", "chapter", "verse"),
    )


class BibleRevision(Base):
    __tablename__ = "bible_revision"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    bible_version_id = Column(Integer, ForeignKey("bible_version.id"))
    published = Column(Boolean)
    name = Column(Text)
    back_translation_id = Column(
        Integer, ForeignKey("bible_revision.id"), nullable=True, default=None
    )
    machine_translation = Column(Boolean, default=False)
    deleted = Column(Boolean, default=False)
    deletedAt = Column(TIMESTAMP, default=None)

    back_translation = relationship("BibleRevision", remote_side=[id])
    bible_version = relationship("BibleVersion", back_populates="revisions")
    verse_text = relationship(
        "VerseText", cascade="all, delete", back_populates="bible_revision"
    )
    assessments_as_revision = relationship(
        "Assessment",
        cascade="all, delete",
        back_populates="revision",
        foreign_keys="[Assessment.revision_id]",
    )
    assessments_as_reference = relationship(
        "Assessment",
        cascade="all, delete",
        back_populates="reference",
        foreign_keys="[Assessment.reference_id]",
    )

    __table_args__ = (
        Index("ix_bible_revision_version_id", "bible_version_id"),
        Index("ix_bible_revision_deleted", "deleted"),
    )


class BibleVersion(Base):
    __tablename__ = "bible_version"

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    iso_language = Column(String(3), ForeignKey("iso_language.iso639"))
    iso_script = Column(String(4), ForeignKey("iso_script.iso15924"))
    abbreviation = Column(Text)
    rights = Column(Text)
    forward_translation_id = Column(Integer, nullable=True, default=None)
    back_translation_id = Column(
        Integer, ForeignKey("bible_version.id"), nullable=True, default=None
    )
    machine_translation = Column(Boolean)
    is_reference = Column(Boolean)
    deleted = Column(Boolean, default=False)
    deletedAt = Column(TIMESTAMP, default=None)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=True, default=None)

    owner = relationship("UserDB", backref=backref("bible_versions"))
    back_translation = relationship("BibleVersion", remote_side=[id])
    accessible_by = relationship(
        "BibleVersionAccess", cascade="all, delete", back_populates="bible_version"
    )
    revisions = relationship(
        "BibleRevision", cascade="all, delete", back_populates="bible_version"
    )

    __table_args__ = (Index("ix_bible_version_deleted", "deleted"),)


class BibleVersionAccess(Base):
    __tablename__ = "bible_version_access"

    id = Column(Integer, primary_key=True)
    bible_version_id = Column(Integer, ForeignKey("bible_version.id"), nullable=False)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False)

    # Relationships
    bible_version = relationship("BibleVersion", back_populates="accessible_by")
    group = relationship("Group", back_populates="bible_versions_access")

    __table_args__ = (
        Index("ix_bible_version_access_group", "group_id"),
        Index("ix_bible_version_access_version", "bible_version_id"),
        Index("ix_bible_version_access_version_group", "bible_version_id", "group_id"),
    )


class BookReference(Base):
    __tablename__ = "book_reference"

    abbreviation = Column(Text, primary_key=True)
    name = Column(Text)
    number = Column(Integer)


class ChapterReference(Base):
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


class VerseReference(Base):
    __tablename__ = "verse_reference"

    full_verse_id = Column(Text, primary_key=True, unique=True)
    number = Column(Integer)
    chapter = Column(Text, ForeignKey("chapter_reference.full_chapter_id"))
    book_reference = Column(Text, ForeignKey("book_reference.abbreviation"))


class VerseText(Base):
    __tablename__ = "verse_text"

    id = Column(Integer, primary_key=True)
    text = Column(Text, nullable=True)
    revision_id = Column(Integer, ForeignKey("bible_revision.id"))
    verse_reference = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)

    bible_revision = relationship(
        "BibleRevision", back_populates="verse_text", cascade="all, delete"
    )


class UserDB(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(50), unique=False, nullable=True, default=None)
    hashed_password = Column(String(100), nullable=False)
    is_admin = Column(Boolean, default=False)
    # Relationship with UserGroups
    groups = relationship("UserGroup", back_populates="user", cascade="all, delete")
    owner_of = relationship("BibleVersion", back_populates="owner")


class Group(Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    # Relationship with UserGroups
    users = relationship("UserGroup", back_populates="group")
    bible_versions_access = relationship(
        "BibleVersionAccess", back_populates="group", cascade="all, delete"
    )


class UserGroup(Base):
    __tablename__ = "user_groups"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False)

    __table_args__ = (Index("ix_user_group_user_id", "user_id"),)

    # Relationships
    user = relationship("UserDB", back_populates="groups")
    group = relationship("Group", back_populates="users")

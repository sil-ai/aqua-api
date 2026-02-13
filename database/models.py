from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
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

    __table_args__ = (
        Index("ix_alignment_scores_assessment_score", "assessment_id", "score"),
        Index("ix_alignment_scores_grouping", "book", "chapter", "verse", "source"),
    )


class NgramsTable(Base):
    __tablename__ = "ngrams_table"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), index=True)
    ngram = Column(Text)
    ngram_size = Column(Integer)

    vrefs = relationship("NgramVrefTable", back_populates="ngram")


class NgramVrefTable(Base):
    __tablename__ = "ngram_vref_table"

    id = Column(Integer, primary_key=True)
    ngram_id = Column(Integer, ForeignKey("ngrams_table.id"), index=True)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))

    ngram = relationship("NgramsTable", back_populates="vrefs")


class TfidfPcaVector(Base):
    __tablename__ = "tfidf_pca_vector"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), index=True)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"), index=True)
    vector = Column(Vector(300))  # Dense vector of fixed length

    __table_args__ = (
        Index(
            "tfidf_pca_vector_ivfflat_idx",
            "vector",
            postgresql_using="ivfflat",
            postgresql_ops={"vector": "vector_ip_ops"},
            postgresql_with={"lists": "100"},
        ),
    )


class TextLengthsTable(Base):
    __tablename__ = "text_lengths_table"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), index=True)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"), index=True)
    word_lengths = Column(Numeric)
    char_lengths = Column(Numeric)
    word_lengths_z = Column(Numeric)
    char_lengths_z = Column(Numeric)


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

    __table_args__ = (
        Index(
            "ix_assessment_rev_ref_type_status_end",
            "revision_id",
            "reference_id",
            "type",
            "status",
            "end_time",
        ),
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

    __table_args__ = (
        Index("ix_verse_text_revision_id", "revision_id"),
        Index("ix_verse_text_revision_book", "revision_id", "book"),
        Index(
            "ix_verse_text_verse_reference_revision", "verse_reference", "revision_id"
        ),
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


class AgentLexemeCard(Base):
    __tablename__ = "agent_lexeme_cards"

    id = Column(Integer, primary_key=True)
    source_lemma = Column(Text)  # Source language lemma for cross-reference
    target_lemma = Column(Text, nullable=False)
    source_language = Column(String(3), ForeignKey("iso_language.iso639"))
    target_language = Column(String(3), ForeignKey("iso_language.iso639"))
    pos = Column(Text)
    surface_forms = Column(JSONB)  # JSON array of target language surface forms
    source_surface_forms = Column(JSONB)  # JSON array of source language surface forms
    senses = Column(JSONB)  # JSON array of senses
    # Note: examples are now stored in the agent_lexeme_card_examples table (see examples_rel relationship)
    confidence = Column(Numeric)
    english_lemma = Column(Text)  # English lemma when source/target are not English
    alignment_scores = Column(JSONB)  # Dict: {source_word: alignment_score}
    created_at = Column(TIMESTAMP, default=func.now())
    last_updated = Column(TIMESTAMP, default=func.now())
    last_user_edit = Column(TIMESTAMP, nullable=True)

    __table_args__ = (
        # Unique constraint to prevent duplicate cards
        # Each target_lemma can only have one card per language pair
        Index(
            "ix_agent_lexeme_cards_unique_v2",
            "target_lemma",
            "source_language",
            "target_language",
            unique=True,
        ),
        # Index for common query pattern: language pair + confidence ordering
        Index(
            "ix_agent_lexeme_cards_lang_confidence",
            "source_language",
            "target_language",
            "confidence",
            postgresql_ops={"confidence": "DESC"},
        ),
        # Functional index for case-insensitive target_lemma searches
        Index(
            "ix_agent_lexeme_cards_target_lemma_lower",
            func.lower(target_lemma),
            postgresql_using="btree",
        ),
        # Functional index for case-insensitive source_lemma searches
        Index(
            "ix_agent_lexeme_cards_source_lemma_lower",
            func.lower(source_lemma),
            postgresql_using="btree",
        ),
        # GIN index for JSONB array searches in surface_forms
        Index(
            "ix_agent_lexeme_cards_surface_forms",
            "surface_forms",
            postgresql_using="gin",
        ),
        # GIN index for JSONB array searches in source_surface_forms
        Index(
            "ix_agent_lexeme_cards_source_surface_forms",
            "source_surface_forms",
            postgresql_using="gin",
        ),
    )

    # Relationship to examples
    examples_rel = relationship(
        "AgentLexemeCardExample",
        back_populates="lexeme_card",
        cascade="all, delete-orphan",
    )


class AgentLexemeCardExample(Base):
    __tablename__ = "agent_lexeme_card_examples"

    id = Column(Integer, primary_key=True)
    lexeme_card_id = Column(
        Integer, ForeignKey("agent_lexeme_cards.id", ondelete="CASCADE"), nullable=False
    )
    revision_id = Column(
        Integer, ForeignKey("bible_revision.id", ondelete="CASCADE"), nullable=False
    )
    source_text = Column(Text, nullable=False)
    target_text = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, default=func.now())

    __table_args__ = (
        # Prevent duplicate examples for the same lexeme card + revision
        Index(
            "ix_agent_lexeme_card_examples_unique",
            "lexeme_card_id",
            "revision_id",
            "source_text",
            "target_text",
            unique=True,
        ),
        # Index for querying examples by revision
        Index(
            "ix_agent_lexeme_card_examples_revision",
            "revision_id",
        ),
    )

    # Relationships
    lexeme_card = relationship("AgentLexemeCard", back_populates="examples_rel")
    revision = relationship("BibleRevision")


class AgentWordAlignment(Base):
    __tablename__ = "agent_word_alignments"

    id = Column(Integer, primary_key=True)
    source_word = Column(Text, nullable=False)
    target_word = Column(Text, nullable=False)
    source_language = Column(String(3), ForeignKey("iso_language.iso639"))
    target_language = Column(String(3), ForeignKey("iso_language.iso639"))
    score = Column(Float, nullable=False, default=0.0)
    is_human_verified = Column(Boolean, default=False)  # False until human-verified
    created_at = Column(TIMESTAMP, default=func.now())
    last_updated = Column(TIMESTAMP, default=func.now(), onupdate=func.now())

    __table_args__ = (
        # Unique constraint for atomic upserts
        Index(
            "ux_agent_word_alignments_lang_words",
            "source_language",
            "target_language",
            "source_word",
            "target_word",
            unique=True,
        ),
        # Index for source word lookups by language pair
        Index(
            "ix_agent_word_alignments_lang_source",
            "source_language",
            "target_language",
            "source_word",
        ),
        # Index for target word lookups by language pair
        Index(
            "ix_agent_word_alignments_lang_target",
            "source_language",
            "target_language",
            "target_word",
        ),
        # Index for efficient score-ordered queries
        Index(
            "ix_agent_word_alignments_lang_score",
            "source_language",
            "target_language",
            score.desc(),
        ),
    )


class AgentCritiqueIssue(Base):
    __tablename__ = "agent_critique_issue"

    id = Column(Integer, primary_key=True)

    # Assessment metadata
    assessment_id = Column(Integer, ForeignKey("assessment.id"), nullable=False)

    # Verse information (parsed from vref)
    vref = Column(String(20), nullable=False)
    book = Column(String(10), nullable=False)
    chapter = Column(Integer, nullable=False)
    verse = Column(Integer, nullable=False)

    # Issue classification
    issue_type = Column(
        String(15), nullable=False
    )  # 'omission', 'addition', or 'replacement'

    # Issue details
    source_text = Column(Text, nullable=True)  # The source text (omission/replacement)
    draft_text = Column(Text, nullable=True)  # The draft text (addition/replacement)
    comments = Column(Text, nullable=True)  # Explanation of why this is an issue
    severity = Column(Integer, nullable=False)  # 0=none, 5=critical

    # Resolution tracking
    is_resolved = Column(Boolean, default=False, nullable=False)
    resolved_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    resolved_at = Column(TIMESTAMP, nullable=True)
    resolution_notes = Column(Text, nullable=True)

    # Timestamp
    created_at = Column(TIMESTAMP, default=func.now())

    # Link to the specific translation that was critiqued
    agent_translation_id = Column(
        Integer,
        ForeignKey("agent_translations.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Relationships
    assessment = relationship("Assessment")
    resolved_by = relationship("UserDB", foreign_keys=[resolved_by_id])
    translation = relationship("AgentTranslation", back_populates="critique_issues")

    __table_args__ = (
        Index("ix_agent_critique_issue_assessment", "assessment_id"),
        Index("ix_agent_critique_issue_vref", "vref"),
        Index("ix_agent_critique_issue_book_chapter_verse", "book", "chapter", "verse"),
        Index("ix_agent_critique_issue_type", "issue_type"),
        Index("ix_agent_critique_issue_severity", "severity"),
        Index("ix_agent_critique_issue_resolved", "is_resolved"),
        Index("ix_agent_critique_issue_resolved_by", "resolved_by_id"),
        Index("ix_agent_critique_issue_translation", "agent_translation_id"),
        # The both-NULL clause permits legacy rows that existed before this
        # migration with no text fields set.  New rows always satisfy the
        # type-specific requirements via Pydantic validation.
        CheckConstraint(
            """
            (issue_type = 'omission'    AND source_text IS NOT NULL AND draft_text IS NULL) OR
            (issue_type = 'addition'    AND source_text IS NULL     AND draft_text IS NOT NULL) OR
            (issue_type = 'replacement' AND source_text IS NOT NULL AND draft_text IS NOT NULL) OR
            (source_text IS NULL AND draft_text IS NULL)
            """,
            name="ck_critique_issue_text_fields",
        ),
    )


class AgentTranslation(Base):
    __tablename__ = "agent_translations"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(
        Integer, ForeignKey("assessment.id", ondelete="CASCADE"), nullable=False
    )
    vref = Column(String(20), nullable=False)
    version = Column(Integer, default=1, nullable=False)
    draft_text = Column(Text, nullable=True)
    hyper_literal_translation = Column(Text, nullable=True)
    literal_translation = Column(Text, nullable=True)
    english_translation = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, default=func.now())

    assessment = relationship("Assessment")
    critique_issues = relationship("AgentCritiqueIssue", back_populates="translation")

    __table_args__ = (
        Index(
            "ix_agent_translations_unique",
            "assessment_id",
            "vref",
            "version",
            unique=True,
        ),
        Index("ix_agent_translations_assessment_vref", "assessment_id", "vref"),
    )

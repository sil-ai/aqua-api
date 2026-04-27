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
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class AlignmentThresholdScores(Base):
    __tablename__ = "alignment_threshold_scores"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"))
    score = Column(Numeric)
    flag = Column(Boolean, default=False, server_default="false")
    note = Column(Text)
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean, default=False, server_default="false")
    book = Column(Text)
    chapter = Column(Integer)
    verse = Column(Integer)


class AlignmentTopSourceScores(Base):
    __tablename__ = "alignment_top_source_scores"

    id = Column(Integer, primary_key=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), index=True)
    score = Column(Numeric)
    flag = Column(Boolean, default=False, server_default="false")
    vref = Column(Text, ForeignKey("verse_reference.full_verse_id"))
    note = Column(Text)
    source = Column(Text)
    target = Column(Text)
    hide = Column(Boolean, default=False, server_default="false")
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
    status_detail = Column(Text, nullable=True)
    percent_complete = Column(Float, nullable=True)
    is_training = Column(Boolean, nullable=False, default=False, server_default="false")
    requested_time = Column(TIMESTAMP, default=func.now())
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    assessment_version = Column(String, default="1")
    deleted = Column(Boolean, default=False)
    deletedAt = Column(TIMESTAMP, default=None)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=True, default=None)
    kwargs = Column(JSONB, nullable=True, default=None)

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


class TrainingJob(Base):
    __tablename__ = "training_job"

    id = Column(Integer, primary_key=True)
    type = Column(Text, nullable=False)
    source_revision_id = Column(
        Integer, ForeignKey("bible_revision.id"), nullable=False
    )
    target_revision_id = Column(
        Integer, ForeignKey("bible_revision.id"), nullable=False
    )
    source_language = Column(
        String(3), ForeignKey("iso_language.iso639"), nullable=False
    )
    target_language = Column(
        String(3), ForeignKey("iso_language.iso639"), nullable=False
    )

    status = Column(Text, nullable=False, default="queued")
    status_detail = Column(Text, nullable=True)
    percent_complete = Column(Float, nullable=True)

    external_ids = Column(JSONB, nullable=True)
    result_url = Column(Text, nullable=True)
    result_metadata = Column(JSONB, nullable=True)
    options = Column(JSONB, nullable=True)

    requested_time = Column(TIMESTAMP, default=func.now())
    start_time = Column(TIMESTAMP, nullable=True)
    end_time = Column(TIMESTAMP, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    session_id = Column(Text, nullable=True)

    assessment_id = Column(
        Integer,
        ForeignKey("assessment.id", ondelete="SET NULL"),
        nullable=True,
    )

    deleted = Column(Boolean, default=False)
    deleted_at = Column(TIMESTAMP, nullable=True)

    source_revision = relationship("BibleRevision", foreign_keys=[source_revision_id])
    target_revision = relationship("BibleRevision", foreign_keys=[target_revision_id])
    owner = relationship("UserDB")
    assessment = relationship("Assessment", foreign_keys=[assessment_id])

    __table_args__ = (
        Index("ix_training_job_status", "status"),
        Index("ix_training_job_type_status", "type", "status"),
        Index("ix_training_job_lang_pair", "source_language", "target_language"),
        Index(
            "ix_training_job_revisions_type_status",
            "source_revision_id",
            "target_revision_id",
            "type",
            "status",
        ),
        Index("ix_training_job_session_id", "session_id"),
        Index("ix_training_job_assessment_id", "assessment_id"),
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
        # Case-insensitive unique constraint to prevent duplicate cards
        # Each LOWER(target_lemma) can only have one card per language pair
        Index(
            "ix_agent_lexeme_cards_unique_v3",
            func.lower(target_lemma),
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
        # Lightweight index for batch-loading examples by card + revision
        # (the unique index includes text columns, making it 20x larger)
        Index(
            "ix_agent_lexeme_card_examples_card_revision",
            "lexeme_card_id",
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
    # revision being translated (assessment.revision_id, i.e. the target text)
    revision_id = Column(Integer, ForeignKey("bible_revision.id"), nullable=False)
    # reference language/script (from assessment's reference BibleVersion)
    language = Column(String(3), ForeignKey("iso_language.iso639"), nullable=False)
    script = Column(String(4), ForeignKey("iso_script.iso15924"), nullable=False)
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
            "revision_id",
            "language",
            "script",
            "vref",
            "version",
            unique=True,
        ),
        Index("ix_agent_translations_assessment_vref", "assessment_id", "vref"),
        Index(
            "ix_agent_translations_rev_lang_script_vref",
            "revision_id",
            "language",
            "script",
            "vref",
        ),
    )


class EflomalAssessment(Base):
    """One row per eflomal training run (one per assessment).

    Stores metadata about the training run. Can query by source/target language
    or assessment_id.

    """

    __tablename__ = "eflomal_assessment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer, ForeignKey("assessment.id"), nullable=False, unique=True
    )
    source_language = Column(String(3), nullable=True)
    target_language = Column(String(3), nullable=True)
    num_verse_pairs = Column(Integer)
    num_alignment_links = Column(Integer)
    num_dictionary_entries = Column(Integer)
    num_missing_words = Column(Integer)
    created_at = Column(TIMESTAMP, default=func.now())

    __table_args__ = (
        Index(
            "ix_eflomal_assessment_language_pair",
            "source_language",
            "target_language",
        ),
    )


class EflomalDictionary(Base):
    """Statistical dictionary of word-pair alignments learned during training.

    Each row is a unique (source_word, target_word) pair aggregated across all
    verses in the Bible corpus.  Words are stored in their original
    (un-normalized) form.

    - count: how many times this pair was aligned across all verses.
    - probability: averaged per-word posterior probability (acc_ps) from
      eflomal, representing the model's confidence in this alignment.

    Used at inference time for greedy matching and link scoring.
    """

    __tablename__ = "eflomal_dictionary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer, ForeignKey("eflomal_assessment.id", ondelete="CASCADE"), nullable=False
    )
    source_word = Column(String, nullable=False)
    target_word = Column(String, nullable=False)
    count = Column(Integer, nullable=False)
    probability = Column(Float, nullable=False)

    __table_args__ = (
        # Unique constraint on the full pair
        Index(
            "ux_eflomal_dictionary_assessment_source_target",
            "assessment_id",
            "source_word",
            "target_word",
            unique=True,
        ),
        # Index for source word lookups at inference time
        Index(
            "ix_eflomal_dictionary_assessment_source", "assessment_id", "source_word"
        ),
    )


class EflomalCooccurrence(Base):
    """Verse-level co-occurrence statistics for word pairs.

    Separate from EflomalDictionary because the key space differs:
    - Dictionary contains only pairs the model actually aligned.
    - This table contains ALL word pairs that co-occurred in any verse,
      including pairs that were never aligned (co_occur > 0, aligned = 0).
    Words are stored in normalized form (lowercase, alphanumeric only).

    - co_occur_count: number of verses where both words appear.
    - aligned_count: number of those verses where the model aligned them.

    The ratio aligned/co_occur is used as a co-occurrence consistency signal
    in the scoring formula (weighted geometric mean with dictionary probability).
    """

    __tablename__ = "eflomal_cooccurrence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer, ForeignKey("eflomal_assessment.id", ondelete="CASCADE"), nullable=False
    )
    source_word = Column(String, nullable=False)
    target_word = Column(String, nullable=False)
    co_occur_count = Column(Integer, nullable=False)
    aligned_count = Column(Integer, nullable=False)

    __table_args__ = (
        Index(
            "ix_eflomal_cooccurrence_lookup",
            "assessment_id",
            "source_word",
            "target_word",
        ),
    )


class EflomalTargetWordCount(Base):
    """Corpus-wide frequency of each target-language word.

    Counts every occurrence of each word across all verses in the target
    Bible text, regardless of whether the word was aligned or co-occurred
    with any source word.  Words are stored in normalized form.

    Used as the denominator in missing-word detection: a word that appears
    500 times but is only aligned 10 times behaves very differently from one
    that appears and is aligned 10 times.  Neither EflomalDictionary nor
    EflomalCooccurrence captures this marginal frequency.
    """

    __tablename__ = "eflomal_target_word_count"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer, ForeignKey("eflomal_assessment.id", ondelete="CASCADE"), nullable=False
    )
    word = Column(String, nullable=False)
    count = Column(Integer, nullable=False)

    __table_args__ = (
        Index(
            "ux_eflomal_target_word_count_assessment_word",
            "assessment_id",
            "word",
            unique=True,
        ),
    )


class EflomalPrior(Base):
    """LEX-format priors from the final alignment loop, one row per BPE token pair.

    Consumed at predict-time by eflomal together with the BPE models to
    score unseen verse pairs against a small anchor sample from training.

    - source_bpe / target_bpe: SentencePiece pieces (e.g. "▁house").
    - alpha: Dirichlet-prior strength, typically in [0.5, 0.95].
    """

    __tablename__ = "eflomal_prior"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer, ForeignKey("eflomal_assessment.id", ondelete="CASCADE"), nullable=False
    )
    source_bpe = Column(Text, nullable=False)
    target_bpe = Column(Text, nullable=False)
    alpha = Column(Float, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "alpha >= 0.5 AND alpha <= 0.95", name="ck_eflomal_prior_alpha_range"
        ),
        Index("ix_eflomal_prior_assessment", "assessment_id"),
        Index(
            "ux_eflomal_prior_assessment_source_target",
            "assessment_id",
            "source_bpe",
            "target_bpe",
            unique=True,
        ),
    )


class EflomalBpeModel(Base):
    """Serialized SentencePiece BPE model protobuf, one per direction.

    Two rows per assessment — direction 'source' and 'target'. The
    model_bytes column stores the output of
    SentencePieceProcessor.serialized_model_proto() directly.
    """

    __tablename__ = "eflomal_bpe_model"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer, ForeignKey("eflomal_assessment.id", ondelete="CASCADE"), nullable=False
    )
    direction = Column(Text, nullable=False)
    model_bytes = Column(LargeBinary, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "direction IN ('source', 'target')",
            name="ck_eflomal_bpe_model_direction",
        ),
        Index(
            "ux_eflomal_bpe_model_assessment_direction",
            "assessment_id",
            "direction",
            unique=True,
        ),
    )


class LanguageProfile(Base):
    __tablename__ = "language_profiles"

    iso_639_3 = Column(String(3), ForeignKey("iso_language.iso639"), primary_key=True)
    name = Column(Text, nullable=False)
    autonym = Column(Text, nullable=True)
    family = Column(Text, nullable=True)
    branch = Column(Text, nullable=True)
    script = Column(Text, nullable=True)
    typology_summary = Column(Text, nullable=True)
    morphology_notes = Column(Text, nullable=True)
    grammar_sketch = Column(Text, nullable=True)
    common_affixes = Column(JSONB, nullable=True)
    sources = Column(JSONB, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class LanguageMorpheme(Base):
    __tablename__ = "language_morphemes"

    id = Column(Integer, primary_key=True)
    iso_639_3 = Column(
        String(3),
        ForeignKey("language_profiles.iso_639_3", ondelete="CASCADE"),
        nullable=False,
    )
    morpheme = Column(Text, nullable=False)
    morpheme_class = Column(Text, nullable=False)
    first_seen_revision_id = Column(
        Integer,
        ForeignKey("bible_revision.id", ondelete="SET NULL"),
        nullable=True,
    )
    first_seen_at = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (
        Index(
            "ux_language_morphemes_iso_morpheme",
            "iso_639_3",
            "morpheme",
            unique=True,
        ),
        Index("ix_language_morphemes_iso", "iso_639_3"),
        Index("ix_language_morphemes_iso_class", "iso_639_3", "morpheme_class"),
    )


class LanguageAffix(Base):
    __tablename__ = "language_affixes"

    id = Column(Integer, primary_key=True)
    iso_639_3 = Column(
        String(3),
        ForeignKey("language_profiles.iso_639_3", ondelete="CASCADE"),
        nullable=False,
    )
    form = Column(Text, nullable=False)
    position = Column(Text, nullable=False)
    gloss = Column(Text, nullable=False)
    examples = Column(JSONB, nullable=True)
    n_runs = Column(SmallInteger, nullable=False, server_default="1")
    source_model = Column(Text, nullable=True)
    first_seen_revision_id = Column(
        Integer,
        ForeignKey("bible_revision.id", ondelete="SET NULL"),
        nullable=True,
    )
    first_seen_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint(
            "position IN ('prefix', 'suffix', 'infix')",
            name="ck_language_affixes_position",
        ),
        CheckConstraint("n_runs >= 1", name="ck_language_affixes_n_runs_min_1"),
        Index(
            "ux_language_affixes_iso_form_position_gloss",
            "iso_639_3",
            "form",
            "position",
            "gloss",
            unique=True,
        ),
        Index("ix_language_affixes_iso", "iso_639_3"),
        Index("ix_language_affixes_iso_position", "iso_639_3", "position"),
    )


class TokenizerRun(Base):
    __tablename__ = "tokenizer_runs"

    id = Column(Integer, primary_key=True)
    iso_639_3 = Column(
        String(3),
        ForeignKey("language_profiles.iso_639_3", ondelete="CASCADE"),
        nullable=False,
    )
    revision_id = Column(
        Integer,
        ForeignKey("bible_revision.id", ondelete="CASCADE"),
        nullable=False,
    )
    n_sample_verses = Column(Integer)
    sample_method = Column(Text)
    source_model = Column(Text)
    stats_json = Column(JSONB)
    status = Column(Text, nullable=False, server_default="completed")
    created_at = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (
        Index("ix_tokenizer_runs_iso", "iso_639_3"),
        Index("ix_tokenizer_runs_revision", "revision_id"),
    )


class VerseMorphemeIndex(Base):
    __tablename__ = "verse_morpheme_index"

    id = Column(Integer, primary_key=True)
    verse_text_id = Column(
        Integer,
        ForeignKey("verse_text.id", ondelete="CASCADE"),
        nullable=False,
    )
    morpheme_id = Column(
        Integer,
        ForeignKey("language_morphemes.id", ondelete="CASCADE"),
        nullable=False,
    )
    count = Column(Integer, nullable=False, server_default="1")
    surface_forms = Column(JSONB)

    __table_args__ = (
        UniqueConstraint("verse_text_id", "morpheme_id", name="uq_verse_morpheme"),
        Index("ix_verse_morpheme_index_morpheme", "morpheme_id"),
        Index("ix_verse_morpheme_index_verse", "verse_text_id"),
    )


class WordMorphemeIndex(Base):
    __tablename__ = "word_morpheme_index"

    id = Column(Integer, primary_key=True)
    iso_639_3 = Column(
        String(3),
        ForeignKey("language_profiles.iso_639_3", ondelete="CASCADE"),
        nullable=False,
    )
    word = Column(Text, nullable=False)
    morpheme_id = Column(
        Integer,
        ForeignKey("language_morphemes.id", ondelete="CASCADE"),
        nullable=False,
    )
    position = Column(Integer, nullable=False)
    total_morphemes = Column(Integer, nullable=False)
    word_count = Column(Integer, nullable=False, server_default="1")

    __table_args__ = (
        UniqueConstraint(
            "iso_639_3",
            "word",
            "morpheme_id",
            "position",
            name="uq_word_morpheme_pos",
        ),
        Index("ix_word_morpheme_iso", "iso_639_3"),
        Index("ix_word_morpheme_morpheme", "morpheme_id"),
    )


class TfidfArtifactRun(Base):
    """One row per TF-IDF assessment that produced encoder artifacts.

    Header row for the TF-IDF artifact store. Artifacts (two vectorizers +
    one SVD matrix) hang off this by assessment_id.
    """

    __tablename__ = "tfidf_artifact_runs"

    assessment_id = Column(
        Integer,
        ForeignKey("assessment.id", ondelete="CASCADE"),
        primary_key=True,
    )
    source_language = Column(String(3), nullable=True)
    n_components = Column(Integer, nullable=False)
    n_word_features = Column(Integer, nullable=False)
    n_char_features = Column(Integer, nullable=False)
    n_corpus_vrefs = Column(Integer, nullable=False)
    sklearn_version = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (
        Index("ix_tfidf_artifact_runs_lang", "source_language"),
        Index("ix_tfidf_artifact_runs_lang_created", "source_language", "created_at"),
    )


class TfidfVectorizerArtifact(Base):
    """Fitted TfidfVectorizer state — one row for 'word', one for 'char'."""

    __tablename__ = "tfidf_vectorizers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(
        Integer,
        ForeignKey("tfidf_artifact_runs.assessment_id", ondelete="CASCADE"),
        nullable=False,
    )
    kind = Column(Text, nullable=False)
    vocabulary = Column(JSONB, nullable=False)
    idf = Column(JSONB, nullable=False)
    params = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "assessment_id", "kind", name="uq_tfidf_vectorizer_assessment_kind"
        ),
        CheckConstraint("kind IN ('word', 'char')", name="ck_tfidf_vectorizer_kind"),
    )


class TfidfSvd(Base):
    """Fitted TruncatedSVD components matrix, stored as raw npy bytes."""

    __tablename__ = "tfidf_svd"

    assessment_id = Column(
        Integer,
        ForeignKey("tfidf_artifact_runs.assessment_id", ondelete="CASCADE"),
        primary_key=True,
    )
    n_components = Column(Integer, nullable=False)
    n_features = Column(Integer, nullable=False)
    components_npy = Column(LargeBinary, nullable=False)
    dtype = Column(Text, nullable=False, server_default="float32")


class TfidfSvdStaging(Base):
    """In-flight chunked upload of TF-IDF artifacts. Dropped after commit/abort."""

    __tablename__ = "tfidf_svd_staging"

    upload_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    assessment_id = Column(
        Integer,
        ForeignKey("assessment.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_language = Column(String(3), nullable=True)
    n_components = Column(Integer, nullable=False)
    n_corpus_vrefs = Column(Integer, nullable=False)
    sklearn_version = Column(Text, nullable=False)
    word_vocabulary = Column(JSONB, nullable=False)
    word_idf = Column(JSONB, nullable=False)
    word_params = Column(JSONB, nullable=False)
    char_vocabulary = Column(JSONB, nullable=False)
    char_idf = Column(JSONB, nullable=False)
    char_params = Column(JSONB, nullable=False)
    svd_n_components = Column(Integer, nullable=False)
    svd_n_features = Column(Integer, nullable=False)
    svd_dtype = Column(Text, nullable=False)
    total_chunks = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_tfidf_svd_staging_assessment", "assessment_id"),
        Index("ix_tfidf_svd_staging_created_at", "created_at"),
    )


class TfidfSvdChunk(Base):
    """One staged slice of an SVD components matrix. vstacked on commit."""

    __tablename__ = "tfidf_svd_chunk"

    upload_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tfidf_svd_staging.upload_id", ondelete="CASCADE"),
        primary_key=True,
    )
    chunk_index = Column(Integer, primary_key=True)
    components_bytes = Column(LargeBinary, nullable=False)
    received_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

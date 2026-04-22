import datetime
import math
import re
import unicodedata
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class VersionUpdate(BaseModel):
    id: int
    name: str = None
    iso_language: str = None
    iso_script: str = None
    abbreviation: str = None
    rights: Union[str, None] = None
    forwardTranslation: Union[int, None] = None
    backTranslation: Union[int, None] = None
    machineTranslation: bool = False
    add_to_groups: Optional[List[int]] = None
    remove_from_groups: Optional[List[int]] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "name": "English King James Version",
                "iso_language": "eng",
                "iso_script": "Latn",
                "abbreviation": "english_-_king_james_version",
                "machineTranslation": False,
                "add_to_groups": [1, 2],
                "remove_from_groups": [3, 4],
            }
        },
    }


class VersionIn(BaseModel):
    name: str
    iso_language: str
    iso_script: str
    abbreviation: str
    rights: Optional[str] = None
    forwardTranslation: Optional[int] = None
    backTranslation: Optional[int] = None
    machineTranslation: Optional[bool] = False
    is_reference: Optional[bool] = False
    add_to_groups: List[int]

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "English King James Version",
                "iso_language": "eng",
                "iso_script": "Latn",
                "abbreviation": "english_-_king_james_version",
                "machineTranslation": False,
                "add_to_groups": [1],
            }
        },
    }


class VersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    iso_language: str
    iso_script: str
    abbreviation: str
    rights: Union[str, None] = None
    forwardTranslation: Union[int, None] = None
    backTranslation: Union[int, None] = None
    machineTranslation: bool = False
    owner_id: int


class VersionOut_v3(BaseModel):
    id: int
    name: str
    iso_language: str
    iso_script: str
    abbreviation: str
    rights: Union[str, None] = None
    forward_translation_id: Union[int, None] = None
    back_translation_id: Union[int, None] = None
    machineTranslation: bool = False
    owner_id: Union[int, None] = None
    group_ids: List[int] = []
    is_reference: bool = False

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "name": "English King James Version",
                "iso_language": "eng",
                "iso_script": "Latn",
                "abbreviation": "english_-_king_james_version",
                "machineTranslation": False,
                "is_reference": False,
                "owner_id": 1,
                "group_ids": [1, 2],
            }
        },
        "from_attributes": True,
    }


class RevisionIn(BaseModel):
    version_id: int
    name: Optional[str] = None
    published: Optional[bool] = False
    backTranslation: Optional[int] = None
    machineTranslation: Optional[bool] = False

    model_config = {
        "json_schema_extra": {
            "example": {
                "version_id": 1,
                "name": "June 2024",
                "published": False,
                "backTranslation": 1,
                "machineTranslation": False,
            }
        },
    }


class RevisionOut(BaseModel):
    id: int
    bible_version_id: int
    version_abbreviation: Optional[str] = None
    date: Optional[datetime.date] = None
    name: Optional[str] = None
    published: Optional[bool] = False
    backTranslation: Optional[int] = None
    machineTranslation: Optional[bool] = False
    iso_language: Optional[str] = None


class RevisionOut_v3(BaseModel):
    id: int
    bible_version_id: int
    version_abbreviation: Optional[str] = None
    date: Optional[datetime.date] = None
    name: Optional[str] = None
    published: Optional[bool] = False
    back_translation_id: Optional[int] = None
    machineTranslation: Optional[bool] = False
    iso_language: Optional[str] = None
    is_reference: Optional[bool] = False

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "bible_version_id": 1,
                "version_abbreviation": "english_-_king_james_version",
                "date": "2024-06-01",
                "name": "June 2024",
                "published": False,
                "back_translation_id": 1,
                "machineTranslation": False,
                "iso_language": "eng",
                "is_reference": False,
            }
        },
    }


class VerseText(BaseModel):
    id: Optional[int] = None
    text: str
    verse_reference: str
    verse_references: Optional[List[str]] = None
    first_verse_reference: Optional[str] = None
    revision_id: int
    book: Optional[str] = None
    chapter: Optional[int] = None
    verse: Optional[int] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "text": "In the beginning God created the heaven and the earth.",
                "verse_reference": "GEN 1:1",
                "verse_references": ["GEN 1:1"],
                "first_verse_reference": "GEN 1:1",
                "revision_id": 1,
                "book": "GEN",
                "chapter": 1,
                "verse": 1,
            }
        },
    }


class AssessmentStatus(str, Enum):
    queued = "queued"
    running = "running"
    finished = "finished"
    failed = "failed"


ASSESSMENT_VALID_TRANSITIONS = {
    AssessmentStatus.queued: {AssessmentStatus.running, AssessmentStatus.failed},
    # running → running is intentional: allows runners to send progress updates
    AssessmentStatus.running: {
        AssessmentStatus.running,
        AssessmentStatus.finished,
        AssessmentStatus.failed,
    },
}

ASSESSMENT_TERMINAL_STATUSES = {AssessmentStatus.finished, AssessmentStatus.failed}


class AssessmentStatusUpdate(BaseModel):
    status: AssessmentStatus
    status_detail: Optional[str] = None

    model_config = {"use_enum_values": True}


class AssessmentType(Enum):
    word_alignment = "word-alignment"
    sentence_length = "sentence-length"
    semantic_similarity = "semantic-similarity"
    ngrams = "ngrams"
    tfidf = "tfidf"
    text_lengths = "text-lengths"
    agent_critique = "agent-critique"


class AssessmentIn(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    source_language: Optional[str] = None
    target_language: Optional[str] = None
    kwargs: Optional[Dict[str, Any]] = None

    @field_validator("kwargs")
    @classmethod
    def validate_kwargs(cls, v):
        if v is None:
            return v
        if len(v) > 20:
            raise ValueError("kwargs may not contain more than 20 keys")
        for key, val in v.items():
            if len(key) > 64:
                raise ValueError(
                    f"kwargs key '{key[:64]}...' exceeds 64-character limit"
                )
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", key):
                raise ValueError(
                    f"kwargs key '{key}' must be a valid Python identifier"
                )
            if not isinstance(val, (str, int, float, bool, type(None))):
                raise ValueError(
                    f"kwargs values must be scalar types, got {type(val).__name__} for key '{key}'"
                )
            if isinstance(val, str) and len(val) > 1000:
                raise ValueError("kwargs string values must not exceed 1000 characters")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "revision_id": 1,
                "reference_id": 1,
                "type": "word-alignment",
                "kwargs": {"top_k": 5},
            }
        },
        "use_enum_values": True,
    }


class AssessmentOut(BaseModel):
    id: int
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    status: Optional[str] = None
    requested_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    owner_id: Optional[int] = None
    status_detail: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "revision_id": 1,
                "reference_id": 1,
                "type": "word-alignment",
                "status": "finished",
                "requested_time": "2024-06-01T12:00:00",
                "start_time": "2024-06-01T12:00:00",
                "end_time": "2024-06-01T12:00:00",
                "owner_id": 1,
            }
        },
        "from_attributes": True,
    }


class SemanticSimilarityRequest(BaseModel):
    text1: str = Field(..., max_length=10000)
    text2: str = Field(..., max_length=10000)
    source_language: str = Field(..., max_length=10)
    target_language: str = Field(..., max_length=10)

    model_config = {
        "json_schema_extra": {
            "example": {
                "text1": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                "text2": "In the beginning God created the heavens and the earth.",
                "source_language": "swh",
                "target_language": "eng",
            }
        }
    }


class SemanticSimilarityResponse(BaseModel):
    score: float


class TextLengthsInferenceResponse(BaseModel):
    word_count_difference: int
    char_count_difference: int


class TextPair(BaseModel):
    vref: Optional[str] = Field(default=None, max_length=50)
    source_text: Optional[str] = Field(default=None, max_length=10000)
    target_text: str = Field(..., max_length=10000)


class PredictInput(BaseModel):
    pairs: List[TextPair] = Field(..., min_length=1, max_length=5000)
    assessment_id: Optional[int] = None
    revision_id: Optional[int] = None
    reference_id: Optional[int] = None
    source_language: Optional[str] = Field(default=None, max_length=10)
    target_language: Optional[str] = Field(default=None, max_length=10)
    limit: Optional[int] = Field(default=None, ge=1, le=10000)
    apps: Optional[List[str]] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "pairs": [
                    {
                        "vref": "GEN 1:1",
                        "source_text": "In the beginning God created the heavens and the earth.",
                        "target_text": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                    }
                ],
                "revision_id": 1,
                "reference_id": 2,
                "source_language": "eng",
                "target_language": "swh",
                "apps": ["ngrams", "tfidf"],
            }
        }
    }


class PredictAppResult(BaseModel):
    status: Literal["ok", "error"]
    data: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: int


class PredictFanoutResponse(BaseModel):
    pairs: List[TextPair]
    results: Dict[str, PredictAppResult]


# Results model to record in the DB.


class Result_v1(BaseModel):
    id: Optional[int] = None
    vref: Optional[str] = None
    source: Optional[str] = None
    target: Optional[str] = None
    score: float
    flag: bool = False
    note: Optional[str] = None
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None


class Result_v2(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    vref: Optional[str] = None
    source: Optional[str] = None
    target: Optional[List[dict]] = None
    score: float
    flag: bool = False
    note: Optional[str] = None
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None
    hide: bool = False

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "score": 0.28,
                "flag": False,
                "vref": "GEN 1:1",
                "hide": False,
                "assessment_id": 1,
            }
        },
    }


class MultipleResult(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    revision_id: Optional[int] = None
    reference_id: Optional[int] = None
    vref: Optional[str] = None
    score: float
    mean_score: Optional[float] = None
    stdev_score: Optional[float] = None
    z_score: Optional[float] = None
    flag: bool = False
    note: Optional[str] = None
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None
    hide: bool = False


class NgramResult(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    ngram: str
    ngram_size: int
    vrefs: List[str]  # ✅ Store multiple verse references for the n-gram


class TextLengthsResult(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    vref: Optional[str] = None
    vrefs: Optional[List[str]] = None
    word_lengths: float
    char_lengths: float
    word_lengths_z: float
    char_lengths_z: float


class TfidfResult(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    vref: Optional[str] = None
    similarity: float  # or cosine_distance: float
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {"vref": "GEN 1:2", "similarity": 0.0835, "assessment_id": 1}
        }
    }


class WordAlignment(BaseModel):
    model_config = {"from_attributes": True}

    id: Optional[int] = None
    assessment_id: int
    vref: Optional[str] = None
    source: str
    target: str
    score: float
    flag: bool = False
    note: Optional[str] = None
    hide: bool = False
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None


class AlignmentMatch(BaseModel):
    source_word: str
    target_word: str
    rank: int
    probability: float
    support_mass: float
    support_hits: int
    strength_mass: float
    strength_margin_mass: float
    strength_confidence: float


class Language(BaseModel):
    iso639: str
    name: str

    model_config = {
        "json_schema_extra": {"example": {"iso639": "eng", "name": "English"}},
    }


class Script(BaseModel):
    iso15924: str
    name: str

    model_config = {
        "json_schema_extra": {"example": {"iso15924": "Latn", "name": "Latin"}},
    }


class User(BaseModel):
    id: Optional[int] = None
    username: str
    email: Optional[EmailStr] = None  # Assuming users have an email field
    is_admin: Optional[bool] = False

    class Config:
        orm_mode = True


# group pydantic model
class Group(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None

    class Config:
        orm_mode = True


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


# Agent Word Alignment models
class AgentWordAlignmentIn(BaseModel):
    source_word: str
    target_word: str
    source_language: str
    target_language: str
    score: float = 0.0
    is_human_verified: bool = False

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_word": "love",
                "target_word": "amor",
                "source_language": "eng",
                "target_language": "spa",
                "score": 0.95,
                "is_human_verified": False,
            }
        }
    }


class AgentWordAlignmentOut(BaseModel):
    id: int
    source_word: str
    target_word: str
    source_language: str
    target_language: str
    score: float
    is_human_verified: bool
    created_at: Optional[datetime.datetime] = None
    last_updated: Optional[datetime.datetime] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "source_word": "love",
                "target_word": "amor",
                "source_language": "eng",
                "target_language": "spa",
                "score": 0.95,
                "is_human_verified": False,
                "created_at": "2024-06-01T12:00:00",
                "last_updated": "2024-06-01T12:00:00",
            }
        },
        "from_attributes": True,
    }


class AgentWordAlignmentBulkItem(BaseModel):
    source_word: str
    target_word: str
    score: float = 0.0
    is_human_verified: bool = False


class AgentWordAlignmentBulkRequest(BaseModel):
    source_language: str  # ISO 639-3
    target_language: str  # ISO 639-3
    alignments: list[AgentWordAlignmentBulkItem]


class LexemeCardIn(BaseModel):
    source_lemma: Optional[str] = None
    target_lemma: str
    source_language: str
    target_language: str

    @field_validator("target_lemma")
    @classmethod
    def normalize_target_lemma(cls, v):
        return unicodedata.normalize("NFC", v).lower() if v else v

    @field_validator("source_lemma")
    @classmethod
    def normalize_source_lemma(cls, v):
        return unicodedata.normalize("NFC", v) if v else v

    pos: Optional[str] = None
    surface_forms: Optional[list] = None
    source_surface_forms: Optional[list] = None  # Source language surface forms
    senses: Optional[list] = None
    examples: Optional[list] = None  # List of example dicts for the given revision_id
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    alignment_scores: Optional[Dict[str, float]] = None

    @field_validator("surface_forms")
    @classmethod
    def normalize_surface_forms(cls, v):
        if v is None:
            return v
        return [unicodedata.normalize("NFC", s) if isinstance(s, str) else s for s in v]

    @field_validator("source_surface_forms")
    @classmethod
    def normalize_source_surface_forms(cls, v):
        if v is None:
            return v
        return [unicodedata.normalize("NFC", s) if isinstance(s, str) else s for s in v]

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_lemma": "love",
                "target_lemma": "amor",
                "source_language": "eng",
                "target_language": "spa",
                "pos": "verb",
                "surface_forms": [
                    "amor",
                    "amo",
                    "amas",
                    "ama",
                    "amamos",
                    "anan",
                ],  # Target language surface forms
                "source_surface_forms": [
                    "love",
                    "loves",
                    "loved",
                    "loving",
                ],  # Source language surface forms
                "senses": [
                    {
                        "definition": "to feel deep affection",
                        "examples": ["I love you"],
                    },
                    {"definition": "to enjoy greatly", "examples": ["I love pizza"]},
                ],
                "examples": [
                    {"source": "I love you", "target": "Te amo"},
                    {"source": "They love music", "target": "Aman la música"},
                ],
                "confidence": 0.95,
                "english_lemma": "love",
                "alignment_scores": {"love": 0.92, "you": 0.88},
            }
        }
    }


class LexemeCardOut(BaseModel):
    id: int
    source_lemma: Optional[str] = None
    target_lemma: str
    source_language: str
    target_language: str
    pos: Optional[str] = None
    surface_forms: Optional[list] = None
    source_surface_forms: Optional[list] = None  # Source language surface forms
    senses: Optional[list] = None
    examples: Optional[list] = None  # Filtered list for the requested revision_id
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    alignment_scores: Optional[Dict[str, float]] = None
    created_at: Optional[datetime.datetime] = None
    last_updated: Optional[datetime.datetime] = None
    last_user_edit: Optional[datetime.datetime] = None


class ListMode(str, Enum):
    """Mode for handling list fields in PATCH operations."""

    append = "append"  # Add new items to existing list
    replace = "replace"  # Overwrite entire list
    merge = "merge"  # Append + deduplicate (case-insensitive for string lists)


class LexemeCardPatch(BaseModel):
    """Partial update model for lexeme cards - all fields optional."""

    source_lemma: Optional[str] = None
    target_lemma: Optional[str] = None

    @field_validator("target_lemma")
    @classmethod
    def normalize_target_lemma(cls, v):
        return unicodedata.normalize("NFC", v).lower() if v else v

    @field_validator("source_lemma")
    @classmethod
    def normalize_source_lemma(cls, v):
        return unicodedata.normalize("NFC", v) if v else v

    pos: Optional[str] = None
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    surface_forms: Optional[List[str]] = None
    source_surface_forms: Optional[List[str]] = None
    senses: Optional[List[dict]] = None
    examples: Optional[List[dict]] = None  # Each dict has: source, target, revision_id
    # Dict values can be float or None (None means remove that key)
    alignment_scores: Optional[Dict[str, Optional[float]]] = None

    @field_validator("surface_forms")
    @classmethod
    def normalize_surface_forms(cls, v):
        if v is None:
            return v
        return [unicodedata.normalize("NFC", s) for s in v]

    @field_validator("source_surface_forms")
    @classmethod
    def normalize_source_surface_forms(cls, v):
        if v is None:
            return v
        return [unicodedata.normalize("NFC", s) for s in v]

    model_config = {
        "json_schema_extra": {
            "example": {
                "surface_forms": ["new_form1", "new_form2"],
                "examples": [
                    {
                        "source": "Example source",
                        "target": "Example target",
                        "revision_id": 123,
                    }
                ],
            }
        }
    }


class OmissionIssueIn(BaseModel):
    """Omission critique issue: source text missing from the draft."""

    source_text: str = Field(min_length=1)
    comments: Optional[str] = None
    severity: int = Field(ge=0, le=5)  # 0=none, 5=critical

    model_config = {"str_strip_whitespace": True}


class AdditionIssueIn(BaseModel):
    """Addition critique issue: draft text not present in the source."""

    draft_text: str = Field(min_length=1)
    comments: Optional[str] = None
    severity: int = Field(ge=0, le=5)  # 0=none, 5=critical

    model_config = {"str_strip_whitespace": True}


class ReplacementIssueIn(BaseModel):
    """Replacement critique issue: source text incorrectly rendered as draft text."""

    source_text: str = Field(min_length=1)
    draft_text: str = Field(min_length=1)
    comments: Optional[str] = None
    severity: int = Field(ge=0, le=5)  # 0=none, 5=critical

    model_config = {"str_strip_whitespace": True}


class CritiqueStorageRequest(BaseModel):
    """Request to store critique results for a verse.

    The critique is linked to a specific agent translation by agent_translation_id.
    The assessment_id and vref are derived from the referenced translation.
    """

    agent_translation_id: int
    omissions: list[OmissionIssueIn] = []
    additions: list[AdditionIssueIn] = []
    replacements: list[ReplacementIssueIn] = []

    model_config = {
        "json_schema_extra": {
            "example": {
                "agent_translation_id": 1,
                "omissions": [
                    {
                        "source_text": "in the beginning",
                        "comments": "Missing key phrase",
                        "severity": 4,
                    }
                ],
                "additions": [
                    {
                        "draft_text": "extra words",
                        "comments": "Not in source",
                        "severity": 2,
                    }
                ],
                "replacements": [
                    {
                        "source_text": "love",
                        "draft_text": "like",
                        "comments": "Incorrect translation",
                        "severity": 3,
                    }
                ],
            }
        }
    }


class CritiqueIssueOut(BaseModel):
    """Individual critique issue for output."""

    id: int
    assessment_id: int
    agent_translation_id: int
    vref: str
    book: str
    chapter: int
    verse: int
    issue_type: Literal["omission", "addition", "replacement"]
    source_text: Optional[str] = None
    draft_text: Optional[str] = None
    comments: Optional[str] = None
    severity: int
    is_resolved: bool = False
    resolved_by_id: Optional[int] = None
    resolved_at: Optional[datetime.datetime] = None
    resolution_notes: Optional[str] = None
    created_at: Optional[datetime.datetime] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "assessment_id": 123,
                "agent_translation_id": 1,
                "vref": "JHN 1:1",
                "book": "JHN",
                "chapter": 1,
                "verse": 1,
                "issue_type": "omission",
                "source_text": "in the beginning",
                "draft_text": None,
                "comments": "Missing key phrase from source text",
                "severity": 4,
                "is_resolved": False,
                "resolved_by_id": None,
                "resolved_at": None,
                "resolution_notes": None,
                "created_at": "2024-06-01T12:00:00",
            }
        },
        "from_attributes": True,
    }


class CritiqueIssueResolutionRequest(BaseModel):
    """Request to resolve a critique issue."""

    resolution_notes: Optional[str] = Field(
        None, description="Optional notes about how the issue was resolved"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "resolution_notes": "Issue was addressed by updating the translation in the revision.",
            }
        },
        "from_attributes": True,
    }


class RevisionChapters(BaseModel):
    """Response model for available chapters in a revision."""

    chapters: Dict[str, List[int]]

    model_config = {
        "json_schema_extra": {
            "example": {
                "chapters": {
                    "GEN": [1, 2, 3, 4, 5],
                    "EXO": [1, 2, 3],
                }
            }
        },
    }


# Agent Translation models
class AgentTranslationIn(BaseModel):
    """Single translation input for storage."""

    vref: str
    draft_text: Optional[str] = None
    hyper_literal_translation: Optional[str] = None
    literal_translation: Optional[str] = None
    english_translation: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "vref": "JHN 1:1",
                "draft_text": "Na mwanzo kulikuwa na Neno",
                "hyper_literal_translation": "And beginning there-was with Word",
                "literal_translation": "In the beginning was the Word",
                "english_translation": "In the beginning was the Word",
            }
        }
    }


class AgentTranslationStorageRequest(BaseModel):
    """Request to store a single translation."""

    assessment_id: int
    vref: str
    draft_text: Optional[str] = None
    hyper_literal_translation: Optional[str] = None
    literal_translation: Optional[str] = None
    english_translation: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "assessment_id": 123,
                "vref": "JHN 1:1",
                "draft_text": "Na mwanzo kulikuwa na Neno",
                "hyper_literal_translation": "And beginning there-was with Word",
                "literal_translation": "In the beginning was the Word",
                "english_translation": "In the beginning was the Word",
            }
        }
    }


class AgentTranslationBulkRequest(BaseModel):
    """Request to store multiple translations in bulk."""

    assessment_id: int
    translations: List[AgentTranslationIn]

    model_config = {
        "json_schema_extra": {
            "example": {
                "assessment_id": 123,
                "translations": [
                    {
                        "vref": "JHN 1:1",
                        "draft_text": "Na mwanzo kulikuwa na Neno",
                        "hyper_literal_translation": "And beginning there-was with Word",
                        "literal_translation": "In the beginning was the Word",
                    },
                    {
                        "vref": "JHN 1:2",
                        "draft_text": "Huyu alikuwa mwanzoni na Mungu",
                        "hyper_literal_translation": "This-one he-was beginning with God",
                        "literal_translation": "He was in the beginning with God",
                    },
                ],
            }
        }
    }


class AgentTranslationOut(BaseModel):
    """Response model for agent translation."""

    id: int
    assessment_id: int
    revision_id: int
    language: str
    script: str
    vref: str
    version: int
    draft_text: Optional[str] = None
    hyper_literal_translation: Optional[str] = None
    literal_translation: Optional[str] = None
    english_translation: Optional[str] = None
    created_at: Optional[datetime.datetime] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "assessment_id": 123,
                "revision_id": 456,
                "language": "eng",
                "script": "Latn",
                "vref": "JHN 1:1",
                "version": 1,
                "draft_text": "Na mwanzo kulikuwa na Neno",
                "hyper_literal_translation": "And beginning there-was with Word",
                "literal_translation": "In the beginning was the Word",
                "english_translation": "In the beginning was the Word",
                "created_at": "2024-06-01T12:00:00",
            }
        },
        "from_attributes": True,
    }


# Training models


class TrainingType(str, Enum):
    serval_nmt = "serval-nmt"
    semantic_similarity = "semantic-similarity"


class TrainingStatus(str, Enum):
    queued = "queued"
    preparing = "preparing"
    training = "training"
    downloading = "downloading"
    uploading = "uploading"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"


class TrainingJobIn(BaseModel):
    source_revision_id: int
    target_revision_id: int
    options: Optional[Dict[str, Any]] = None


class TrainingJobOut(BaseModel):
    id: int
    type: str
    source_revision_id: int
    target_revision_id: int
    source_language: str
    target_language: str
    status: str
    status_detail: Optional[str] = None
    percent_complete: Optional[float] = None
    external_ids: Optional[Dict[str, Any]] = None
    result_url: Optional[str] = None
    result_metadata: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None
    requested_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    owner_id: Optional[int] = None
    session_id: Optional[str] = None

    model_config = {"from_attributes": True, "use_enum_values": True}


class InferenceReadiness(BaseModel):
    ready: bool
    pending_training: List[str] = Field(default_factory=list)


class TrainingResponse(BaseModel):
    session_id: str
    training_jobs: List[TrainingJobOut]
    inference_readiness: Dict[str, InferenceReadiness]


class TrainingJobStatusUpdate(BaseModel):
    status: TrainingStatus
    status_detail: Optional[str] = None
    percent_complete: Optional[float] = Field(None, ge=0.0, le=100.0)
    external_ids: Optional[Dict[str, Any]] = None
    result_url: Optional[str] = None
    result_metadata: Optional[Dict[str, Any]] = None

    model_config = {"use_enum_values": True}


class EflomalDictionaryItem(BaseModel):
    """Dictionary entry. Words in original form (case preserved)."""

    source_word: str
    target_word: str
    count: int
    probability: float


class EflomalCooccurrenceItem(BaseModel):
    """Co-occurrence entry. Words in normalized form (lowercase, alphanumeric)."""

    source_word: str
    target_word: str
    co_occur_count: int
    aligned_count: int


class EflomalTargetWordCountItem(BaseModel):
    """Target word frequency. Word in normalized form."""

    word: str
    count: int


class EflomalResultsPushRequest(BaseModel):
    """Create the eflomal_assessment metadata row (no bulk data).

    After this call succeeds, push dictionary, cooccurrences, and
    target-word-counts via their own endpoints, then PATCH the
    assessment status to 'finished'.
    """

    assessment_id: int
    source_language: Optional[str] = None
    target_language: Optional[str] = None
    num_verse_pairs: int
    num_alignment_links: int
    num_dictionary_entries: int
    num_missing_words: int


class EflomalAssessmentOut(BaseModel):
    """Push response — summary only, not the 470K+ data rows."""

    id: int
    assessment_id: int
    source_language: Optional[str] = None
    target_language: Optional[str] = None
    num_verse_pairs: int
    num_alignment_links: int
    num_dictionary_entries: int
    num_missing_words: int
    created_at: Optional[datetime.datetime] = None
    model_config = {"from_attributes": True}


class EflomalResultsPullResponse(BaseModel):
    """Full eflomal training artifacts for inference consumption.

    Replaces Modal's load_artifacts() — all data needed to run realtime_assess()
    is returned in a single response for the caller to load and cache.
    """

    assessment_id: int
    source_language: Optional[str] = None
    target_language: Optional[str] = None
    num_verse_pairs: int
    num_alignment_links: int
    num_dictionary_entries: int
    num_missing_words: int
    created_at: Optional[datetime.datetime] = None
    dictionary: list[EflomalDictionaryItem]
    cooccurrences: list[EflomalCooccurrenceItem]
    target_word_counts: list[EflomalTargetWordCountItem]


# --- TF-IDF artifact (encoder state) models ---


class TfidfVectorizerPayload(BaseModel):
    vocabulary: Dict[str, int]
    idf: List[float]
    params: Dict[str, Any]


class TfidfSvdPayload(BaseModel):
    n_components: int
    n_features: int
    dtype: Literal["float32", "float64"] = "float32"
    components_b64: str


class TfidfArtifactsPushRequest(BaseModel):
    source_language: Optional[str] = Field(default=None, max_length=3)
    n_components: int
    n_corpus_vrefs: int
    sklearn_version: str
    word_vectorizer: TfidfVectorizerPayload
    char_vectorizer: TfidfVectorizerPayload
    svd: TfidfSvdPayload


class TfidfArtifactsPushResponse(BaseModel):
    assessment_id: int
    n_word_features: int
    n_char_features: int
    components_bytes: int


class TfidfArtifactsPullResponse(BaseModel):
    assessment_id: int
    source_language: Optional[str] = None
    n_components: int
    n_word_features: int
    n_char_features: int
    n_corpus_vrefs: int
    sklearn_version: str
    created_at: Optional[datetime.datetime] = None
    word_vectorizer: TfidfVectorizerPayload
    char_vectorizer: TfidfVectorizerPayload
    svd: TfidfSvdPayload


class TfidfByVectorRequest(BaseModel):
    assessment_id: int
    vector: List[float]
    limit: int = Field(default=10, ge=1, le=500)
    reference_id: Optional[int] = None

    @field_validator("vector")
    @classmethod
    def _reject_non_finite(cls, v: List[float]) -> List[float]:
        if any(not math.isfinite(x) for x in v):
            raise ValueError("vector must not contain inf or nan")
        return v


# --- Assessment Results Push/Delete models ---


class AssessmentResultItem(BaseModel):
    vref: str
    score: float
    flag: bool = False
    source: Optional[str] = None
    target: Optional[Any] = None
    note: Optional[str] = None


class AlignmentScoreItem(BaseModel):
    vref: str
    score: float
    flag: bool = False
    source: Optional[str] = None
    target: Optional[str] = None
    note: Optional[str] = None


class TextLengthsItem(BaseModel):
    vref: str
    word_lengths: float
    char_lengths: float
    word_lengths_z: float
    char_lengths_z: float


class TfidfPcaVectorItem(BaseModel):
    vref: str
    vector: List[float] = Field(..., min_length=300, max_length=300)


class NgramItem(BaseModel):
    ngram: str
    ngram_size: int
    vrefs: List[str] = Field(..., max_length=50_000)


class InsertResponse(BaseModel):
    ids: List[int]


class DeleteRequest(BaseModel):
    ids: List[int]


class DeleteResponse(BaseModel):
    deleted: int


MorphemeClass = Literal["LEXICAL", "GRAMMATICAL", "BOUND_ROOT", "UNKNOWN"]


class LanguageProfileIn(BaseModel):
    name: str
    autonym: Optional[str] = None
    family: Optional[str] = None
    branch: Optional[str] = None
    script: Optional[str] = None
    typology_summary: Optional[str] = None
    morphology_notes: Optional[str] = None
    grammar_sketch: Optional[str] = Field(None, max_length=65536)
    common_affixes: Optional[List[Dict[str, Any]]] = None
    sources: Optional[List[str]] = None


class LanguageProfileOut(LanguageProfileIn):
    model_config = ConfigDict(from_attributes=True)

    iso_639_3: str
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class MorphemeIn(BaseModel):
    morpheme: str
    morpheme_class: MorphemeClass


class MorphemeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    morpheme: str
    morpheme_class: MorphemeClass
    first_seen_revision_id: Optional[int] = None


class MorphemeListOut(BaseModel):
    iso_639_3: str
    total: int
    morphemes: List[MorphemeOut]


class TokenizerRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    iso_639_3: str
    revision_id: int
    n_sample_verses: Optional[int] = None
    sample_method: Optional[str] = None
    source_model: Optional[str] = None
    status: str
    stats_json: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime.datetime] = None


class TokenizerRunListOut(BaseModel):
    runs: List[TokenizerRunOut]


class TokenizerRunRequest(BaseModel):
    iso_639_3: str
    revision_id: int
    n_sample_verses: Optional[int] = None
    sample_method: Optional[str] = None
    source_model: Optional[str] = None
    profile: Optional[LanguageProfileIn] = None
    morphemes: List[MorphemeIn] = Field(default_factory=list)
    stats: Optional[Dict[str, Any]] = None


class TokenizerRunCommitResponse(BaseModel):
    run_id: int
    # n_morphemes_new + n_morphemes_existing == unique morphemes after
    # casefolding (may be < len(payload.morphemes) due to case dedup).
    # n_class_conflicts is a subset of n_morphemes_existing: a conflict is
    # an existing row whose stored class disagrees with the incoming class
    # (the stored class wins; the incoming class is logged and discarded).
    n_morphemes_new: int
    n_morphemes_existing: int
    n_class_conflicts: int


class IndexRequest(BaseModel):
    iso_639_3: str
    revision_id: int


class IndexResponse(BaseModel):
    verses_indexed: int
    unique_morpheme_verse_pairs: int


AffixPosition = Literal["prefix", "suffix", "infix"]


class AffixIn(BaseModel):
    form: str = Field(..., min_length=1)
    position: AffixPosition
    gloss: str = Field(..., min_length=1)
    examples: Optional[List[str]] = None
    n_runs: int = Field(default=1, ge=1, le=32767)


class AffixOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    form: str
    position: AffixPosition
    gloss: str
    examples: Optional[List[str]] = None
    n_runs: int = Field(default=1, ge=1, le=32767)
    source_model: Optional[str] = None
    first_seen_revision_id: Optional[int] = None


class AffixListOut(BaseModel):
    iso_639_3: str
    total: int
    affixes: List[AffixOut]


class AffixCommitRequest(BaseModel):
    iso_639_3: str
    revision_id: Optional[int] = None
    source_model: Optional[str] = None
    affixes: List[AffixIn] = Field(default_factory=list)


class AffixCommitResponse(BaseModel):
    n_affixes_new: int
    n_affixes_updated: int
    n_affixes_unchanged: int


class AffixReplaceResponse(BaseModel):
    n_deleted: int
    n_inserted: int


class WordIndexRequest(BaseModel):
    iso_639_3: str = Field(..., min_length=3, max_length=3)
    revision_id: int


class WordIndexResponse(BaseModel):
    unique_words_indexed: int
    word_morpheme_pairs: int


class CooccurrenceItem(BaseModel):
    morpheme: str
    morpheme_class: str
    co_occurrence_count: int
    example_words: List[str]
    typical_position: str


class CooccurrenceResponse(BaseModel):
    morpheme: str
    total_words_containing: int
    is_truncated: bool = False
    cooccurrences: List[CooccurrenceItem]


class MorphemeSearchResult(BaseModel):
    verse_reference: str
    text: str
    comparison_text: Optional[str] = None
    surface_forms: List[str]
    count: int


class MorphemeSearchResponse(BaseModel):
    morpheme: str
    iso_639_3: str
    result_count: int
    results: List[MorphemeSearchResult]

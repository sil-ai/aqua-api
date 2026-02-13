import datetime
from enum import Enum
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, EmailStr, Field


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
    revision_id: int
    book: Optional[str] = None
    chapter: Optional[int] = None
    verse: Optional[int] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "text": "In the beginning God created the heaven and the earth.",
                "verse_reference": "GEN 1:1",
                "revision_id": 1,
                "book": "GEN",
                "chapter": 1,
                "verse": 1,
            }
        },
    }


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
    train: Optional[bool] = None
    source_language: Optional[str] = None
    target_language: Optional[str] = None
    first_vref: Optional[str] = None
    last_vref: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {"revision_id": 1, "reference_id": 1, "type": "word-alignment"}
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
    # class Config:
    #     use_enum_values = True

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "revision_id": 1,
                "reference_id": 1,
                "type": "word-alignment",
                "status": "completed",
                "requested_time": "2024-06-01T12:00:00",
                "start_time": "2024-06-01T12:00:00",
                "end_time": "2024-06-01T12:00:00",
                "owner_id": 1,
            }
        },
        "from_attributes": True,
    }


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
    pos: Optional[str] = None
    surface_forms: Optional[list] = None
    source_surface_forms: Optional[list] = None  # Source language surface forms
    senses: Optional[list] = None
    examples: Optional[list] = None  # List of example dicts for the given revision_id
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    alignment_scores: Optional[Dict[str, float]] = None

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
    pos: Optional[str] = None
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    surface_forms: Optional[List[str]] = None
    source_surface_forms: Optional[List[str]] = None
    senses: Optional[List[dict]] = None
    examples: Optional[List[dict]] = None  # Each dict has: source, target, revision_id
    # Dict values can be float or None (None means remove that key)
    alignment_scores: Optional[Dict[str, Optional[float]]] = None

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


class AdditionIssueIn(BaseModel):
    """Addition critique issue: draft text not present in the source."""

    draft_text: str = Field(min_length=1)
    comments: Optional[str] = None
    severity: int = Field(ge=0, le=5)  # 0=none, 5=critical


class ReplacementIssueIn(BaseModel):
    """Replacement critique issue: source text incorrectly rendered as draft text."""

    source_text: str = Field(min_length=1)
    draft_text: str = Field(min_length=1)
    comments: Optional[str] = None
    severity: int = Field(ge=0, le=5)  # 0=none, 5=critical


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

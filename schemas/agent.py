"""Agent-domain schemas: word alignments, lexeme cards, card translations,
critiques, and agent translations (issue #729).
"""

import datetime
import unicodedata
from enum import Enum
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


# Agent Word Alignment models
class AgentWordAlignmentIn(BaseModel):
    source_word: str
    target_word: str
    source_version_id: int
    target_version_id: int
    score: float = 0.0
    is_human_verified: bool = False

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_word": "love",
                "target_word": "amor",
                "source_version_id": 1,
                "target_version_id": 2,
                "score": 0.95,
                "is_human_verified": False,
            }
        }
    }


class AgentWordAlignmentOut(BaseModel):
    id: int
    source_word: str
    target_word: str
    source_version_id: int
    target_version_id: int
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
                "source_version_id": 1,
                "target_version_id": 2,
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
    source_version_id: int
    target_version_id: int
    alignments: list[AgentWordAlignmentBulkItem]


class LexemeCardIn(BaseModel):
    source_lemma: Optional[str] = None
    target_lemma: str
    source_version_id: int
    target_version_id: int

    @field_validator("target_lemma")
    @classmethod
    def normalize_target_lemma(cls, v):
        return unicodedata.normalize("NFC", v).lower() if v else v

    @field_validator("source_lemma")
    @classmethod
    def normalize_source_lemma(cls, v):
        return unicodedata.normalize("NFC", v).lower() if v else v

    pos: Optional[str] = None
    surface_forms: Optional[list] = None
    source_surface_forms: Optional[list] = None  # Source language surface forms
    senses: Optional[list] = None
    examples: Optional[list] = None  # List of example dicts for the given revision_id
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    alignment_scores: Optional[Dict[str, float]] = None
    # Opaque token bumped when the canonical card-builder rebuilds the card.
    # Derived translations record this in parent_build_version so cache
    # invalidation can detect when the canonical has moved on.
    build_version: Optional[str] = None
    # Provenance: model id/name that built this card (e.g. "claude-sonnet-...",
    # "gpt-oss-..."). Lets downstream consumers harvest only trusted cards.
    model: Optional[str] = None

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
                "source_version_id": 1,
                "target_version_id": 2,
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
                "build_version": "agent-20260514T123000Z",
                "model": "claude-sonnet-4-6",
            }
        }
    }


class LexemeCardOut(BaseModel):
    id: int
    source_lemma: Optional[str] = None
    target_lemma: str
    source_version_id: int
    target_version_id: int
    pos: Optional[str] = None
    surface_forms: Optional[list] = None
    source_surface_forms: Optional[list] = None  # Source language surface forms
    senses: Optional[list] = None
    # Each example dict has shape {"id": int, "source": str | None, "target": str};
    # source is None when a translation overlay was requested (either via
    # ?lang= or auto-derived by pivot routing) but no card_translations row
    # exists for this card. Filtered to the requested revision_id when set.
    examples: Optional[list] = None
    confidence: Optional[float] = None
    english_lemma: Optional[str] = None
    alignment_scores: Optional[Dict[str, float]] = None
    build_version: Optional[str] = None
    model: Optional[str] = None
    created_at: Optional[datetime.datetime] = None
    last_updated: Optional[datetime.datetime] = None
    last_user_edit: Optional[datetime.datetime] = None
    # True when the response reflects the requested language: either canonical
    # source matched ?lang, or a card_translations overlay was applied. False
    # only on the bulk endpoint when ?lang was requested but no overlay
    # existed — in that case source-side fields above are null. The by-id
    # endpoint never returns False (it 404s instead, to trigger derivation).
    has_translation_overlay: bool = True


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

    # NFC-only here (no .lower()). The canonical write path lowercases at the
    # call site to honor the canonical source_lemma index; the overlay write
    # path preserves casing (overlay source_lemma has no lookup-key role and
    # is display data, matching CardTranslationIn's preservation contract).
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
    build_version: Optional[str] = None
    model: Optional[str] = None

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


class CardTranslationExampleIn(BaseModel):
    example_id: int
    source_text: str

    @field_validator("source_text")
    @classmethod
    def normalize_source_text(cls, v):
        return unicodedata.normalize("NFC", v) if v else v


class CardTranslationIn(BaseModel):
    """Write-side payload for a single (card, target_language) translation overlay."""

    language_iso: str = Field(min_length=3, max_length=3)
    source_lemma: Optional[str] = None
    source_surface_forms: Optional[List[str]] = None
    senses: Optional[List[dict]] = None
    parent_build_version: Optional[str] = None
    build_version: Optional[str] = None
    examples: List[CardTranslationExampleIn] = []

    @field_validator("language_iso")
    @classmethod
    def normalize_language_iso(cls, v):
        return v.lower() if v else v

    # Unlike LexemeCardIn.source_lemma (lowercased to match target_lemma's
    # case-insensitive uniqueness), card_translations.source_lemma has no
    # lookup-key role, so casing is preserved.
    @field_validator("source_lemma")
    @classmethod
    def normalize_source_lemma(cls, v):
        return unicodedata.normalize("NFC", v) if v else v

    @field_validator("source_surface_forms")
    @classmethod
    def normalize_source_surface_forms(cls, v):
        if v is None:
            return v
        return [unicodedata.normalize("NFC", s) if isinstance(s, str) else s for s in v]

    model_config = {
        "json_schema_extra": {
            "example": {
                "language_iso": "spa",
                "source_lemma": "camino",
                "source_surface_forms": ["camino", "caminos"],
                "senses": [
                    {"definition": "camino, ruta, sendero"},
                ],
                "parent_build_version": "v1",
                "build_version": "spa-v1",
                "examples": [
                    {
                        "example_id": 12345,
                        "source_text": "nos salvó al renovarnos al nacer de nuevo",
                    },
                ],
            }
        }
    }


class CardTranslationExampleOut(BaseModel):
    id: int
    example_id: int
    source_text: str
    created_at: Optional[datetime.datetime] = None


class CardTranslationOut(BaseModel):
    """Read-side shape of a stored translation overlay."""

    id: int
    card_id: int
    language_iso: str
    source_lemma: Optional[str] = None
    source_surface_forms: Optional[list] = None
    senses: Optional[list] = None
    parent_build_version: Optional[str] = None
    build_version: Optional[str] = None
    created_at: Optional[datetime.datetime] = None
    last_updated: Optional[datetime.datetime] = None
    last_user_edit: Optional[datetime.datetime] = None
    examples: List[CardTranslationExampleOut] = []


class SuggestionItem(BaseModel):
    """A proposed replacement/rendering, used for span suggestions and whole-verse alternatives."""

    text: str = Field(min_length=1)
    note: Optional[str] = None

    model_config = {"str_strip_whitespace": True}


class IssueIn(BaseModel):
    """A single MQM-aligned critique issue."""

    dimension: Literal["accuracy", "terminology", "linguistic_conventions"]
    subtype: str = Field(min_length=1, max_length=100)
    source_text: Optional[str] = None
    draft_text: Optional[str] = None
    comments: Optional[str] = None
    severity: Optional[int] = Field(default=None, ge=1, le=5)
    detector: Optional[str] = Field(default=None, max_length=50)
    evidence: Optional[List[str]] = None
    suggestions: Optional[List[SuggestionItem]] = None

    model_config = {"str_strip_whitespace": True}


class CritiqueStorageRequest(BaseModel):
    """Request to store MQM-aligned critique issues for a verse.

    The critique is linked to a specific agent translation by agent_translation_id.
    The assessment_id and vref are derived from the referenced translation.
    """

    agent_translation_id: int
    issues: list[IssueIn] = []

    model_config = {
        "json_schema_extra": {
            "example": {
                "agent_translation_id": 1,
                "issues": [
                    {
                        "dimension": "accuracy",
                        "subtype": "mistranslation/hallucination-numbers",
                        "source_text": "forty days",
                        "draft_text": "fourteen days",
                        "comments": "Number mistranslated",
                        "severity": 4,
                        "detector": "number_diff",
                        "evidence": ["source: 40", "draft: 14"],
                        "suggestions": [
                            {"text": "forty days", "note": "match the source number"}
                        ],
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
    dimension: str
    subtype: str
    source_text: Optional[str] = None
    draft_text: Optional[str] = None
    comments: Optional[str] = None
    severity: Optional[int] = None
    detector: Optional[str] = None
    evidence: Optional[List[str]] = None
    suggestions: Optional[List[SuggestionItem]] = None
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
                "dimension": "accuracy",
                "subtype": "mistranslation/hallucination-numbers",
                "source_text": "forty days",
                "draft_text": "fourteen days",
                "comments": "Number mistranslated",
                "severity": 4,
                "detector": "number_diff",
                "evidence": ["source: 40", "draft: 14"],
                "suggestions": [
                    {"text": "forty days", "note": "match the source number"}
                ],
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


# Agent Translation models
class AgentTranslationIn(BaseModel):
    """Single translation input for storage."""

    vref: str
    draft_text: Optional[str] = None
    hyper_literal_translation: Optional[str] = None
    literal_translation: Optional[str] = None
    english_translation: Optional[str] = None
    alternatives: Optional[List[SuggestionItem]] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "vref": "JHN 1:1",
                "draft_text": "Na mwanzo kulikuwa na Neno",
                "hyper_literal_translation": "And beginning there-was with Word",
                "literal_translation": "In the beginning was the Word",
                "english_translation": "In the beginning was the Word",
                "alternatives": [
                    {
                        "text": "In the beginning the Word already existed",
                        "note": "smoother English phrasing",
                    }
                ],
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
    alternatives: Optional[List[SuggestionItem]] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "assessment_id": 123,
                "vref": "JHN 1:1",
                "draft_text": "Na mwanzo kulikuwa na Neno",
                "hyper_literal_translation": "And beginning there-was with Word",
                "literal_translation": "In the beginning was the Word",
                "english_translation": "In the beginning was the Word",
                "alternatives": [
                    {
                        "text": "In the beginning the Word already existed",
                        "note": "smoother English phrasing",
                    }
                ],
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
                        "alternatives": [
                            {
                                "text": "In the beginning the Word already existed",
                                "note": "smoother English phrasing",
                            }
                        ],
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
    reference_version_id: int
    script: str
    vref: str
    version: int
    draft_text: Optional[str] = None
    hyper_literal_translation: Optional[str] = None
    literal_translation: Optional[str] = None
    english_translation: Optional[str] = None
    alternatives: Optional[List[SuggestionItem]] = None
    created_at: Optional[datetime.datetime] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "assessment_id": 123,
                "revision_id": 456,
                "reference_version_id": 789,
                "script": "Latn",
                "vref": "JHN 1:1",
                "version": 1,
                "draft_text": "Na mwanzo kulikuwa na Neno",
                "hyper_literal_translation": "And beginning there-was with Word",
                "literal_translation": "In the beginning was the Word",
                "english_translation": "In the beginning was the Word",
                "alternatives": [
                    {
                        "text": "In the beginning the Word already existed",
                        "note": "smoother English phrasing",
                    }
                ],
                "created_at": "2024-06-01T12:00:00",
            }
        },
        "from_attributes": True,
    }


class BulkLexemeCardDeleteResponse(BaseModel):
    """Row counts deleted by DELETE /v3/agent/lexeme-card?target_version_id=X.

    Wipes every lexeme card for the given target_version_id regardless of
    source_version_id (cards built under different pivots all go), plus
    cascades to examples and card_translations.
    """

    target_version_id: int
    lexeme_cards_deleted: int
    examples_deleted: int
    card_translations_deleted: int


__all__ = [
    "AgentWordAlignmentIn",
    "AgentWordAlignmentOut",
    "AgentWordAlignmentBulkItem",
    "AgentWordAlignmentBulkRequest",
    "LexemeCardIn",
    "LexemeCardOut",
    "ListMode",
    "LexemeCardPatch",
    "CardTranslationExampleIn",
    "CardTranslationIn",
    "CardTranslationExampleOut",
    "CardTranslationOut",
    "SuggestionItem",
    "IssueIn",
    "CritiqueStorageRequest",
    "CritiqueIssueOut",
    "CritiqueIssueResolutionRequest",
    "AgentTranslationIn",
    "AgentTranslationStorageRequest",
    "AgentTranslationBulkRequest",
    "AgentTranslationOut",
    "BulkLexemeCardDeleteResponse",
]

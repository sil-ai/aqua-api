import datetime
import math
import re
import unicodedata
from enum import Enum
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)


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
    transcribed_audio: Optional[bool] = None
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
    transcribed_audio: Optional[bool] = False
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
    transcribed_audio: bool = False
    deleted: bool = False

    @field_validator("deleted", mode="before")
    @classmethod
    def _coerce_deleted_null_to_false(cls, value):
        # BibleVersion.deleted column is nullable; legacy rows have NULL.
        return False if value is None else value

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
                "deleted": False,
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


class WordCount(BaseModel):
    word: str
    count: int = Field(ge=1)


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


# All assessment runs (training and non-training) follow queued → running →
# finished. Progress within `running` is reported via percent_complete on
# self-loop PATCHes. `failed` is reachable from any non-terminal state.
ASSESSMENT_VALID_TRANSITIONS = {
    AssessmentStatus.queued: {
        AssessmentStatus.running,
        AssessmentStatus.failed,
    },
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
    percent_complete: Optional[float] = Field(None, ge=0.0, le=100.0)

    model_config = {"use_enum_values": True}


class AssessmentType(Enum):
    word_alignment = "word-alignment"
    sentence_length = "sentence-length"
    semantic_similarity = "semantic-similarity"
    ngrams = "ngrams"
    tfidf = "tfidf"
    text_lengths = "text-lengths"
    agent_critique = "agent-critique"


def _validate_assessment_kwargs(v):
    """Shared validator for Assessment.kwargs / TrainingJob.options.

    /v3/train persists options onto Assessment.kwargs (issue #571), so both
    endpoints must enforce the same shape — otherwise /v3/train can create
    Assessment rows that break existing /v3/assessment kwargs queries.
    """
    if v is None:
        return v
    if len(v) > 20:
        raise ValueError("kwargs may not contain more than 20 keys")
    for key, val in v.items():
        if len(key) > 64:
            raise ValueError(f"kwargs key '{key[:64]}...' exceeds 64-character limit")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", key):
            raise ValueError(f"kwargs key '{key}' must be a valid Python identifier")
        if not isinstance(val, (str, int, float, bool, type(None))):
            raise ValueError(
                f"kwargs values must be scalar types, got {type(val).__name__} for key '{key}'"
            )
        if isinstance(val, str) and len(val) > 1000:
            raise ValueError("kwargs string values must not exceed 1000 characters")
    return v


class AssessmentIn(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    kwargs: Optional[Dict[str, Any]] = None

    @field_validator("kwargs")
    @classmethod
    def validate_kwargs(cls, v):
        return _validate_assessment_kwargs(v)

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
    percent_complete: Optional[float] = None
    is_training: bool = False
    kwargs: Optional[Dict[str, Any]] = None
    attempt_count: int = 0

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
                "kwargs": None,
                "attempt_count": 0,
            }
        },
        "from_attributes": True,
    }


class SemanticSimilarityRequest(BaseModel):
    text1: str = Field(..., max_length=10000)
    text2: str = Field(..., max_length=10000)
    source_version_id: int
    target_version_id: int

    model_config = {
        "json_schema_extra": {
            "example": {
                "text1": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                "text2": "In the beginning God created the heavens and the earth.",
                "source_version_id": 1,
                "target_version_id": 2,
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
    source_version_id: Optional[int] = None
    target_version_id: Optional[int] = None
    limit: Optional[int] = Field(default=None, ge=1, le=10000)
    apps: Optional[List[str]] = None
    include_translation: bool = False
    include_critique: bool = False
    # Agent-only override for the LLM the agent should use. The allowlist
    # lives in the agent's separate Modal repo (models.selectable_models in
    # its config.yaml), so we can't validate the name here — agent rejects
    # unknown names server-side. max_length caps abuse at this boundary.
    model: Optional[str] = Field(default=None, max_length=200)

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
                "source_version_id": 1,
                "target_version_id": 2,
                "apps": ["ngrams", "tfidf", "agent"],
                "include_translation": False,
                "include_critique": False,
                "model": "claude-opus-4-7",
            }
        }
    }

    @model_validator(mode="after")
    def _critique_requires_translation(self) -> "PredictInput":
        # Mirrors the agent-side validator in aqua-assessments
        # (shared/predict_input.py): critique runs over translations, so
        # asking for it without translation is a bug, not a silent no-op.
        # Reject early at the API boundary so the caller sees a 422 rather
        # than a per-app error string in the fan-out response.
        if self.include_critique and not self.include_translation:
            raise ValueError("include_critique=True requires include_translation=True")
        return self


class PredictAppResult(BaseModel):
    status: Literal["ok", "error", "not_trained"]
    data: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: int


class PredictJobHandle(BaseModel):
    id: str
    status: Literal["running", "complete", "failed"]
    includes: List[Literal["translation", "critique"]]
    poll_url: str


class PredictFanoutResponse(BaseModel):
    pairs: List[TextPair]
    results: Dict[str, PredictAppResult]
    job: Optional[PredictJobHandle] = None

    @model_serializer(mode="wrap")
    def _drop_unset_job(self, handler):
        # The fast-only path (no include_translation/critique) shouldn't
        # surface a `"job": null` key in the response — keep the wire
        # shape identical to pre-async callers.
        data = handler(self)
        if data.get("job") is None:
            data.pop("job", None)
        return data


class PredictJobPair(BaseModel):
    vref: Optional[str] = None
    source_text: Optional[str] = None
    target_text: str
    translation: Optional[Dict[str, Any]] = None
    critique: Optional[Dict[str, Any]] = None
    lexeme_cards: Optional[List[Dict[str, Any]]] = None


class PredictJobStatusResponse(BaseModel):
    id: str
    status: Literal["running", "complete", "failed"]
    includes: List[Literal["translation", "critique"]]
    pairs: List[PredictJobPair]
    error: Optional[str] = None


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


class TfidfNeighbour(BaseModel):
    vref: str
    similarity: float
    target_revision_text: Optional[str] = None
    source_revision_text: Optional[str] = None


class TfidfNeighboursBlock(BaseModel):
    target_neighbours: List[TfidfNeighbour] = Field(default_factory=list)
    # Asymmetric with `NgramCorpusBlock.source_corpus`: tfidf has no
    # `None`/`[]` distinction here — an empty list means either "no
    # source-side training" or "trained, no neighbours found". Clients
    # cannot tell the two apart from this field alone.
    source_neighbours: List[TfidfNeighbour] = Field(default_factory=list)


class NgramVerseHit(BaseModel):
    vref: str
    target_revision_text: Optional[str] = None
    source_revision_text: Optional[str] = None


class NgramMatch(BaseModel):
    id: int
    ngram: str
    ngram_size: int
    verses: List[NgramVerseHit] = Field(default_factory=list)


class NgramCorpusMatches(BaseModel):
    matches: List[NgramMatch] = Field(default_factory=list)


class NgramCorpusBlock(BaseModel):
    target_corpus: NgramCorpusMatches = Field(default_factory=NgramCorpusMatches)
    # `None` (not an empty block) when the session has no source-side ngrams
    # training to match against — distinguishes "we looked and found nothing"
    # from "no source-side corpus available".
    source_corpus: Optional[NgramCorpusMatches] = None


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


# Training models


class TrainingType(str, Enum):
    semantic_similarity = "semantic-similarity"
    tfidf = "tfidf"
    word_alignment = "word-alignment"
    ngrams = "ngrams"
    agent_critique = "agent-critique"


class TrainingJobIn(BaseModel):
    """Identify the training pair by version_id (preferred — latest non-deleted
    revision is resolved server-side) or by revision_id when a specific
    revision is required. Exactly one of (version_id, revision_id) must be
    provided per side."""

    model_config = {
        "json_schema_extra": {
            "example": {
                "source_version_id": 1,
                "target_version_id": 2,
                "options": {"use_eflomal": True},
                "apps": ["word-alignment", "tfidf"],
            }
        }
    }

    source_version_id: Optional[int] = None
    target_version_id: Optional[int] = None
    source_revision_id: Optional[int] = None
    target_revision_id: Optional[int] = None
    options: Optional[Dict[str, Any]] = None
    apps: Optional[List[str]] = None

    @field_validator("options")
    @classmethod
    def validate_options(cls, v):
        return _validate_assessment_kwargs(v)

    @model_validator(mode="after")
    def _require_one_id_per_side(self):
        if (self.source_version_id is None) == (self.source_revision_id is None):
            raise ValueError(
                "Provide exactly one of source_version_id or source_revision_id"
            )
        if (self.target_version_id is None) == (self.target_revision_id is None):
            raise ValueError(
                "Provide exactly one of target_version_id or target_revision_id"
            )
        return self


class TrainingJobOut(BaseModel):
    """TrainingJob is metadata only after aqua-api#584 — status, timing, and
    progress are sourced from the linked Assessment row when serializing."""

    id: int
    type: str
    source_revision_id: int
    target_revision_id: int
    source_version_id: int
    target_version_id: int
    options: Optional[Dict[str, Any]] = None
    requested_time: Optional[datetime.datetime] = None
    owner_id: Optional[int] = None
    session_id: Optional[str] = None
    assessment_id: Optional[int] = None

    # Mirrored from the linked Assessment row.
    status: Optional[str] = None
    status_detail: Optional[str] = None
    percent_complete: Optional[float] = None
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None

    model_config = {"use_enum_values": True}


class InferenceReadiness(BaseModel):
    ready: bool
    pending_training: List[str] = Field(default_factory=list)


class TrainingResponse(BaseModel):
    session_id: str
    training_jobs: List[TrainingJobOut]
    inference_readiness: Dict[str, InferenceReadiness]


class TrainingSessionVrefResults(BaseModel):
    vref: str
    semantic_similarity: Optional[Result_v2] = None
    word_alignment: List[WordAlignment] = Field(default_factory=list)
    # Verse-level aggregate score for word-alignment, written to
    # `assessment_result` alongside the per-word-pair rows in
    # `alignment_top_source_scores`. Mirrors the shape of
    # `semantic_similarity` so clients can treat the two the same way.
    word_alignment_score: Optional[Result_v2] = None
    # Per-vref tfidf and ngrams mirror the predict pair shape: `tfidf` carries
    # nearest-neighbour vrefs from both sides (target and source) of the
    # trained corpora; `ngrams` carries trained ngrams that fire on this
    # verse, with `verses` lists hydrated from the corresponding revision
    # text. Predict's cross-axis matches (target ngrams in source text and
    # vice versa) are not returned — train-status reads stored corpus hits
    # only.
    tfidf: Optional[TfidfNeighboursBlock] = None
    ngrams: Optional[NgramCorpusBlock] = None
    # Full lexeme cards (same shape as `GET /v3/agent/lexeme-card`) whose
    # lemma or any surface form intersects this verse on either side.
    # Cards are filtered by the verse — the cards themselves are returned
    # in full, not a subset of their fields.
    lexeme_cards: List[LexemeCardOut] = Field(default_factory=list)


class TrainingSessionResultsPage(BaseModel):
    items: List[TrainingSessionVrefResults]
    total_count: int
    page: Optional[int] = None
    page_size: Optional[int] = None


class TrainingSessionResultsResponse(BaseModel):
    session_id: str
    training_jobs: List[TrainingJobOut]
    inference_readiness: Dict[str, InferenceReadiness]
    results: TrainingSessionResultsPage
    # True when the lexeme-card load hit the per-request cap and only
    # the highest-confidence prefix of cards was matched against the
    # page's vrefs. Lets a client distinguish "no card matches found"
    # from "we didn't look at every card".
    lexeme_cards_truncated: bool = False


class EflomalDictionaryItem(BaseModel):
    """Dictionary entry. Words in original form (case preserved)."""

    source_word: str
    target_word: str
    count: int
    probability: float


class EflomalTargetWordCountItem(BaseModel):
    """Target word frequency. Word in normalized form."""

    word: str
    count: int


class EflomalResultsPushRequest(BaseModel):
    """Create the eflomal_assessment metadata row (no bulk data).

    After this call succeeds, push dictionary, target-word-counts,
    priors, and BPE models via their own endpoints, then PATCH the
    assessment status to 'finished'.
    """

    assessment_id: int
    source_version_id: Optional[int] = None
    target_version_id: Optional[int] = None
    num_verse_pairs: int
    num_alignment_links: int
    num_dictionary_entries: int
    num_missing_words: int


class EflomalAssessmentOut(BaseModel):
    """Push response — summary only, not the 470K+ data rows."""

    id: int
    assessment_id: int
    source_version_id: int
    target_version_id: int
    num_verse_pairs: int
    num_alignment_links: int
    num_dictionary_entries: int
    num_missing_words: int
    created_at: Optional[datetime.datetime] = None
    model_config = {"from_attributes": True}


class EflomalPriorItem(BaseModel):
    """LEX prior row: one BPE token pair with a Dirichlet-prior alpha."""

    source_bpe: str
    target_bpe: str
    alpha: float = Field(ge=0.5, le=0.95, allow_inf_nan=False)


class EflomalReverseDictSource(BaseModel):
    """One source-word candidate in a reverse_dict entry."""

    source: str
    count: int


class EflomalBpeModels(BaseModel):
    """SentencePiece BPE models (serialized protobuf), one per direction."""

    source_model_b64: str
    target_model_b64: str


class EflomalResultsPullResponse(BaseModel):
    """Full eflomal training artifacts for inference consumption.

    Replaces Modal's load_artifacts() — all data needed to run realtime_assess()
    is returned in a single response for the caller to load and cache.
    """

    assessment_id: int
    source_version_id: int
    target_version_id: int
    num_verse_pairs: int
    num_alignment_links: int
    num_dictionary_entries: int
    num_missing_words: int
    created_at: Optional[datetime.datetime] = None
    reference_id: Optional[int] = None
    revision_id: Optional[int] = None
    reverse_dict: dict[str, list[EflomalReverseDictSource]] = Field(
        default_factory=dict
    )
    target_word_counts: list[EflomalTargetWordCountItem]
    priors: list[EflomalPriorItem] = Field(default_factory=list)
    bpe_models: Optional[EflomalBpeModels] = None


# --- TF-IDF artifact (encoder state) models ---


class TfidfVectorizerPayload(BaseModel):
    vocabulary: Dict[str, int]
    idf: List[float]
    params: Dict[str, Any]


# TfidfSvdMeta (shape + dtype only) is shared by chunked uploads that don't
# carry the bytes inline. TfidfSvdPayload adds the base64-encoded .npy blob.
class TfidfSvdMeta(BaseModel):
    n_components: int = Field(..., ge=1, le=4096)
    n_features: int = Field(..., ge=1, le=10_000_000)
    dtype: Literal["float32", "float64"] = "float32"


class TfidfSvdPayload(TfidfSvdMeta):
    components_b64: str


# Pull responses can downcast the components matrix to float16 or int8 to
# halve/quarter wire size. int8 carries a scale factor so clients can
# rehydrate via `arr.astype(float32) * int8_scale / 127`. Stays separate from
# TfidfSvdPayload so the push validator's float32/float64-only contract is
# unaffected by widening the response dtype set; only the dtype field is
# overridden to widen the Literal.
class TfidfSvdPullPayload(TfidfSvdMeta):
    dtype: Literal["float32", "float64", "float16", "int8"] = "float32"
    components_b64: str
    int8_scale: Optional[float] = None

    @model_validator(mode="after")
    def _int8_requires_scale(self) -> "TfidfSvdPullPayload":
        if self.dtype == "int8" and self.int8_scale is None:
            raise ValueError("int8_scale is required when dtype='int8'")
        if self.dtype != "int8" and self.int8_scale is not None:
            raise ValueError("int8_scale is only valid when dtype='int8'")
        return self


class TfidfArtifactsPushRequest(BaseModel):
    source_version_id: Optional[int] = None
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
    source_version_id: int
    n_components: int
    n_word_features: int
    n_char_features: int
    n_corpus_vrefs: int
    sklearn_version: str
    created_at: Optional[datetime.datetime] = None
    word_vectorizer: TfidfVectorizerPayload
    char_vectorizer: TfidfVectorizerPayload
    svd: TfidfSvdPullPayload


# --- Chunked TF-IDF artifact upload (for components_ arrays over the single-POST cap) ---


class TfidfArtifactsInitRequest(BaseModel):
    source_version_id: Optional[int] = None
    n_components: int
    n_corpus_vrefs: int
    sklearn_version: str
    word_vectorizer: TfidfVectorizerPayload
    char_vectorizer: TfidfVectorizerPayload
    svd: TfidfSvdMeta
    total_chunks: int = Field(..., ge=1, le=1024)


class TfidfArtifactsInitResponse(BaseModel):
    upload_id: str
    assessment_id: int
    total_chunks: int


class TfidfArtifactsChunkRequest(BaseModel):
    upload_id: str
    chunk_index: int = Field(..., ge=0)
    components_b64: str


class TfidfArtifactsChunkResponse(BaseModel):
    upload_id: str
    chunk_index: int
    bytes_received: int
    chunks_received: int
    total_chunks: int


class TfidfArtifactsCommitRequest(BaseModel):
    upload_id: str


class TfidfArtifactsAbortRequest(BaseModel):
    upload_id: str


class TfidfArtifactsAbortResponse(BaseModel):
    upload_id: str
    chunks_removed: int


TFIDF_CORPUS_VECTOR_DIM = 300


class TfidfByVectorRequest(BaseModel):
    assessment_id: int
    vector: List[float] = Field(
        ..., min_length=TFIDF_CORPUS_VECTOR_DIM, max_length=TFIDF_CORPUS_VECTOR_DIM
    )
    limit: int = Field(default=10, ge=1, le=500)
    reference_id: Optional[int] = Field(default=None, ge=1)

    @field_validator("vector")
    @classmethod
    def _reject_non_finite(cls, v: List[float]) -> List[float]:
        if any(not math.isfinite(x) for x in v):
            raise ValueError("vector must not contain inf or nan")
        return v


TFIDF_MAX_BATCH_VECTORS = 500
TFIDF_MAX_BATCH_RESULTS = 25000


class TfidfByVectorsRequest(BaseModel):
    assessment_id: int
    vectors: List[List[float]] = Field(
        ..., min_length=1, max_length=TFIDF_MAX_BATCH_VECTORS
    )
    limit: int = Field(default=10, ge=1, le=500)
    reference_id: Optional[int] = Field(default=None, ge=1)

    @field_validator("vectors")
    @classmethod
    def _validate_vectors(cls, vs: List[List[float]]) -> List[List[float]]:
        for i, vec in enumerate(vs):
            if len(vec) != TFIDF_CORPUS_VECTOR_DIM:
                raise ValueError(
                    f"vectors[{i}] has length {len(vec)}, expected {TFIDF_CORPUS_VECTOR_DIM}"
                )
            if any(not math.isfinite(x) for x in vec):
                raise ValueError(f"vectors[{i}] must not contain inf or nan")
        return vs


class TfidfByVectorsResponse(BaseModel):
    results: List[List[TfidfResult]]


# Upper bound on a single text to encode. A verse is well under this; the cap
# just stops a pathological multi-MB string from driving a huge transform on a
# worker thread.
TFIDF_MAX_TEXT_CHARS = 10_000


class TfidfByTextRequest(BaseModel):
    assessment_id: int
    text: str = Field(..., min_length=1, max_length=TFIDF_MAX_TEXT_CHARS)
    limit: int = Field(default=10, ge=1, le=500)
    reference_id: Optional[int] = Field(default=None, ge=1)
    # Drop the result whose vref matches exclude_vref (leakage guard). When
    # exclude_book is True, drop all results in exclude_vref's book instead.
    exclude_vref: Optional[str] = None
    exclude_book: bool = False

    @model_validator(mode="after")
    def _exclude_book_needs_vref(self) -> "TfidfByTextRequest":
        if self.exclude_book and not self.exclude_vref:
            raise ValueError("exclude_book=True requires exclude_vref")
        return self


class TfidfByTextsRequest(BaseModel):
    assessment_id: int
    texts: List[
        Annotated[str, Field(min_length=1, max_length=TFIDF_MAX_TEXT_CHARS)]
    ] = Field(..., min_length=1, max_length=TFIDF_MAX_BATCH_VECTORS)
    limit: int = Field(default=10, ge=1, le=500)
    reference_id: Optional[int] = Field(default=None, ge=1)
    # Per-text exclusion: exclude_vrefs[i] applies to texts[i]. Either omit it
    # or pass a same-length list as texts.
    exclude_vrefs: Optional[List[str]] = None
    exclude_book: bool = False

    @model_validator(mode="after")
    def _validate_exclusions(self) -> "TfidfByTextsRequest":
        if self.exclude_vrefs is not None and len(self.exclude_vrefs) != len(
            self.texts
        ):
            raise ValueError(
                "exclude_vrefs must be the same length as texts when provided"
            )
        if self.exclude_book and not self.exclude_vrefs:
            raise ValueError("exclude_book=True requires exclude_vrefs")
        return self


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


class TrainingArtifactOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    target_version_id: int
    iso_639_3: str
    grammar_sketch: Optional[str] = None
    source_model: Optional[str] = None
    # `source` indicates which store satisfied the grammar_sketch read:
    # "training_artifacts" = the version-keyed row supplied grammar_sketch.
    # "language_profile"   = grammar_sketch came from the iso-keyed
    #                         language_profiles row. This covers two cases:
    #                         (a) no training_artifacts row exists for this
    #                         version yet, and (b) a training_artifacts row
    #                         exists but its grammar_sketch is NULL. In case
    #                         (b), `source_model` is still surfaced from the
    #                         version-keyed row even though the sketch fell
    #                         back, so provenance isn't lost.
    source: Literal["training_artifacts", "language_profile"]
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class TrainingArtifactsDeleteResponse(BaseModel):
    """Row counts deleted by DELETE /v3/tokenizer/training-artifacts/{version_id}.

    The DELETE only touches rows version-stamped to this version_id;
    legacy `target_version_id IS NULL` rows and rows stamped to other
    versions of the same ISO are left intact. Lexeme cards are also
    untouched — they have their own DELETE endpoint.
    """

    target_version_id: int
    training_artifacts_deleted: int
    affixes_deleted: int
    morphemes_deleted: int


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


class BulkAffixDeleteResponse(BaseModel):
    """Row counts deleted by DELETE /v3/affixes-by-version/{version_id}.

    Mirrors the soft-union semantics of ``GET /v3/affixes-by-version``:
    deletes both rows version-stamped to ``version_id`` and legacy
    iso-keyed rows (``target_version_id IS NULL``) that share the
    version's ISO. Rows stamped to other versions of the same ISO are
    left intact. Counts are split so callers can see what came from
    each bucket.
    """

    target_version_id: int
    iso_639_3: str
    version_stamped_deleted: int
    iso_keyed_deleted: int
    total_deleted: int


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

    id: int
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


class AffixPatch(BaseModel):
    """Partial update model for language affixes — all fields optional."""

    form: Optional[str] = Field(default=None, min_length=1)
    position: Optional[AffixPosition] = None
    gloss: Optional[str] = Field(default=None, min_length=1)
    examples: Optional[List[str]] = None
    n_runs: Optional[int] = Field(default=None, ge=1, le=32767)
    source_model: Optional[str] = None

    @field_validator("form", "gloss", mode="after")
    @classmethod
    def _nfc_strip(cls, v):
        if v is None:
            return v
        v = unicodedata.normalize("NFC", v).strip()
        if not v:
            raise ValueError("must not be empty after NFC + strip")
        return v


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


class PivotCandidateIn(BaseModel):
    pivot_iso: str = Field(..., min_length=3, max_length=3)
    pivot_revision_id: int
    notes: Optional[str] = None


class PivotCandidateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    pivot_iso: str
    pivot_revision_id: int
    pivot_version_id: int  # derived via bible_revision.bible_version_id
    notes: Optional[str] = None
    language_profile: Optional[LanguageProfileOut] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class PivotCandidateListOut(BaseModel):
    candidates: List[PivotCandidateOut]


class LanguagePivotIn(BaseModel):
    target_iso: str = Field(..., min_length=3, max_length=3)
    pivot_iso: str = Field(..., min_length=3, max_length=3)
    notes: Optional[str] = None


class LanguagePivotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    target_iso: str
    pivot_iso: str
    pivot_revision_id: int
    pivot_version_id: int  # derived via bible_revision.bible_version_id
    notes: Optional[str] = None
    language_profile: Optional[LanguageProfileOut] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class LanguagePivotListOut(BaseModel):
    mappings: List[LanguagePivotOut]


class LanguagePivotMissOut(BaseModel):
    target_iso: str
    candidates: List[PivotCandidateOut]
    hint: str

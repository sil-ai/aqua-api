"""Assessment lifecycle, realtime-inference, and assessment-result schemas
(issue #729).

Covers the assessment status machine, the ``AssessmentIn``/``AssessmentOut``
request/response pair, the realtime semantic-similarity / text-lengths
inference shapes, the per-verse result models recorded in the DB
(``Result_*``, ngram/tfidf blocks, ``WordAlignment``), and the bulk
result push/delete item shapes.
"""

import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from .validators import _validate_assessment_kwargs


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


__all__ = [
    "AssessmentStatus",
    "ASSESSMENT_VALID_TRANSITIONS",
    "ASSESSMENT_TERMINAL_STATUSES",
    "AssessmentStatusUpdate",
    "AssessmentType",
    "AssessmentIn",
    "AssessmentOut",
    "SemanticSimilarityRequest",
    "SemanticSimilarityResponse",
    "TextLengthsInferenceResponse",
    "Result_v1",
    "Result_v2",
    "MultipleResult",
    "NgramResult",
    "TextLengthsResult",
    "TfidfResult",
    "TfidfNeighbour",
    "TfidfNeighboursBlock",
    "NgramVerseHit",
    "NgramMatch",
    "NgramCorpusMatches",
    "NgramCorpusBlock",
    "WordAlignment",
    "AlignmentMatch",
    "AssessmentResultItem",
    "AlignmentScoreItem",
    "TextLengthsItem",
    "TfidfPcaVectorItem",
    "NgramItem",
    "InsertResponse",
    "DeleteRequest",
    "DeleteResponse",
]

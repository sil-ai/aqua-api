"""Training job and training-session result schemas (issue #729)."""

import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .agent import LexemeCardOut
from .assessment import (
    NgramCorpusBlock,
    Result_v2,
    TfidfNeighboursBlock,
    WordAlignment,
)
from .validators import _validate_assessment_kwargs

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


__all__ = [
    "TrainingType",
    "TrainingJobIn",
    "TrainingJobOut",
    "InferenceReadiness",
    "TrainingResponse",
    "TrainingSessionVrefResults",
    "TrainingSessionResultsPage",
    "TrainingSessionResultsResponse",
]

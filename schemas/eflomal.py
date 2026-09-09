"""Eflomal word-alignment training-artifact schemas (issue #729)."""

import datetime
from typing import Optional

from pydantic import BaseModel, Field


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


__all__ = [
    "EflomalDictionaryItem",
    "EflomalTargetWordCountItem",
    "EflomalResultsPushRequest",
    "EflomalAssessmentOut",
    "EflomalPriorItem",
    "EflomalReverseDictSource",
    "EflomalBpeModels",
    "EflomalResultsPullResponse",
]

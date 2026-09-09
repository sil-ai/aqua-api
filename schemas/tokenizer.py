"""Tokenizer / morphology schemas: language profiles, morphemes, tokenizer
runs, indexing, cooccurrence, and morpheme search (issue #729).
"""

import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

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


__all__ = [
    "MorphemeClass",
    "LanguageProfileIn",
    "LanguageProfileOut",
    "MorphemeIn",
    "MorphemeOut",
    "MorphemeListOut",
    "TrainingArtifactOut",
    "TrainingArtifactsDeleteResponse",
    "TokenizerRunOut",
    "TokenizerRunListOut",
    "TokenizerRunRequest",
    "TokenizerRunCommitResponse",
    "IndexRequest",
    "IndexResponse",
    "WordIndexRequest",
    "WordIndexResponse",
    "CooccurrenceItem",
    "CooccurrenceResponse",
    "MorphemeSearchResult",
    "MorphemeSearchResponse",
]

"""TF-IDF artifact (encoder state) and TF-IDF query request/response schemas
(issue #729).
"""

import datetime
import math
from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .assessment import TfidfResult

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


__all__ = [
    "TfidfVectorizerPayload",
    "TfidfSvdMeta",
    "TfidfSvdPayload",
    "TfidfSvdPullPayload",
    "TfidfArtifactsPushRequest",
    "TfidfArtifactsPushResponse",
    "TfidfArtifactsPullResponse",
    "TfidfArtifactsInitRequest",
    "TfidfArtifactsInitResponse",
    "TfidfArtifactsChunkRequest",
    "TfidfArtifactsChunkResponse",
    "TfidfArtifactsCommitRequest",
    "TfidfArtifactsAbortRequest",
    "TfidfArtifactsAbortResponse",
    "TFIDF_CORPUS_VECTOR_DIM",
    "TfidfByVectorRequest",
    "TFIDF_MAX_BATCH_VECTORS",
    "TFIDF_MAX_BATCH_RESULTS",
    "TfidfByVectorsRequest",
    "TfidfByVectorsResponse",
    "TFIDF_MAX_TEXT_CHARS",
    "TfidfByTextRequest",
    "TfidfByTextsRequest",
]

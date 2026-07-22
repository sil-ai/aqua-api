"""Affix (prefix/suffix/infix) schemas (issue #729)."""

import unicodedata
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

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


__all__ = [
    "AffixPosition",
    "AffixIn",
    "AffixOut",
    "AffixListOut",
    "AffixCommitRequest",
    "AffixCommitResponse",
    "AffixReplaceResponse",
    "AffixPatch",
    "BulkAffixDeleteResponse",
]

"""Pivot-language candidate and mapping schemas (issue #729)."""

import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .tokenizer import LanguageProfileOut


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


__all__ = [
    "PivotCandidateIn",
    "PivotCandidateOut",
    "PivotCandidateListOut",
    "LanguagePivotIn",
    "LanguagePivotOut",
    "LanguagePivotListOut",
    "LanguagePivotMissOut",
]

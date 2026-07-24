"""Bible version / revision / verse schemas (issue #729)."""

import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    transcribed_audio: bool = False
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
    transcribed_audio: bool = False
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


__all__ = [
    "VersionUpdate",
    "VersionIn",
    "VersionOut",
    "VersionOut_v3",
    "RevisionIn",
    "RevisionOut",
    "RevisionOut_v3",
    "WordCount",
    "VerseText",
    "RevisionChapters",
    "Language",
    "Script",
]

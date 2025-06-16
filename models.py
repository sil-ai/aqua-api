import datetime
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, EmailStr


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
    add_to_groups: Optional[List[int]] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "English King James Version",
                "iso_language": "eng",
                "iso_script": "Latn",
                "abbreviation": "english_-_king_james_version",
                "machineTranslation": False,
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


class VerseText(BaseModel):
    id: Optional[int] = None
    text: str
    verse_reference: str
    revision_id: int
    book: Optional[str] = None
    chapter: Optional[int] = None
    verse: Optional[int] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "text": "In the beginning God created the heaven and the earth.",
                "verse_reference": "GEN 1:1",
                "revision_id": 1,
                "book": "GEN",
                "chapter": 1,
                "verse": 1,
            }
        },
    }


class AssessmentType(Enum):
    word_alignment = "word-alignment"
    sentence_length = "sentence-length"
    semantic_similarity = "semantic-similarity"
    model_config = ConfigDict(from_attributes=True)
    ngrams = "ngrams"
    tfidf = "tfidf"
    text_proportions = "text-proportions"


class AssessmentIn(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    train: Optional[bool] = None

    model_config = {
        "json_schema_extra": {
            "example": {"revision_id": 1, "reference_id": 1, "type": "word-alignment"}
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
    # class Config:
    #     use_enum_values = True

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 1,
                "revision_id": 1,
                "reference_id": 1,
                "type": "word-alignment",
                "status": "completed",
                "requested_time": "2024-06-01T12:00:00",
                "start_time": "2024-06-01T12:00:00",
                "end_time": "2024-06-01T12:00:00",
                "owner_id": 1,
            }
        },
        "from_attributes": True,
    }


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


class TextProportionsResult(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    vref: Optional[str] = None
    word_proportions: float
    char_proportions: float
    word_proportions_z: float
    char_proportions_z: float


class TfidfResult(BaseModel):
    id: Optional[int] = None
    assessment_id: Optional[int] = None
    vref: Optional[str] = None
    similarity: float  # or cosine_distance: float

    model_config = {
        "json_schema_extra": {
            "example": {"vref": "GEN 1:2", "similarity": 0.0835, "assessment_id": 1}
        }
    }


class WordAlignment(BaseModel):
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

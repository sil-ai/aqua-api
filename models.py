from pydantic import BaseModel, ConfigDict, EmailStr
from enum import Enum
from typing import Union, Optional, List
import datetime


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
    model_config = ConfigDict(from_attributes=True)
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


class RevisionIn(BaseModel):
    version_id: int
    name: Optional[str] = None
    published: Optional[bool] = False
    backTranslation: Optional[int] = None
    machineTranslation: Optional[bool] = False


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


class VerseText(BaseModel):
    id: Optional[int] = None
    text: str
    verse_reference: str
    revision_id: int
    book: Optional[str] = None
    chapter: Optional[int] = None
    verse: Optional[int] = None


class AssessmentType(Enum):
    dummy = "dummy"
    word_alignment = "word-alignment"
    sentence_length = "sentence-length"
    missing_words = "missing-words"
    semantic_similarity = "semantic-similarity"
    model_config = ConfigDict(from_attributes=True)
    question_answering = "question-answering"


class AssessmentIn(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType

    class Config:
        use_enum_values = True


class AssessmentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
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


class WordAlignment(BaseModel):
    id: Optional[int] = None
    assessment_id: int
    vref: str = None
    source: str
    target: str
    score: float
    flag: bool = False
    note: Optional[str] = None
    hide: bool = False
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None


# # Results model to record in the DB.
# class MissingWord(BaseModel):
#     assessment_id: int
#     vref: str
#     source: str
#     target: str
#     score: float
#     flag: bool = False
#     note: Optional[str] = None


# # Results is a list of results to push to the DB
# class Results(BaseModel):
#     results: List[Result]

# # Results is a list of missing words to push to the DB
# class MissingWords(BaseModel):
#     missing_words: List[MissingWord]


class Language(BaseModel):
    iso639: str
    name: str


class Script(BaseModel):
    iso15924: str
    name: str


class User(BaseModel):
    id: Optional[int] = None
    username: str
    email: Optional[EmailStr] = None  # Assuming users have an email field
    is_admin: Optional[bool] = False
    password: Optional[str] = None

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

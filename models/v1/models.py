from pydantic import BaseModel
from enum import Enum
from typing import Union, Optional, List
import datetime


class VersionIn(BaseModel):
    name: str
    isoLanguage: str
    isoScript: str
    abbreviation: str
    rights: Union[str, None] = None
    forwardTranslation: Union[int, None] = None
    backTranslation: Union[int, None] = None
    machineTranslation: bool = False


class VersionOut(BaseModel):
    id: int
    name: str
    isoLanguage: str
    isoScript: str
    abbreviation: str
    rights: Union[str, None] = None
    forwardTranslation: Union[int, None] = None
    backTranslation: Union[int, None] = None
    machineTranslation: bool = False


class RevisionIn(BaseModel):
    version_id: int
    name: Optional[str] = None
    published: Optional[bool] = False


class RevisionOut(BaseModel):
    id: int
    version_id: int
    version_abbreviation: str
    date: Optional[datetime.date] = None
    name: Optional[str] = None
    published: Optional[bool] = False


class VerseText(BaseModel):
    id: Optional[int] = None
    text: str
    verseReference: str
    revision_id: int


class AssessmentType(Enum):
    dummy = 'dummy'
    word_alignment = 'word-alignment'
    sentence_length = 'sentence-length'
    missing_words = 'missing-words'
    semantic_similarity = 'semantic-similarity'
    question_answering = 'question-answering'


class AssessmentIn(BaseModel):
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    
    class Config: 
        use_enum_values = True


class AssessmentOut(BaseModel):
    id: int
    revision_id: int
    reference_id: Optional[int] = None
    type: AssessmentType
    status: Optional[str] = None
    requested_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    
    class Config: 
        use_enum_values = True


# Results model to record in the DB.
class Result(BaseModel):
    id: Optional[int] = None
    assessment_id: int
    vref: str
    source: Optional[str] = None
    target: Optional[str] = None
    score: float
    flag: bool = False
    note: Optional[str] = None
    revision_text: Optional[str] = None
    reference_text: Optional[str] = None
    

class Language(BaseModel):
    iso639: str
    name: str


class Script(BaseModel):
    iso15924: str
    name: str
from pydantic import BaseModel
from enum import Enum
from typing import Union, Optional
from datetime import date


class Version(BaseModel):
    id: Optional[int] = None
    name: str
    isoLanguage: str
    isoScript: str
    abbreviation: str
    rights: Union[str, None] = None
    forwardTranslation: Union[int, None] = None
    backTranslation: Union[int, None] = None
    machineTranslation: bool = False


class Revision(BaseModel):
    id: Optional[int] = None
    date: date
    version_id: int
    name: Optional[str] = None
    published: bool = False


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


class Assessment(BaseModel):
    assessment: Union[int, None] = None
    revision: int
    reference: Union[int, None] = None  # Can be an int or 'null'
    type: AssessmentType

    class Config:  
        use_enum_values = True


class Language(BaseModel):
    iso639: str
    name: str


class Script(BaseModel):
    iso15924: str
    name: str
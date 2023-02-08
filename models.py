from pydantic import BaseModel
from enum import Enum
from typing import Union


class Version(BaseModel):
    name: str
    isoLanguage: str
    isoScript: str
    abbreviation: str
    rights: Union[str, None] = None
    forwardTranslation: Union[int, None] = None
    backTranslation: Union[int, None] = None
    machineTranslation: bool = False


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
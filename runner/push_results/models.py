from pydantic import BaseModel
from typing import List, Optional


# Results model to record in the DB.
class Result(BaseModel):
    assessment_id: int
    vref: str
    score: float
    flag: bool = False
    note: Optional[str] = None

# Results model to record in the DB.
class MissingWord(BaseModel):
    assessment_id: int
    vref: str
    source: str
    score: float
    flag: bool = False
    note: Optional[str] = None


# Results is a list of results to push to the DB
class Results(BaseModel):
    results: List[Result]

# Results is a list of missing words to push to the DB
class MissingWords(BaseModel):
    missing_words: List[MissingWord]

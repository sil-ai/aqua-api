from pydantic import BaseModel
from typing import List, Literal


# The information corresponding to the given assessment.
class Assessment(BaseModel):
    assessment: int
    type: Literal['semantic-similarity']
    revision: int
    reference: int


# Results model to record in the DB.
class Result(BaseModel):
    assessment_id: int
    verse: str
    score: float
    flag: bool
    note: str


# Results is a list of results to push to the DB
class Results(BaseModel):
    results: List[Result]

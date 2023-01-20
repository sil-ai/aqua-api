from pydantic import BaseModel
from typing import List, Optional


# Results model to record in the DB.
class Result(BaseModel):
    assessment_id: int
    vref: str
    score: float
    flag: bool = False
    note: Optional[str] = None


# Results is a list of results to push to the DB
class Results(BaseModel):
    results: List[Result]

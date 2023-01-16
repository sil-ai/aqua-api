from pydantic import BaseModel
from typing import List

# The information needed to run a semantic similarity assessment configuration.
class SemSimConfig(BaseModel):
    draft_revision: int
    reference_revision: int

# The information corresponding to the given assessment.
class SemSimAssessment(BaseModel):
    assessment_id: int
    assessment_type = 'semantic-similarity'
    configuration: SemSimConfig

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
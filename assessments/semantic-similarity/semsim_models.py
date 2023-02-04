from pydantic import BaseModel
from typing import Literal

# The information needed to run a semantic similarity assessment configuration.
class SemSimConfig(BaseModel):
    draft_revision: int
    reference_revision: int
    type: Literal["semantic-similarity"]

# The information corresponding to the given assessment.
class SemSimAssessment(BaseModel):
    assessment_id: int
    #assessment_type = 'semantic-similarity'
    configuration: SemSimConfig

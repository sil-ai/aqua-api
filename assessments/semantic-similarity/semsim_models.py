from pydantic import BaseModel

# The information needed to run a semantic similarity assessment configuration.
class SemSimConfig(BaseModel):
    draft_revision: int
    reference_revision: int

# The information corresponding to the given assessment.
class SemSimAssessment(BaseModel):
    assessment_id: int
    assessment_type = 'semantic-similarity'
    configuration: SemSimConfig

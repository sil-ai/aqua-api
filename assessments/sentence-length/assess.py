from pydantic import BaseModel
from typing import List
import modal
import os

stub = modal.Stub("sentence-length")
stub.pull_revision = modal.Function.from_name("pull_revision", "pull_revision")

# The information needed to run a sentence length assessment configuration.
class SentLengthConfig(BaseModel):
    draft_revision:int

# The information corresponding to the given assessment.
class SentLengthAssessment(BaseModel):
    assessment_id: int
    assessment_type: str
    configuration: SentLengthConfig

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

#run the assessment
#for now, average words per sentence
@stub.function
def assess(assessment: SentLengthAssessment):
    #pull the revision
    rev_num = assessment.configuration.draft_revision
    #lines = pull_revision(rev_num).split('\n')
    lines = modal.container_app.pull_revision.call(rev_num)
    #lines = lines.split('\n')

    #calculate average words per sentence for each line
    results = []
    for line in lines:
        #get score
        words = line.split(' ')
        sentences = line.split('.')
        avg_words = len(words) / len(sentences)

        #if score is above 10, flag
        flag = False
        if avg_words > 10:
            flag = True
        
        #add to results
        results.append(Result(assessment_id=assessment.assessment_id, verse=line, score=avg_words, flag=flag, note=''))

    return Results(results=results)
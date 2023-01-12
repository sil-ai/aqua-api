from pydantic import BaseModel
from typing import List
import modal
import os

stub = modal.Stub("sentence-length")

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

#pull the revision
#returns a string of the vref bible text file
def pull_revision(int: id):
    #function lives at /aqua-api/runner/pull_revision/pull_revision.py
    #for now, return text_bible.txt
    # print(os.getcwd())
    # with open('../../fixtures/test_bible.txt', 'r') as f:
    #     lines = f.read()
    lines = "This is a test\nHello there\nABC\nModal is cool. but is a bit of a pain to figure out."
    return lines

#run the assessment
#for now, average words per sentence
@stub.function
def assess(assessment: SentLengthAssessment):
    #pull the revision
    rev_num = assessment.configuration.draft_revision
    lines = pull_revision(rev_num).split('\n')

    #calculate average words per sentence for each line
    results = []
    for line in lines:
        #get score
        words = line.split(' ')
        sentences = line.split('.')
        avg_words = len(words) / len(sentences)

        #if score is above 10, flag
        if avg_words > 10:
            results.append(Result(assessment_id=assessment.assessment_id, verse=line, score=avg_words, flag=True, note=''))
        else:
            results.append(Result(assessment_id=assessment.assessment_id, verse=line, score=avg_words, flag=False, note=''))

    return Results(results=results)
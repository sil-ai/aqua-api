from pydantic import BaseModel
from typing import List
import modal
import os
import pandas as pd
from pathlib import Path

# Manage suffix on modal endpoint if testing.
suffix = ''
if os.environ.get('MODAL_TEST') == 'TRUE':
    suffix = '_test'


# Define the modal stub.
stub = modal.Stub(
    "sentence-length" + suffix,
    image=modal.Image.debian_slim().pip_install(
        'pydantic',
        'pandas'
    )
    .copy(mount=modal.Mount(local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root"))),
)
#get the pull_revision function
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


#read in vref
def get_vrefs():
    with open('/root/vref.txt', 'r') as f:
        vrefs = f.readlines()
    vrefs = [vref.strip() for vref in vrefs]
    return vrefs

def get_words_per_sentence(text):
    #if text contains only spaces, return 0
    if text.isspace():
        return 0
    #get score
    words = text.split(' ')
    sentences = text.split('.')
    avg_words = len(words) / len(sentences)

    #handle edge case where line is blank or <range>
    if avg_words < 2:
        avg_words = 0

    #round to 2 decimal places
    avg_words = round(avg_words, 2)
    
    return avg_words

#run the assessment
#for now, average words per sentence
@stub.function
def assess(assessment: SentLengthAssessment):
    #pull the revision
    rev_num = assessment.configuration.draft_revision
    lines = modal.container_app.pull_revision.call(rev_num)
    lines = [line.strip() for line in lines]

    #get vrefs
    vrefs = get_vrefs()

    df = pd.DataFrame({'vref': vrefs, 'verse': lines})

    #replace <range> with blank
    df['verse'] = df['verse'].replace('<range>', '')

    #get book, chapter, and verse columns
    df['book'] = df['vref'].str.split(' ').str[0]
    df['chapter'] = df['vref'].str.split(' ').str[1].str.split(':').str[0]
    #df['verse'] = df['vref'].str.split(' ').str[1].str.split(':').str[1]

    #group by book and chapter
    chapter_df = df.groupby(['book', 'chapter']).agg({'verse': ' '.join}).reset_index()

    #calculate average words per sentence for each chapter
    chapter_df['score'] = chapter_df['verse'].apply(get_words_per_sentence)

    #add scores to original df
    #every verse in a chapter will have the same score
    df = df.merge(chapter_df[['book', 'chapter', 'score']], on=['book', 'chapter'])

    #add to results
    results = []
    for index, row in df.iterrows():
        results.append(Result(assessment_id=assessment.assessment_id, verse=row['verse'], score=row['score'], flag=False, note=''))

    return Results(results=results)
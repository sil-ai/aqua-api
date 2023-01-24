from typing import List
import os
from pathlib import Path
import string
from typing import Optional

from pydantic import BaseModel
import modal

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

import sys
sys.path.append('../../../')



# Use Token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    # run api key fetch function requiring 
    # input of AWS credentials
    from key_fetch import get_secret
    
    api_keys = get_secret(
            os.getenv("KEY_VAULT"),
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )
    if api_key not in api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )

    return True

# Manage suffix on modal endpoint if testing.
suffix = ''
if os.environ.get('MODAL_TEST') == 'TRUE':
    suffix = '_test'

# Define the modal stub.
stub = modal.Stub(
    "sentence_length" + suffix,
    image=modal.Image.debian_slim().pip_install(
        'pydantic',
        'pandas',
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
        "sqlalchemy==1.4.36",
    )
    .copy(mount=modal.Mount(local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root"))),
)
#get the pull_revision and push_results functions
stub.run_pull_revision = modal.Function.from_name("pull_revision", "pull_revision")
stub.run_push_results = modal.Function.from_name("push_results_test", "push_results")
stub.run_delete_results = modal.Function.from_name("push_results_test", "delete_results")


# The information needed to run a sentence length assessment configuration.
class SentLengthConfig(BaseModel):
    draft_revision:int


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

    #round to 2 decimal places
    #avg_words = round(avg_words, 2)
    
    return avg_words

def get_long_words(text, n=7):
    #get % of words that are >= n characters
    #if text contains only spaces, return 0
    if text.isspace():
        return 0
    
    #clean text
    text = text.translate(str.maketrans('', '', string.punctuation))

    #get score
    words = text.split(' ')
    long_words = [word for word in words if len(word) > n]
    percent_long_words = float(len(long_words) / len(words))
    return percent_long_words*100

def get_lix_score(text):
    lix = (get_long_words(text)+get_words_per_sentence(text))
    #round
    lix = round(lix, 2)
    return lix

#run the assessment
#for now, use the Lix formula
@stub.function(dependencies=[Depends(api_key_auth)])
def sentence_length(assessment_id: int, configuration: dict):
    import pandas as pd
    assessment_config = SentLengthConfig(**configuration)
    
    #pull the revision
    rev_num = assessment_config.draft_revision
    lines = modal.container_app.run_pull_revision.call(rev_num)
    lines = [line.strip() for line in lines]

    #get vrefs
    vrefs = get_vrefs()

    df = pd.DataFrame({'vref': vrefs, 'verse': lines})

    #replace <range> with blank
    df['verse'] = df['verse'].replace('<range>', '')


    #get book, chapter, and verse columns
    df['book'] = df['vref'].str.split(' ').str[0]
    df['chapter'] = df['vref'].str.split(' ').str[1].str.split(':').str[0]

    #group by book and chapter
    chapter_df = df.groupby(['book', 'chapter']).agg({'verse': ' '.join}).reset_index()

    #calculate lix score for each chapter
    chapter_df['lix_score'] = chapter_df['verse'].apply(get_lix_score)
    chapter_df['wps'] = chapter_df['verse'].apply(get_words_per_sentence)
    chapter_df['percent_long_words'] = chapter_df['verse'].apply(get_long_words)
    
    #add scores to original df
    #every verse in a chapter will have the same score
    df = df.merge(chapter_df[['book', 'chapter', 'lix_score', 'wps', 'percent_long_words']], on=['book', 'chapter'])

    #add to results
    results = []
    for index, row in df.iterrows():
        results.append({'assessment_id': assessment_id, 'vref': row['vref'], 'score': row['lix_score'], 'flag': False})

    print('Pushing results to the database')
    response, ids = modal.container_app.run_push_results.call(results)
    response, ids = modal.container_app.run_delete_results.call(ids)

    return response, results, ids
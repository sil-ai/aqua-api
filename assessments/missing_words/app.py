from pydantic import BaseModel
import modal.aio
import asyncio
import os
from typing import Literal, List, Dict, Optional, Union
import requests
import time
import json
import base64

word_alignment_results_volume = modal.SharedVolume().persist("word_alignment_results")

# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "test"

else:
    suffix = os.getenv("MODAL_SUFFIX", "")

suffix = f"-{suffix}" if len(suffix) > 0 else ""


stub = modal.aio.AioStub(
    "missing-words" + suffix,
    image=modal.Image.debian_slim().pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
        "asyncio",
        "sqlalchemy",
        "psycopg2-binary",
        "requests_toolbelt==0.9.1",
    )
)

stub.get_results = modal.Function.from_name("save-results" + suffix, "get_results")

# The information corresponding to the given assessment.
class Assessment(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: int
    type: Union[Literal["missing-words"], Literal["word-alignment"]]

def get_versions(AQUA_URL: str, AQUA_API_KEY: str) -> List[dict]:
    """
    Gets a list of versions from the AQuA API.
    
    Returns:
    List[dict]: A list of the versions on the AQuA API, with each version being a dict.
    """
    import requests

    url = AQUA_URL + "/version"
    header = {"Authorization": "Bearer" + " " + AQUA_API_KEY}
    response = requests.get(
        url, headers=header
    )
    versions = response.json()
    return versions


async def get_revision_id(version_abbreviation: str, AQUA_URL: str, AQUA_API_KEY: str) -> Dict[str, int]:
    """
    Get the revision id for the most recently uploaded revision for a given version abbreviation.
    
    Parameters:
    version_abbreviation (str): The abbreviation of the version to retrieve the revision id for.
    
    Returns:
    Dict[str, int]: A dictionary with the version abbreviation as key and the revision id as value.
    
    Raises:
    ValueError: If a revision could not be found for the given version abbreviation.
    """
    import requests

    url = AQUA_URL + "/revision"
    header = {"Authorization": "Bearer" + " " + AQUA_API_KEY}
    response = requests.get(
        url, params={"version_abbreviation": version_abbreviation}, headers=header
    )
    print(response.json())
    if len(response.json()) == 0:
        raise ValueError(
            f"Could not find revision for version {version_abbreviation}."
        )
    revision = response.json()[-1]["id"]  # Choose the most recent revision
    return {version_abbreviation: revision}



@stub.function
async def get_top_source_scores(revision: int, reference: int, database_id: str) -> dict:
    """
    Get the top source scores for a revision and reference.
    
    Parameters:
    revision (int): The revision is.
    reference (int): The reference id.
    
    Returns:
    Dict[int, pd.DataFrame]: A dictionary with the revision as key and a pandas DataFrame of the top source scores.
    """
    top_source_scores_df = modal.container_app.get_results.call(
        revision, reference, database_id
    )
    return {revision: top_source_scores_df}


@stub.function(timeout=7200)
async def run_word_alignment(revision_id: int, reference_id: int, AQUA_DB: str, AQUA_URL: str, AQUA_API_KEY: str, via_api: bool=True, modal_suffix:str='') -> dict:
    """
    Requests a word alignment assessment for a given revision and reference from the
    AQuA API. Keeps checking the database every 20 seconds until the assessment is finished.
    When it is finished, it retrieves the top source scores from the modal shared volume
    and returns them.
    
    Parameters:
    revision (int): The id of the revision to run the word alignment assessment for.
    reference (int): The id of the reference to run the word alignment assessment for.
    
    Returns:
    Dict[str, pd.DataFrame]: A dictionary with the revision id as key and a DataFrame of top source scores as value.
    """
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    print(f"Starting word alignment for database {database_id}, revision {revision_id}, reference {reference_id}")

    
    if via_api:
        print("Posting to AQuA API to run word alignment assessment...")
        assessment_config = Assessment(
        reference_id=reference_id,
        revision_id=revision_id,
        type="word-alignment",
        )
        url = AQUA_URL + "/assessment"
        print(url)
        header = {"Authorization": "Bearer " + AQUA_API_KEY}
        requests.post(url, params={**assessment_config.dict(), 'modal_suffix': modal_suffix}, headers=header)
        
        # Keep checking the database until it is finished
        while True:
            response = requests.get(url, headers=header)
            assessments = response.json()

            assessment_list = [
                assessment
                for assessment in assessments
                if assessment["revision_id"] == revision_id
                and assessment["reference_id"] == reference_id
                and assessment["type"] == "word-alignment"
                and assessment["status"] == "finished"
            ]
            if len(assessment_list) > 0:
                print(f"Word alignment for revision {revision_id}, reference {reference_id}, finished")
                time.sleep(20)  # Give time for the scores to be written to the shared volume
                break
            time.sleep(20)  # Wait and check again in 20 seconds

        top_source_scores_dict = await get_top_source_scores.call(revision_id, reference_id, database_id)
        print(f'{top_source_scores_dict=}')
        return top_source_scores_dict
    
    else:
        # It's helpful to have a method that bypasses the API and doesn't post to the database
        # for testing purposes, particularly when we change the API, and the deployed
        # API is not compatible with the current version of the modal.
        print("Running word alignment assessment without posting to AQuA API...")
        assessment_config = Assessment(
        id=1,
        reference_id=reference_id,
        revision_id=revision_id,
        type="word-alignment",
        )
        suffix = f'-{modal_suffix}' if len(modal_suffix) > 0 else ''
        runner_url = f"https://sil-ai--runner{suffix}-assessment-runner.modal.run/"
        AQUA_DB_BYTES = AQUA_DB.encode('utf-8')
        AQUA_DB_ENCODED = base64.b64encode(AQUA_DB_BYTES)
        params = {
            'AQUA_DB_ENCODED': AQUA_DB_ENCODED,
            'modal_suffix': modal_suffix,
            }
        response = requests.post(runner_url, params=params, json=assessment_config.dict())
        while True:
            top_source_scores_dict = await get_top_source_scores.call(revision_id, reference_id, database_id)
            if top_source_scores_dict[revision_id] is not None:
                print(f'{top_source_scores_dict=}')
                return top_source_scores_dict
            time.sleep(20)  # Wait and check again in 20 seconds


@stub.function(timeout=3600)
def identify_low_scores(
            revision: int, 
            top_source_scores, 
            ref_top_source_scores_dict: dict,
             threshold: float=0.1,
             ):
    """
    Identifies low scores, which are source words that score low in the target language 
    alignment data. For those source words which generally score high in the
    reference top scores, the flag is set to "True".
    
    Parameters:
    revision (int): The revision id.
    top_source_scores (DataFrame): A DataFrame with the top scores for each source word 
        for the target translation.
    ref_top_source_scores_dict (dict): A dictionary of dataframes of the top scores for
        each source word across the reference translations.
    threshold (float): A float for the score below which a match will be considered a 
        possible red flag. Default value is 0.1.
    
    Returns:
    low_scores (DataFrame): A DataFrame with low scores for source-target alignments, 
        with a boolean flag if those scores are high in the reference languages.
    """
    import pandas as pd
    ref_top_source_scores = pd.DataFrame(columns=["vref", "source"])
    for reference, df in ref_top_source_scores_dict.items():
        print(df.head(20))
        print(df.dtypes)
        ref_top_source_scores = ref_top_source_scores.merge(df[['vref', 'source', 'target', 'total_score']], how='outer', on=['vref', 'source'])
        ref_top_source_scores = ref_top_source_scores.rename(columns={'total_score': f'{reference}_score', 'target': f'{reference}_match'})
    print(ref_top_source_scores.head(20))
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].apply(lambda x: max(x, 0))
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].fillna(0)
    low_scores = top_source_scores[top_source_scores['total_score'] < threshold]
    low_scores = low_scores[['vref', 'source', 'total_score']]
    
    if revision in ref_top_source_scores.columns:
        ref_top_source_scores = ref_top_source_scores.drop([revision], axis=1)

    references  = [col for col in ref_top_source_scores.columns if col[-6:] == '_score']
    if len(references) > 0:
        ref_top_source_scores['mean'] = ref_top_source_scores.loc[:, references].mean(axis=1, skipna=True)
        ref_top_source_scores['min'] = ref_top_source_scores.loc[:, references].min(axis=1, skipna=True)
    
    else:
        return low_scores
    
    low_scores = low_scores.merge(ref_top_source_scores, how='left', on=['vref', 'source'], sort=False)
    low_scores.loc[:, 'flag'] = low_scores.apply(lambda row: row['mean'] > 5 * row['total_score'] and row['mean'] > 0.35, axis=1)
    
    print(low_scores.head(20))
    return low_scores


@stub.function(
    timeout=7200,
    secret=modal.Secret.from_name("aqua-api"),
    )
async def assess(assessment_config: Assessment, AQUA_DB: str, refresh_refs: bool=False, via_api: bool=True, modal_suffix: str=''):
    """
    Assess the words from the reference text that are missing in the revision.

    Inputs:
        assessment_config (Assessment): An object representing the configuration of the 
            missing words assessment. 
        push_to_db (bool, optional): A flag indicating whether the results should be 
            pushed to the database (defaults to True). Should be set to false for testing,
            when there is no assessment in the database to log to.

    Outputs:
        dict: A dictionary containing the status of the operation and a list of ids
            for the missing words in the assessmentMissingWords table of the database.
    """
    reference_id = assessment_config.reference_id
    revision_id = assessment_config.revision_id
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    AQUA_URL = os.getenv(f"AQUA_URL_{database_id.replace('-', '_')}")
    AQUA_API_KEY = os.getenv(f"AQUA_API_KEY_{database_id.replace('-', '_')}")
    versions = get_versions(AQUA_URL, AQUA_API_KEY)

    # Baseline versions are those that begin with system_ and will be used for comparison
    baseline_versions = [version for version in versions if len(version['abbreviation']) >= 7 and version['abbreviation'][:7] == 'system_']

    print(f'{baseline_versions=}')

    baseline_revisions = await asyncio.gather(
        *[get_revision_id(version['abbreviation'], AQUA_URL, AQUA_API_KEY) for version in baseline_versions]
    )
    baseline_revision_ids = [
        list(revision.values())[0] for revision in baseline_revisions
    ]

    all_top_source_scores = {}

    if refresh_refs:
        assessments_to_run = [revision_id, *baseline_revision_ids]
        print(assessments_to_run)

    else: 
        #   Get previous word alignments asynchronously
        results = await asyncio.gather(*[get_top_source_scores.call(revision, reference_id, database_id) for revision in [revision_id, *baseline_revision_ids]])

        for result in results:
            all_top_source_scores = {**all_top_source_scores, **result}

        assessments_to_run = [revision for revision, df in all_top_source_scores.items() if df is None]
    print(assessments_to_run)
    results = await asyncio.gather(*[run_word_alignment.call(revision, reference_id, AQUA_DB, AQUA_URL, AQUA_API_KEY, via_api=via_api, modal_suffix=modal_suffix) for revision in assessments_to_run])

    for result in results:
        all_top_source_scores.update(result)

    print(all_top_source_scores)
    revision_top_source_scores_df = all_top_source_scores.pop(revision_id)
    
    low_scores = await identify_low_scores.call(revision_id, revision_top_source_scores_df, all_top_source_scores)
    
    missing_words = []
    low_scores = low_scores.fillna({col: '' for col in low_scores.columns if col[-6:] == '_match'})  # Necessary for blank verses, or NaN gives json database error
    low_scores = low_scores.fillna({col: 0 for col in low_scores.columns if col[-6:] == '_score'})  # Necessary for blank verses, or NaN gives json database error

    for _, row in low_scores.iterrows():
        reference_matches = {col[:-6]: row[col] for col in row.index if col[-6:] == '_match'}
        reference_matches_json = json.dumps(reference_matches, ensure_ascii=False)
        
        missing_words.append({'assessment_id': assessment_config.id, 
                        'vref': row['vref'], 'source': row['source'], 'target': reference_matches_json, 'score': row['total_score'], 
                        'flag': row['flag']})

    return missing_words
from pydantic import BaseModel
import modal.aio
import asyncio
import os
from typing import Literal, List, Dict
import requests
import time

word_alignment_results_volume = modal.SharedVolume().persist("word_alignment_results")

# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "-test"

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

stub.run_push_results = modal.Function.from_name("push_results", "push_results")

stub.get_results = modal.Function.from_name(
    "save-results", "get_results"
)


# The information corresponding to the given assessment.
class Assessment(BaseModel):
    assessment: int
    revision: int
    reference: int
    type: Literal["missing-words"]


stub.run_push_missing_words = modal.Function.from_name("push_results", "push_missing_words")


# @stub.function
def get_versions() -> List[dict]:
    """
    Gets a list of versions from the AQuA API.
    
    Returns:
    List[dict]: A list of the versions on the AQuA API, with each version being a dict.
    """
    import requests

    url = os.getenv("AQUA_URL") + "/version"
    header = {"Authorization": "Bearer" + " " + str(os.getenv("TEST_KEY"))}
    response = requests.get(
        url, headers=header
    )
    versions = response.json()
    return versions


# @stub.function
async def get_revision_id(version_abbreviation: str) -> Dict[str, int]:
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

    url = os.getenv("AQUA_URL") + "/revision"
    header = {"Authorization": "Bearer" + " " + str(os.getenv("TEST_KEY"))}
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
async def get_top_source_scores(revision: int, reference: int) -> dict:
    """
    Get the top source scores for a revision and reference.
    
    Parameters:
    revision (int): The revision is.
    reference (int): The reference id.
    
    Returns:
    Dict[int, pd.DataFrame]: A dictionary with the revision as key and a pandas DataFrame of the top source scores.
    """
    top_source_scores_df = modal.container_app.get_results.call(
        revision, reference
    )
    return {revision: top_source_scores_df}


@stub.function(secret=modal.Secret.from_name("aqua-api"),timeout=7200)
async def run_word_alignment(revision: int, reference: int) -> dict:
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
    
    print(f"Starting word alignment for revision {revision}, reference {reference}")

    assessment_config = {
        "reference": reference,
        "revision": revision,
        "type": "word-alignment",
    }
    url = os.getenv("AQUA_URL") + "/assessment"
    header = {"Authorization": "Bearer " + os.getenv("TEST_KEY")}
    requests.post(url, json=assessment_config, headers=header)
    
    # Keep checking the database until it is finished
    while True:
        response = requests.get(url, headers=header)
        assessments = response.json()["assessments"]

        assessment_list = [
            assessment
            for assessment in assessments
            if assessment["revision"] == revision
            and assessment["reference"] == reference
            and assessment["type"] == "word-alignment"
            and assessment["status"] == "finished"
        ]
        if len(assessment_list) > 0:
            print(f"Word alignment for revision {revision}, reference {reference}, finished")
            time.sleep(20)  # Give time for the scores to be written to the shared volume
            break
        time.sleep(20)  # Wait and check again in 20 seconds

    top_source_scores_dict = await get_top_source_scores.call(revision, reference)
    print(f'{top_source_scores_dict=}')
    return top_source_scores_dict

@stub.function
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
        ref_top_source_scores = ref_top_source_scores.merge(df[['vref', 'source', 'total_score']], how='outer', on=['vref', 'source'])
        ref_top_source_scores = ref_top_source_scores.rename(columns={'total_score': reference})
    print(ref_top_source_scores.head(20))
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].apply(lambda x: max(x, 0))
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].fillna(0)
    low_scores = top_source_scores[top_source_scores['total_score'] < threshold]
    low_scores = low_scores[['vref', 'source', 'total_score']]
    
    if revision in ref_top_source_scores.columns:
        ref_top_source_scores = ref_top_source_scores.drop([revision], axis=1)

    references  = [col for col in ref_top_source_scores.columns if col not in ['vref', 'source']]
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
    secret=modal.Secret.from_name("aqua-api"),
    timeout=7200,
)
async def assess(assessment_config: Assessment, push_to_db: bool = True):
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

    reference = assessment_config.reference
    revision = assessment_config.revision

    versions = get_versions()

    # Baseline versions are those that begin with system_ and will be used for comparison
    baseline_versions = [version for version in versions if len(version['abbreviation']) >= 7 and version['abbreviation'][:7] == 'system_']

    print(f'{baseline_versions=}')

    baseline_revisions = await asyncio.gather(
        *[get_revision_id(version['abbreviation']) for version in baseline_versions]
    )
    baseline_revision_ids = [
        list(revision.values())[0] for revision in baseline_revisions
    ]

    #Run these revisions asynchronously
    results = await asyncio.gather(*[get_top_source_scores.call(revision, reference) for revision in [revision, *baseline_revision_ids]])

    all_top_source_scores = {}
    for result in results:
        all_top_source_scores = {**all_top_source_scores, **result}

    assessments_to_run = [revision for revision, df in all_top_source_scores.items() if df is None]
    print(assessments_to_run)
    results = await asyncio.gather(*[run_word_alignment.call(revision, reference) for revision in assessments_to_run])

    for result in results:
        all_top_source_scores.update(result)

    print(all_top_source_scores)
    revision_top_source_scores_df = all_top_source_scores.pop(revision)
    
    low_scores = await identify_low_scores.call(revision, revision_top_source_scores_df, all_top_source_scores)
    
    missing_words = []
    for _, row in low_scores.iterrows():
        missing_words.append({'assessment_id': assessment_config.assessment, 
                        'vref': row['vref'], 'source': row['source'], 'score': row['total_score'], 
                        'flag': row['flag']})

    if not push_to_db:
        return {'status': 'finished (not pushed to database)', 'ids': []}
    
    print('Pushing results to the database')
    response, ids = modal.container_app.run_push_missing_words.call(missing_words)
    print(f"Finished pushing to the database. Response: {response}")
    return {'status': 'finished', 'ids': ids}
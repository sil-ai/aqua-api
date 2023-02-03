from pydantic import BaseModel
import modal.aio
import asyncio
from pathlib import Path
import json
import os
import pickle
from typing import Literal
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
    # .copy(
    #     mount=modal.Mount(
    #         local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root")
    #     )
    # )
)

stub.run_push_results = modal.Function.from_name("push_results", "push_results")

stub.get_results = modal.Function.from_name(
    "save-results", "get_results"
)


@stub.function
async def get_top_source_scores(revision, reference):
    top_source_scores_df = modal.container_app.get_results.call(
        revision, reference
    )
    return {revision: top_source_scores_df}


# The information corresponding to the given assessment.
class Assessment(BaseModel):
    assessment: int
    revision: int
    reference: int
    type: Literal["missing-words"]


stub.function


async def get_revision_id(version_abbreviation: str):
    import requests

    url = os.getenv("AQUA_URL") + "/revision"
    header = {"Authorization": "Bearer" + " " + str(os.getenv("TEST_KEY"))}
    response = requests.get(
        url, params={"version_abbreviation": version_abbreviation}, headers=header
    )
    revision_id = response.json()[0]["id"]
    return {version_abbreviation: revision_id}


async def run_word_alignment(revision, reference):
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
            break
        time.sleep(20)
    top_source_scores_df = get_top_source_scores.call(revision, reference)

    return {revision: top_source_scores_df}


def identify_red_flags(
            revision: int, top_source_scores, ref_top_source_scores: dict, threshold: float=0.1):
    """
    Takes the directory of the source-target outputs, and a dictionary of reference language to reference language source-target outputs.
    Returns "red flags", which are source words that score low in the target language alignment data, compared to how they
    score in the source - reference language data.
    Inputs:
    target_str              String of the current target language
    top_source_scores         A dataframe with the top scores for each source word for the target translation
    ref_top_source_scores        A dataframe with a summary of the top scores for each source word across all translations
    threshold               A float for the score below which a match will be considered a possible red flag

    Outputs:
    possible_red_flags      A dataframe with low scores for source-target alignments
    red_flags               A dataframe with low scores for source-target alignments, when those same source words score highly in that
                            context in the reference languages.
    """
    
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].apply(lambda x: max(x, 0))
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].fillna(0)
    possible_red_flags = top_source_scores[top_source_scores['total_score'] < threshold]
    possible_red_flags = possible_red_flags[['vref', 'source', 'total_score']]
    
    if revision in ref_top_source_scores.columns:
        ref_top_source_scores = ref_top_source_scores.drop([revision], axis=1)

    references  = [col for col in ref_top_source_scores.columns if col not in ['vref', 'source']]
    if len(references) > 0:
        ref_top_source_scores['mean'] = ref_top_source_scores.loc[:, references].mean(axis=1)
        ref_top_source_scores['min'] = ref_top_source_scores.loc[:, references].min(axis=1)
    elif len(references) > 1:
        ref_top_source_scores['second_min'] = ref_top_source_scores.loc[:, references].apply(lambda row: sorted(list(row))[1], axis=1)
    else:
        return possible_red_flags, possible_red_flags
    
    possible_red_flags = possible_red_flags.merge(ref_top_source_scores, how='left', on=['vref', 'source'], sort=False)
    red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['mean'] > 5 * row['total_score'] and row['mean'] > 0.35, axis=1)]
    
    return possible_red_flags, red_flags


@stub.function(
    secret=modal.Secret.from_name("aqua-api"),
)
async def assess(assessment_config: Assessment, push_to_db: bool = True):
    reference = assessment_config.reference
    revision = assessment_config.revision

    baseline_versions = [
        "en-NASB",
        # 'es-RVR1960',
        "swh-ONEN",
        # 'arb-AVD',
        # 'ko-RNKSV',
    ]

    baseline_revisions = await asyncio.gather(
        *[get_revision_id(version) for version in baseline_versions]
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

    results = await asyncio.gather(*[run_word_alignment.call(revision, reference) for revision in assessments_to_run])

    for result in results:
        all_top_source_scores.update(result)

    revision_top_source_scores_df = all_top_source_scores.pop(revision)

    identify_red_flags.call(revision, revision_top_source_scores_df, all_top_source_scores)

    return all_top_source_scores

    # print('Pushing results to the database')
    # df = total_results['verse_scores']
    # if not push_to_db:
    #     return {'status': 'finished (not pushed to database)', 'ids': []}

    # results = []
    # for _, row in df.iterrows():
    #     results.append({'assessment_id': assessment_config.assessment, 'vref': row['vref'], 'score': row['total_score'], 'flag': False})

    # response, ids = modal.container_app.run_push_results.call(results)

    # return {'status': 'finished', 'ids': ids}

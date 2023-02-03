from pydantic import BaseModel
import modal.aio
import asyncio
from pathlib import Path
import json
import os
import pickle
from typing import Literal


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

stub.get_word_alignment_results = modal.Function.from_name(
    "word-alignment-test", "get_results"
)


@stub.function
async def get_top_source_scores(revision, reference):
    top_source_scores_df = modal.container_app.get_word_alignment_results.call(
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
    all_top_source_scores = await asyncio.gather(*[get_top_source_scores.call(revision, reference) for revision in [revision, *baseline_revision_ids]])

    # for revision in revisions_to_run:
    #     print(revision)
    #     assessment_id = modal.container_app.get_word_alignment_results.call(revision, reference)
    #     completed_assessments[revision] = assessment_id
    # top_source_scores_df = get_top_source_scores(revision, reference)
    import pandas as pd

    top_source_scores_df = pd.DataFrame()

    return all_top_source_scores, top_source_scores_df

    # print('Pushing results to the database')
    # df = total_results['verse_scores']
    # if not push_to_db:
    #     return {'status': 'finished (not pushed to database)', 'ids': []}

    # results = []
    # for _, row in df.iterrows():
    #     results.append({'assessment_id': assessment_config.assessment, 'vref': row['vref'], 'score': row['total_score'], 'flag': False})

    # response, ids = modal.container_app.run_push_results.call(results)

    # return {'status': 'finished', 'ids': ids}

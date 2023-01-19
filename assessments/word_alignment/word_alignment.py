from pydantic import BaseModel
from typing import List
from word_alignment_steps import prepare_data, create_cache, alignment_scores, translation_scores, match_scores
import modal.aio
import asyncio
from pathlib import Path
import json
import pandas as pd

index_cache_volume = modal.SharedVolume().persist("index_cache")

stub = modal.aio.AioStub(
    "word_alignment",
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
    )
    .copy(
        mount=modal.Mount(
            local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root")
        )
    ),
)

stub.run_pull_rev = modal.Function.from_name("pull_revision", "pull_revision")

CACHE_DIR = Path("/cache")

# The information needed to run an alignment configuration.
class AlignmentConfig(BaseModel):
    draft_revision: int
    reference_revisions: List[int]


# The information corresponding to the given assessment.
class AlignmentAssessment(BaseModel):
    assessment_id: int
    assessment_type: str
    configuration: AlignmentConfig


async def create_index_cache(tokenized_df, refresh: bool = False):
    index_cache = create_cache.create_index_cache(tokenized_df)
    # index_cache = {word.word: word.index_list for word in word_dict.values()}

    return index_cache


@stub.function(shared_volumes={CACHE_DIR: index_cache_volume})
async def get_index_cache(revision_id, refresh: bool = False):
    tokenized_df = await get_tokenized_df.call(revision_id)
    CACHE_DIR.mkdir(exist_ok=True)
    index_cache_file = Path(f"{CACHE_DIR}/{revision_id}-index-cache.json")
    if index_cache_file.exists() and not refresh:
        with open(index_cache_file) as f:
            index_cache = json.load(f)
    else:
        index_cache = await create_index_cache(tokenized_df, refresh=refresh)
        with open(index_cache_file, "w") as f:
            json.dump(index_cache, f, indent=4)
    return revision_id, index_cache, tokenized_df


@stub.function
async def get_text(revision_id: int) -> bytes:
    return modal.container_app.run_pull_rev.call(revision_id)


@stub.function
async def get_tokenized_df(revision_id: int):
    vref_filepath = Path("/root/vref.txt")
    src_data = await get_text.call(revision_id)
    df = prepare_data.create_tokens(src_data, vref_filepath)
    return df


@stub.function
async def run_alignment_scores(src_revision_id, condensed_df):
    alignment_scores_df, avg_alignment_scores_df = alignment_scores.run_alignment_scores(condensed_df)
    return {src_revision_id: {'alignment_scores': alignment_scores_df, 'avg_alignment_scores': avg_alignment_scores_df}}

@stub.function
async def run_translation_scores(src_revision_id, condensed_df):
    translation_scores_df = translation_scores.run_translation_scores(condensed_df)
    return {src_revision_id: {'translation_scores': translation_scores_df}}


@stub.function
async def run_match_scores(src_revision_id, condensed_df, src_index_cache, target_index_cache):
    match_scores_df = match_scores.run_match_scores(condensed_df, src_index_cache, target_index_cache)
    return {src_revision_id: {'match_scores': match_scores_df}}


@stub.function
async def create_condensed_df(src_tokenized_df, trg_tokenized_df, src_revision_id):
    combined_df = src_tokenized_df.join(
        trg_tokenized_df.drop(["vref"], axis=1).rename(
            columns={"src_tokenized": "trg_tokenized"}
        ),
        how="inner",
    )
    condensed_df = prepare_data.condense_df(combined_df)

    return src_revision_id, condensed_df


async def word_alignment(assessment: AlignmentAssessment, refresh: bool = False):
    async with stub.run():
        tokenized_dfs = {}
        combined_dfs = {}
        condensed_dfs = {}
        index_caches = {}
        src_revision_ids = assessment.configuration.reference_revisions
        trg_revision_id = assessment.configuration.draft_revision
        results = await asyncio.gather(
            *[
                get_index_cache.call(revision_id, refresh=refresh)
                for revision_id in set([*src_revision_ids, trg_revision_id])
            ]
        )

        for revision_id, index_cache, df in results:
            tokenized_dfs[revision_id] = df
            index_caches[revision_id] = index_cache

        results = await asyncio.gather(
            *[
                create_condensed_df.call(
                    tokenized_dfs[src_revision_id], tokenized_dfs[trg_revision_id], src_revision_id
                )
                for src_revision_id in src_revision_ids
            ]
        )

        for src_revision_id, condensed_df in results:
            print(condensed_df.columns)
            condensed_dfs[src_revision_id] = condensed_df
        
        results = await asyncio.gather(
            *[run_alignment_scores.call(src_revision_id, condensed_df) for src_revision_id, condensed_df in condensed_dfs.items()],
            *[run_translation_scores.call(src_revision_id, condensed_df) for src_revision_id, condensed_df in condensed_dfs.items()],
            *[run_match_scores.call(src_revision_id, condensed_df, index_caches[src_revision_id], index_caches[trg_revision_id]) for src_revision_id, condensed_df in condensed_dfs.items()],

        )
        print(results)
        combined_results = {}
        for item in results:
            for key, value in item.items():
                if key in combined_results:
                    combined_results[key].update(value)
                else:
                    combined_results[key] = value
        print(combined_results)
        

    #     alignment_scores_df, avg_alignment_scores_df = results[0]
    #     translation_scores_df = results[1]
    #     print(alignment_scores_df.head(50))
    #     print(avg_alignment_scores_df.head(50))
    #     print(translation_scores_df.head(50))
    # return results


if __name__ == "__main__":
    assessment = AlignmentAssessment(
        assessment_id=1,
        assessment_type="word_alignment",
        configuration=AlignmentConfig(
            draft_revision=30,
            reference_revisions=[10],
        ),
    )
    asyncio.run(word_alignment(assessment, refresh=False))

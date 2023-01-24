from pydantic import BaseModel
from typing import List
from word_alignment_steps import prepare_data, create_cache, alignment_scores, translation_scores, match_scores, embeddings, total_scores
import modal.aio
import asyncio
from pathlib import Path
import json
import os
from typing import Optional

index_cache_volume = modal.SharedVolume().persist("index_cache")

# Manage suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "_test"

stub = modal.aio.AioStub(
    "word_alignment" + suffix,
    image=modal.Image.debian_slim()
    .pip_install(
        "pandas==1.4.3",
        "machine==0.0.1",
        "sil-machine[thot]>=0.8.3",
        "asyncio"
    )
    .copy(
        mount=modal.Mount(
            local_file=Path("../../fixtures/vref.txt"), remote_dir=Path("/root")
        )
    )
    .copy(
        mount=modal.Mount(
            local_file=Path("data/models/encoder_weights.txt"), remote_dir=Path("/root")
        )
    ),
)

stub.run_pull_rev = modal.Function.from_name("pull_revision", "pull_revision")
stub.run_push_results = modal.Function.from_name("push_results", "push_results")


CACHE_DIR = Path("/cache")

# The information needed to run an alignment configuration.
class WordAlignmentConfig(BaseModel):
    revision: int
    reference: Optional[int]


# The information corresponding to the given assessment.
class WordAlignmentAssessment(BaseModel):
    assessment_id: int
    assessment_type: str
    configuration: WordAlignmentConfig


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
async def run_alignment_scores(condensed_df):
    alignment_scores_df, avg_alignment_scores_df = alignment_scores.run_alignment_scores(condensed_df)
    return {'alignment_scores': alignment_scores_df, 'avg_alignment_scores': avg_alignment_scores_df}

@stub.function
async def run_translation_scores(condensed_df):
    translation_scores_df = translation_scores.run_translation_scores(condensed_df)
    return {'translation_scores': translation_scores_df}


@stub.function
async def run_match_scores(condensed_df, src_index_cache, target_index_cache):
    match_scores_df = match_scores.run_match_scores(condensed_df, src_index_cache, target_index_cache)
    return {'match_scores': match_scores_df}

@stub.function
async def run_embedding_scores(condensed_df, src_index_cache, target_index_cache):
    embedding_scores_df = embeddings.run_embeddings(condensed_df, src_index_cache, target_index_cache)
    return {'embedding_scores': embedding_scores_df}

# @stub.function
def run_total_scores(condensed_df, alignment_scores_df, avg_alignment_scores_df, translation_scores_df, match_scores_df, embedding_scores_df):
    total_scores_df, top_source_scores_df, verse_scores_df = total_scores.run_total_scores(condensed_df, alignment_scores_df, avg_alignment_scores_df, translation_scores_df, match_scores_df, embedding_scores_df)
    return {'total_scores': total_scores_df, 'top_source_scores': top_source_scores_df, 'verse_scores': verse_scores_df}

# @stub.function
def create_condensed_df(src_tokenized_df, trg_tokenized_df):
    combined_df = src_tokenized_df.join(
        trg_tokenized_df.drop(["vref"], axis=1).rename(
            columns={"src_tokenized": "trg_tokenized"}
        ),
        how="inner",
    )
    condensed_df = prepare_data.condense_df(combined_df)

    return condensed_df


async def func1():
    await asyncio.sleep(1)
    print("Function 1 done!")

async def func2():
    await asyncio.sleep(2)
    print("Function 2 done!")

# @stub.function
async def run_word_alignment(assessment_id: int, configuration: dict):
    # async with stub.run():
    assessment_config = WordAlignmentConfig(**configuration)
    tokenized_dfs = {}
    index_caches = {}
    src_revision_id = assessment_config.reference
    trg_revision_id = assessment_config.revision
    results = await asyncio.gather(
        *[
            get_index_cache.call(revision_id, refresh=False)
            for revision_id in [src_revision_id, trg_revision_id]
        ]
    )

    for revision_id, index_cache, df in results:
        tokenized_dfs[revision_id] = df
        index_caches[revision_id] = index_cache

    print(type(tokenized_dfs[src_revision_id]))
    condensed_df = create_condensed_df(
                tokenized_dfs[src_revision_id], tokenized_dfs[trg_revision_id]
            )
    print(type(condensed_df))
    # results = await asyncio.gather(*[run_alignment_scores.call(condensed_df), run_translation_scores.call(condensed_df),
    #     run_match_scores.call(condensed_df, index_caches[src_revision_id], index_caches[trg_revision_id]),
    #     run_embedding_scores.call(condensed_df, index_caches[src_revision_id], index_caches[trg_revision_id]),
    # ]
    # )
    # await asyncio.gather(func1(), func2())
    results = await asyncio.gather(*[
                run_alignment_scores(condensed_df), 
                run_translation_scores(condensed_df),
                run_match_scores(condensed_df, index_caches[src_revision_id], index_caches[trg_revision_id]), 
                run_embedding_scores(condensed_df, index_caches[src_revision_id], index_caches[trg_revision_id]), 
                ])

    step_results = {}
    for item in results:
        for key, value in item.items():
            step_results[key] = value
    print(step_results)
    
    results = run_total_scores(
                        condensed_df, 
                        step_results['alignment_scores'],
                        step_results['avg_alignment_scores'],
                        step_results['translation_scores'],
                        step_results['match_scores'],
                        step_results['embedding_scores'],
                        )


# return results
    print('Pushing results to the database')
    df = results['verse_scores']
    results = []
    for _, row in df.iterrows():
        results.append({'assessment_id': assessment_id, 'vref': row['vref'], 'score': row['total_score'], 'flag': False})

    response, ids = modal.container_app.run_push_results.call(results)
    
    return ids


@stub.function(
    timeout=7200,
)
def word_alignment(assessment_id: int, configuration: dict):
    # with stub.run():
    import modal.aio
    import asyncio
    print('starting word alignment')
    ids = asyncio.run(run_word_alignment(assessment_id, configuration))
    return 200, ids

# # @stub.function
# def main():
#     # with stub.run():
#     assessment = WordAlignmentAssessment(
#         assessment_id=1,
#         assessment_type="word_alignment",
#         configuration={
#             'revision': 138,
#             'reference': 10,
#         },
#     )
#     # configuration = {
#     #     'revision': 138,
#     #     'reference': 10,
#     # }
#     word_alignment.call(assessment.assessment_id, assessment.configuration)

# if __name__ == "__main__":
#     main()

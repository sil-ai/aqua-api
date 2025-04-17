import asyncio
import json
import os
import pickle
from pathlib import Path
from typing import List, Literal, Optional, Union

import modal
import word_alignment_steps.prepare_data as prepare_data
from pandas.core.series import Series
from pydantic import BaseModel

index_cache_volume = modal.NetworkFileSystem.from_name(
    "index_cache", create_if_missing=True
)
machine_model_cache_volume = modal.Volume.from_name(
    "model_cache", create_if_missing=True
)


# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix += "-test"


image_envs = {k: v for k, v in os.environ.items() if k.startswith("MODAL_")}

app = modal.App(
    "word-alignment" + suffix,
    image=modal.Image.debian_slim()
    .pip_install(
        "boto3~=1.28.0",
        "pandas~=1.5.0",
        "psycopg2-binary~=2.9.0",
        "sil-machine~=0.9.0",
        "sil-thot~=3.4.0",
        "sqlalchemy~=1.4.0",
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("../../fixtures/vref.txt"),
            remote_path=Path("/root/vref.txt"),
        )
    )
    .copy_mount(
        mount=modal.Mount.from_local_file(
            local_path=Path("data/models/encoder_weights_whole_bible.txt"),
            remote_path=Path("/root/encoder_weights_whole_bible.txt"),
        )
    )
    .env(image_envs),
)

run_pull_rev = modal.Function.lookup(f"pull-revision{suffix}", "pull_revision")
run_save_results = modal.Function.lookup(f"save-results{suffix}", "save_results")


CACHE_DIR = "/cache"


# The information corresponding to the given assessment.
class Assessment(BaseModel):
    id: Optional[int] = None
    revision_id: int
    reference_id: int
    type: Literal["word-alignment"]


async def create_index_cache(tokenized_df, refresh: bool = False):
    from word_alignment_steps import create_cache

    index_cache = await create_cache.create_index_cache(tokenized_df)

    return index_cache


@app.function(cpu=1)
def get_indices(
    words: List[prepare_data.Word], word_series: Series
) -> List[prepare_data.Word]:
    for word in words:
        word.get_indices(word_series)
    return words


@app.function(cpu=2, volumes={"/model_cache": machine_model_cache_volume}, timeout=7200)
def get_translation_scores(corpus_tuples: list[tuple], model_id: str) -> dict:
    from word_alignment_steps.train_fa_model import create_model

    model = create_model(model_id)
    data = {"vref": [], "source": [], "target": [], "translation_score": []}
    for source_verse, target_verse, vref in corpus_tuples:
        for word1 in set(source_verse):
            for word2 in target_verse:
                data["source"].append(word1)
                data["target"].append(word2)
                data["translation_score"].append(
                    model.get_translation_score(word1, word2)
                )
                data["vref"].append(vref)
    return data


@app.function(network_file_systems={CACHE_DIR: index_cache_volume}, timeout=7200)
async def get_index_cache(revision_id, AQUA_DB, refresh: bool = False):
    tokenized_df = await get_tokenized_df.remote.aio(revision_id, AQUA_DB)
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    index_cache_file = Path(f"{CACHE_DIR}/{database_id}/{revision_id}-index-cache.json")
    (index_cache_file.parent).mkdir(parents=True, exist_ok=True)
    if index_cache_file.exists() and not refresh:
        with open(index_cache_file) as f:
            try:
                index_cache = json.load(f)
            except json.decoder.JSONDecodeError:
                index_cache = await create_index_cache(tokenized_df, refresh=refresh)
                with open(index_cache_file, "w") as f:
                    json.dump(index_cache, f, indent=4)
    else:
        index_cache = await create_index_cache(tokenized_df, refresh=refresh)
        with open(index_cache_file, "w") as f:
            json.dump(index_cache, f, indent=4)

    return revision_id, index_cache, tokenized_df


@app.function()
async def get_text(revision_id: int, AQUA_DB: str) -> bytes:
    return run_pull_rev.remote(revision_id, AQUA_DB)


@app.function()
async def get_tokenized_df(revision_id: int, AQUA_DB: str):
    vref_filepath = Path("/root/vref.txt")
    src_data = await get_text.remote.aio(revision_id, AQUA_DB)
    df = pickle.loads(prepare_data.create_tokens(src_data, vref_filepath))
    return df


def create_condensed_df(src_tokenized_df, trg_tokenized_df):
    combined_df = src_tokenized_df.join(
        trg_tokenized_df.drop(["vref"], axis=1).rename(
            columns={"src_tokenized": "trg_tokenized", "src_list": "trg_list"}
        ),
        how="inner",
    )
    combined_df_pkl = pickle.dumps(combined_df)
    condensed_df = pickle.loads(prepare_data.condense_df(combined_df_pkl))

    return condensed_df


@app.function(cpu=1, timeout=3600)
async def run_alignment_scores(condensed_df):
    from word_alignment_steps import alignment_scores

    (
        alignment_scores_df,
        avg_alignment_scores_df,
    ) = alignment_scores.run_alignment_scores(condensed_df)
    return {
        "alignment_scores": alignment_scores_df,
        "avg_alignment_scores": avg_alignment_scores_df,
    }


@app.function(
    cpu=2,
    volumes={"/model_cache": machine_model_cache_volume},
    timeout=3600,
    # _allow_background_volume_commits=True,
)
async def run_translation_scores(condensed_df):
    from word_alignment_steps import translation_scores

    translation_scores_df = await translation_scores.run_translation_scores(
        condensed_df, volume=machine_model_cache_volume
    )
    return {"translation_scores": translation_scores_df}


@app.function(cpu=1, timeout=3600)
async def run_match_scores(condensed_df, src_index_cache, target_index_cache):
    from word_alignment_steps import match_scores

    match_scores_df = match_scores.run_match_scores(
        condensed_df, src_index_cache, target_index_cache
    )
    return {"match_scores": match_scores_df}


@app.function(cpu=1, timeout=3600)
async def run_embedding_scores(condensed_df, src_index_cache, target_index_cache):
    from word_alignment_steps import embeddings

    embedding_scores_df = embeddings.run_embeddings(
        condensed_df, src_index_cache, target_index_cache
    )
    return {"embedding_scores": embedding_scores_df}


def run_total_scores(
    condensed_df,
    alignment_scores_df,
    avg_alignment_scores_df,
    translation_scores_df,
    match_scores_df,
    embedding_scores_df,
    return_all_results: bool = False,
):
    from word_alignment_steps import total_scores

    (
        total_scores_df,
        top_source_scores_df,
        top_target_scores_df,
        verse_scores_df,
        all_results,
    ) = total_scores.run_total_scores(
        condensed_df,
        alignment_scores_df,
        avg_alignment_scores_df,
        translation_scores_df,
        match_scores_df,
        embedding_scores_df,
        return_all_results=return_all_results,
    )
    return {
        "total_scores": total_scores_df,
        "top_source_scores": top_source_scores_df,
        "top_target_scores": top_target_scores_df,
        "verse_scores": verse_scores_df,
        "all_results": all_results,
    }


@app.function(cpu=2, timeout=7200, secrets=[modal.Secret.from_name("aqua-aws")])
async def assess(
    assessment_config: Union[Assessment, dict],
    AQUA_DB: str,
    return_all_results: bool = False,
    threshold: float = 0.3,
    **kwargs,
):
    if isinstance(assessment_config, dict):
        assessment_config = Assessment(**assessment_config)
    database_id = AQUA_DB.split("@")[1][3:].split(".")[0]
    print(
        f"Starting assessment for {database_id}, revision {assessment_config.revision_id}, reference {assessment_config.reference_id}"
    )
    tokenized_dfs = {}
    index_caches = {}
    src_revision_id = assessment_config.reference_id
    trg_revision_id = assessment_config.revision_id
    results = await asyncio.gather(
        *[
            get_index_cache.remote.aio(revision_id, AQUA_DB, refresh=False)
            for revision_id in [src_revision_id, trg_revision_id]
        ]
    )

    for revision_id, index_cache, verse_scores in results:
        tokenized_dfs[revision_id] = verse_scores
        index_caches[revision_id] = index_cache

    condensed_df = create_condensed_df(
        tokenized_dfs[src_revision_id], tokenized_dfs[trg_revision_id]
    )

    if condensed_df.shape[0] == 0:
        print("There are no verses in common between the revision and reference")
        import pandas as pd

        await run_save_results.remote.aio(
            assessment_config.revision_id,
            assessment_config.reference_id,
            pd.DataFrame(columns=["vref", "source", "target", "total_score"]),
            database_id,
            source_type="source",
        )

        await run_save_results.remote.aio(
            assessment_config.revision_id,
            assessment_config.reference_id,
            pd.DataFrame(columns=["vref", "source", "target", "total_score"]),
            database_id,
            source_type="target",
        )
        return {
            "results": [],
            "alignment_threshold_scores": {},
            "alignment_top_source_scores": {},
            "alignment_top_target_scores": {},
        }  # There are no verses in common, so no word alignment to run

    results = await asyncio.gather(
        *[
            run_alignment_scores.remote.aio(condensed_df),
            run_translation_scores.remote.aio(condensed_df),
            run_match_scores.remote.aio(
                condensed_df,
                index_caches[src_revision_id],
                index_caches[trg_revision_id],
            ),
            run_embedding_scores.remote.aio(
                condensed_df,
                index_caches[src_revision_id],
                index_caches[trg_revision_id],
            ),
        ]
    )

    step_results = {}
    for item in results:
        for key, value in item.items():
            step_results[key] = value

    print("Running total scores")
    total_results = run_total_scores(
        condensed_df,
        step_results["alignment_scores"],
        step_results["avg_alignment_scores"],
        step_results["translation_scores"],
        step_results["match_scores"],
        step_results["embedding_scores"],
        return_all_results=return_all_results,
    )

    verse_scores = total_results["verse_scores"]
    all_results = total_results["all_results"]
    top_source_scores = total_results["top_source_scores"]
    top_target_scores = total_results["top_target_scores"]
    total_scores = total_results["total_scores"]
    if return_all_results:
        print("Saving all results to S3")
        import boto3

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket="aqua-word-alignment",
            Key=f"{database_id}/{assessment_config.revision_id}-{assessment_config.reference_id}/verse_scores.csv",
            Body=verse_scores.to_csv(index=False),
        )
        s3.put_object(
            Bucket="aqua-word-alignment",
            Key=f"{database_id}/{assessment_config.revision_id}-{assessment_config.reference_id}/all_results.csv",
            Body=all_results.to_csv(index=False),
        )
        s3.put_object(
            Bucket="aqua-word-alignment",
            Key=f"{database_id}/{assessment_config.revision_id}-{assessment_config.reference_id}/top_source_scores.csv",
            Body=top_source_scores.to_csv(index=False),
        )
        s3.put_object(
            Bucket="aqua-word-alignment",
            Key=f"{database_id}/{assessment_config.revision_id}-{assessment_config.reference_id}/top_target_scores.csv",
            Body=top_target_scores.to_csv(index=False),
        )
        s3.put_object(
            Bucket="aqua-word-alignment",
            Key=f"{database_id}/{assessment_config.revision_id}-{assessment_config.reference_id}/total_scores.csv",
            Body=total_scores.to_csv(index=False),
        )
    print("Saving results to modal shared volume")

    await run_save_results.remote.aio(
        assessment_config.revision_id,
        assessment_config.reference_id,
        total_results["top_source_scores"],
        database_id,
        source_type="source",
    )

    await run_save_results.remote.aio(
        assessment_config.revision_id,
        assessment_config.reference_id,
        total_results["top_target_scores"],
        database_id,
        source_type="target",
    )

    total_scores = total_results["total_scores"]
    top_source_scores = total_results["top_source_scores"]
    top_target_scores = total_results["top_target_scores"]

    results = []
    for _, row in verse_scores.iterrows():
        results.append(
            {"vref": row["vref"], "score": row["total_score"], "flag": False}
        )

    threshold_scores = total_scores[total_scores["total_score"] >= threshold]
    threshold_scores = threshold_scores.rename(columns={"total_score": "score"})
    top_source_scores = top_source_scores.rename(columns={"total_score": "score"})
    top_target_scores = top_target_scores.rename(columns={"total_score": "score"})

    return {
        "results": results,
        "alignment_threshold_scores": threshold_scores.to_dict(orient="records"),
        "alignment_top_source_scores": top_source_scores.to_dict(orient="records"),
        "alignment_top_target_scores": top_target_scores.to_dict(orient="records"),
    }

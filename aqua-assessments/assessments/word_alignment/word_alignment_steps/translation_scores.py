import math
import os
from shutil import rmtree
import time
from typing import Optional

from machine.corpora import TextFileTextCorpus
import modal
import pandas as pd

import word_alignment_steps.train_fa_model as train_fa_model


# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "test"


suffix = f"-{suffix}" if len(suffix) > 0 else ""


async def get_translation_scores(
    model_id: str,
    corpus: TextFileTextCorpus,
    vrefs: list = None,
) -> pd.DataFrame:
    """
    Takes a model and a corpus, and returns a dataframe with all word combinations in the corpus
    and their corresponding translation score in the model.

    Inputs:
    model           A ThotSymmetrizedWordAlignmentModel
    corpus          A TextFileTextCorpus
    vrefs           An optional list of verse references to include in the Dataframe

    Outputs:
    df              A dataframe with the translation scores for every source word / target word combination
    """
    corpus_tuples = [
        (source_verse, target_verse)
        for source_verse, target_verse in corpus.lowercase().to_tuples()
        if len(source_verse) > 0 and len(target_verse) > 0
    ]
    if not vrefs:
        vrefs = [None] * len(corpus_tuples)
    corpus_tuples = [
        (source_verse, target_verse, vref)
        for (source_verse, target_verse), vref in zip(corpus_tuples, vrefs)
    ]

    # Depending on corpus size, split corpus into up to 20 batches
    batch_size = min(len(corpus_tuples), math.ceil(len(corpus_tuples) / 20))
    corpus_tuples_batched = [
        corpus_tuples[i : i + batch_size]
        for i in range(0, len(corpus_tuples), batch_size)
    ]

    get_translation_scores_parallel = modal.Function.lookup(
        f"word-alignment{suffix}", "get_translation_scores"
    )

    data = {"vref": [], "source": [], "target": [], "translation_score": []}
    async for batch in get_translation_scores_parallel.map.aio(
        corpus_tuples_batched,
        kwargs={"model_id": model_id},
    ):
        for k, v in batch.items():
            data[k].extend(v)


    df = pd.DataFrame(data)
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe of sources and targets and groups the data by these sources and targets.

    Inputs:
    df          A dataframe with "source" and "target" columns

    Outputs
    no_dups     A dataframe summarising the results grouped by source and target
                with "align_count" and "word_score" columns
    """
    # remove duplicates and average out verse and word scores
    dups = df.groupby(["source", "target"]).size().reset_index()
    avgs = df.groupby(["source", "target"]).mean(numeric_only=True).reset_index()
    no_dups = pd.merge(dups, avgs)
    no_dups.rename(columns={0: "co-occurrence_count"}, inplace=True)
    return no_dups


async def run_translation_scores(
    condensed_df: pd.DataFrame,
    is_bible: bool = True,
    volume: Optional[modal.Volume] = None,
) -> pd.DataFrame:
    """
    Takes two input text files, runs get_alignments on them, and saves the resulting dataframe
    to a csv file in a directory within outpath.

    Inputs:
    source           Path to a source text file of line-aligned text
    target           Path to a target text file of line-aligned text
    outpath            Path to output directory
    is_bible           Boolean for whether the text is Bible, and hence vref references should be used. If True, both
                        input files must be of length 41,899.
    parallel_corpus    A corpus to process. Normally the corpus is produced from the source and target,
                        but if it has already been produced it can optionally be provided here to save
                        calculating it again.
    symmetrized_model   The model to use. Normally the model is instantiated and trained with the source and target,
                        but if it has already been created and trained it can optionally be provided here to save
                        training it again.

    Outputs:
    TextFileTextCorpus      In case you want to re-use it without creating it from scratch
    ThotSymmetrizedWordAlignmentModel       In case you want to re-use it without training from scratch

    """
    # Train fast_align model
    symmetrized_model, model_id, parallel_corpus = train_fa_model.train_model(
        condensed_df
    )
    symmetrized_model.close()

    # Commit model files to volume, or sleep for background auto commit if volume handle not present
    if volume:
        volume.commit()
    else:
        time.sleep(15)

    vrefs = list(condensed_df["vref"])
    # Get alignments
    print("Getting translation scores...")
    df = await get_translation_scores(model_id, parallel_corpus, vrefs)

    # Cleanup trained model from cache
    rmtree(f"/model_cache/{model_id}")

    # Aggregate the results
    df_agg = aggregate(df)

    df_agg[df_agg.select_dtypes(["float"]).columns] = df_agg.select_dtypes(
        ["float"]
    ).astype("float16")

    return df_agg

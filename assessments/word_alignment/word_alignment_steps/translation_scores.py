import pandas as pd
from machine.corpora import TextFileTextCorpus

from machine.translation.thot import (
    ThotSymmetrizedWordAlignmentModel,
)

import word_alignment_steps.train_fa_model as train_fa_model


def get_translation_scores(
    model: ThotSymmetrizedWordAlignmentModel,
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
    data = {"vref": [], "source": [], "target": [], "translation_score": []}
    c = 0
    for source_verse, target_verse in corpus.lowercase().to_tuples():
        if len(source_verse) > 0 and len(target_verse) > 0:
            vref = vrefs[c] if vrefs else None
            c = c + 1
            for word1 in set(source_verse):
                for word2 in target_verse:
                    data["source"].append(word1)
                    data["target"].append(word2)
                    data["translation_score"].append(
                        model.get_translation_score(word1, word2)
                    )
                    data["vref"].append(vref)
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


def run_translation_scores(
    condensed_df: pd.DataFrame,
    is_bible: bool = True,
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
    symmetrized_model, parallel_corpus = train_fa_model.train_model(condensed_df)
    vrefs = list(condensed_df["vref"])

    # Get alignments
    print("Getting translation scores...")
    df = get_translation_scores(symmetrized_model, parallel_corpus, vrefs)

    # Aggregate the results
    df_agg = aggregate(df)

    df_agg[df_agg.select_dtypes(["float"]).columns] = df_agg.select_dtypes(
        ["float"]
    ).astype("float16")

    return df_agg

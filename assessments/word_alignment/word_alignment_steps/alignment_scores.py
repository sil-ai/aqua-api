from typing import List, Tuple, Optional

import pandas as pd
from machine.corpora import TextFileTextCorpus
import word_alignment_steps.train_fa_model as train_fa_model

from machine.translation.thot import (
    ThotSymmetrizedWordAlignmentModel,
)


def get_best_alignment_scores(
    model: ThotSymmetrizedWordAlignmentModel,
    corpus: TextFileTextCorpus,
    vrefs: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Takes a corpus and a word alignment model and calculates word alignments for each aligned line.
    Returns a dataframe with words that have been aligned by the model.

    Inputs:
    model           A machine.translation.thot.ThotSymmetrizedWordAlignmentModel
    corpus          A machine.corpora.TextFileTextCorpus of aligned texts
    vrefs           An optional list of verse references for each aligned line

    Outputs:
    df                      A dataframe, where each row is an alignment of a source word and a target word.
        source              The source word
        target              The target word
        alignment_count     Integer 1, to later be summed as a count
        verse_score         The average alignment score for the line in question
        vref                The verse reference for that line,     if a vref file has been supplied.
    """
    data = {
        "vref": [],
        "source": [],
        "target": [],
        "alignment_count": [],
        "alignment_score": [],
    }
    segments = list(corpus.lowercase().to_tuples())
    alignments = model.align_batch(segments)
    c = 0
    # for source_segments, target_segments in batch(segments, model.batch_size):

    for (source_segment, target_segment), alignment in zip(list(segments), alignments):
        word_pairs = alignment.to_aligned_word_pairs()
        model.compute_aligned_word_pair_scores(
            source_segment, target_segment, word_pairs
        )

        vref = vrefs[c] if vrefs else None
        c = c + 1
        for pair in word_pairs:
            data["source"].append(source_segment[pair.source_index])
            data["target"].append(target_segment[pair.target_index])
            data["alignment_count"] = 1
            data["alignment_score"].append(pair.alignment_score)
            data["vref"].append(vref)

    df = pd.DataFrame(data)
    return df


def average_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe of aligned matches, removes duplicate source word / target word combinations,
    returns the dataframe.

    Inputs:
    df          A dataframe of alignment matches

    Outputs:
    no_dups     A dataframe with duplicates removed, grouped by source and target words.
    """
    # remove duplicates and average out verse and word scores
    dups = df.groupby(["source", "target"]).size().reset_index()
    avgs = (
        df[["source", "target", "alignment_count", "alignment_score"]]
        .groupby(["source", "target"])
        .mean(numeric_only=True)
        .reset_index()
    )
    no_dups = pd.merge(dups, avgs)
    no_dups.drop(columns=["alignment_count"], inplace=True)
    no_dups.rename(columns={0: "alignment_count"}, inplace=True)
    return no_dups


def run_alignment_scores(
    condensed_df: pd.DataFrame,
    is_bible: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Takes two input text files, runs get_alignments on them, and saves the resulting dataframe
    to a csv file in a directory within outpath.

    Inputs:
    source           Path to a source text file of line-aligned text
    target           Path to a target text file of line-aligned text
    outpath            Path to base output directory
    is_bible           Boolean for whether the text is Bible, and hence vref references should be used.
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
    print("Getting alignment scores...")
    df = get_best_alignment_scores(symmetrized_model, parallel_corpus, vrefs)

    # Remove duplicates
    avg_df = average_scores(df)
    avg_df = avg_df.drop(["alignment_score"], axis=1)
    df = df.drop(["alignment_count"], axis=1)

    df[df.select_dtypes(["float"]).columns] = df.select_dtypes(["float"]).astype(
        "float16"
    )
    avg_df[avg_df.select_dtypes(["float"]).columns] = avg_df.select_dtypes(
        ["float"]
    ).astype("float16")
    
    return df, avg_df  # , parallel_corpus, symmetrized_model

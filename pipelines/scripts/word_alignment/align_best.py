# imports
import argparse
import string
import os
import sys
from typing import List, Tuple, Optional

from unicodedata import category
import pandas as pd
from tqdm import tqdm
from machine.corpora import TextFileTextCorpus
from machine.tokenization import LatinWordTokenizer
from machine.translation import SymmetrizationHeuristic
from align import train_model
from get_data import condense_files, create_corpus, get_ref_df, write_condensed_files

from machine.translation.thot import (
    ThotFastAlignWordAlignmentModel,
    ThotSymmetrizedWordAlignmentModel,
)
from pathlib import Path


def get_best_alignment_scores(
                                model: ThotSymmetrizedWordAlignmentModel, 
                                corpus: TextFileTextCorpus, 
                                vrefs: Optional[List[str]] = None
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
    data = {"vref": [], "source": [], "target": [], "alignment_count": [], "verse_score": [], "alignment_score": []}
    segments = list(corpus.lowercase().to_tuples())
    alignments = model.align_batch(segments)
    c = 0
    # for source_segments, target_segments in batch(segments, model.batch_size):

    for (source_segment, target_segment), alignment in tqdm(zip(list(segments), alignments)):
        word_pairs = alignment.to_aligned_word_pairs()
        model.compute_aligned_word_pair_scores(source_segment, target_segment, word_pairs)

        verse_score = model.get_avg_translation_score(
            source_segment, target_segment, alignment
        )
        vref = vrefs[c] if vrefs else None
        c = c + 1
        for pair in word_pairs:
            data["source"].append(source_segment[pair.source_index])
            data["target"].append(target_segment[pair.target_index])
            data["alignment_count"] = 1
            data["alignment_score"].append(pair.alignment_score)
            data["verse_score"].append(verse_score)
            data["vref"].append(vref)

    df = pd.DataFrame(data)
    return df


def get_vref_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe that includes both "vref" and "verse_score" columns, and returns
    a dataframe of just those two columns.

    Inputs:
    df      A dataframe with both "vref" and "verse_score" columns

    Outputs:
    vref_df     A dataframe with just "vref" and "verse_score" columns
    """
    # remove duplicate verses
    df = df.drop_duplicates(subset=["vref"])
    vref_df = df[["vref", "verse_score"]]
    return vref_df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
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
    avgs = df[['source', 'target', 'alignment_count', 'verse_score', 'alignment_score']].groupby(["source", "target"]).mean(numeric_only=True).reset_index()
    no_dups = pd.merge(dups, avgs)
    no_dups.drop(columns=['alignment_count'], inplace=True)
    no_dups.rename(columns={0: "alignment_count"}, inplace=True)
    return no_dups


def run_best_align(
                    source: Path, 
                    target: Path, 
                    outpath: Path, 
                    is_bible: bool=False, 
                    parallel_corpus: TextFileTextCorpus = None, 
                    symmetrized_model: ThotSymmetrizedWordAlignmentModel = None,
                    ) -> Tuple[TextFileTextCorpus, ThotSymmetrizedWordAlignmentModel]:
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
    # get vrefs
    ref_df = get_ref_df(source, target, is_bible)
    df = condense_files(ref_df)
    vrefs = list(df['vref'])
    condensed_source, condensed_target = write_condensed_files(df, outpath)
    
    # create parallel corpus
    if not parallel_corpus:
        parallel_corpus = create_corpus(condensed_source, condensed_target)

    # Train fast_align model
    if not symmetrized_model:
        symmetrized_model = train_model(parallel_corpus)

    # Get alignments
    print("Getting alignment scores...")
    df = get_best_alignment_scores(symmetrized_model, parallel_corpus, vrefs)

    # Remove duplicates
    no_dups = remove_duplicates(df)

    # write results to csv
    if not outpath.exists():
        outpath.mkdir(parents=True)

    no_dups.to_csv(outpath / "avg_alignment_scores.csv", index=False)

    df.to_csv(outpath / "alignment_scores_by_verse.csv", index=False)

    # delete temp files
    condensed_source.unlink()
    condensed_target.unlink()

    return parallel_corpus, symmetrized_model

if __name__ == "__main__":
    # command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source translation")
    parser.add_argument("--target", type=Path, help="target translation")
    parser.add_argument("--outpath", type=Path, help="where to write results")
    parser.add_argument("--is-bible", type=bool, action='store_true', help="is bible data")
    args, unknown = parser.parse_known_args()

    run_best_align(args.source, args.target, args.outpath, threshold=args.threshold, is_bible=args.is_bible)

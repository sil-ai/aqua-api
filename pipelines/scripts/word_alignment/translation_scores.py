import argparse
from typing import Tuple
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from machine.corpora import TextFileTextCorpus
from machine.translation import SymmetrizationHeuristic

from machine.translation.thot import (
    ThotFastAlignWordAlignmentModel,
    ThotSymmetrizedWordAlignmentModel,
)

import get_data


def train_model(corpus: TextFileTextCorpus) -> ThotSymmetrizedWordAlignmentModel:
    """
    Takes an aligned corpus as input and trains a model on that corpus
    Inputs:
    corpus          A TextFileTextCorpus
    Outputs:
    symmetrized_model       A ThotSymmetrizedWordAlignmentModel trained on the corpus
    """
    src_trg_model = ThotFastAlignWordAlignmentModel()
    trg_src_model = ThotFastAlignWordAlignmentModel()
    symmetrized_model = ThotSymmetrizedWordAlignmentModel(src_trg_model, trg_src_model)
    symmetrized_model.heuristic = SymmetrizationHeuristic.GROW_DIAG_FINAL_AND
    trainer = symmetrized_model.create_trainer(corpus.lowercase())
    trainer.train(
        lambda status: print(
            f"Training Symmetrized FastAlign model: {status.percent_completed:.2%}"
        )
    )
    trainer.save()
    return symmetrized_model


def get_translation_scores(model: ThotSymmetrizedWordAlignmentModel, corpus: TextFileTextCorpus, vrefs: list = None) -> pd.DataFrame:
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
    for source_verse, target_verse in tqdm(corpus.lowercase().to_tuples()):
        if len(source_verse) > 0 and len(target_verse) > 0:
            vref = vrefs[c] if vrefs else None
            c = c + 1
            for word1 in set(source_verse):
                for word2 in target_verse:
                    data["source"].append(word1)
                    data["target"].append(word2)
                    data["translation_score"].append(model.get_translation_score(word1, word2))
                    data["vref"].append(vref)
    df = pd.DataFrame(data)
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
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


def run_align(
    source: Path, target: Path, outpath: Path, is_bible: bool=False, parallel_corpus = None, symmetrized_model = None
) -> Tuple[TextFileTextCorpus, ThotSymmetrizedWordAlignmentModel]:
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
    # get vrefs
    ref_df = get_data.get_ref_df(source, target, is_bible)

    # create parallel corpus
    if not parallel_corpus:
        # remove empty lines
        df = get_data.condense_files(ref_df)
        vrefs = list(df['vref'])
        condensed_source, condensed_target = get_data.write_condensed_files(df, outpath)
        parallel_corpus = get_data.create_corpus(condensed_source, condensed_target)

    # Train fast_align model
    if not symmetrized_model:
        symmetrized_model = train_model(parallel_corpus)

    # Get alignments
    print("Getting translation scores...")
    df = get_translation_scores(symmetrized_model, parallel_corpus, vrefs)

    # Apply threshold
    no_dups = remove_duplicates(df)

    # write results to csv
    if not outpath.exists():
        outpath.mkdir()

    no_dups.to_csv(outpath / "translation_scores.csv", index=False)

    return (parallel_corpus, symmetrized_model)


if __name__ == "__main__":
    # command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source translation")
    parser.add_argument("--target", type=Path, help="target translation")
    parser.add_argument("--outpath", type=Path, help="where to write results")
    parser.add_argument("--is-bible", type=bool, action='store_true', help="is bible data")

    args, unknown = parser.parse_known_args()

    run_align(args.source, args.target, args.outpath, is_bible=args.is_bible)
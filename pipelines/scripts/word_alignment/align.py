import argparse
import string
import os
import sys
from typing import Tuple

from unicodedata import category
import pandas as pd
from tqdm import tqdm
from machine.corpora import TextFileTextCorpus
from machine.tokenization import LatinWordTokenizer
from machine.translation import SymmetrizationHeuristic

from machine.translation.thot import (
    ThotFastAlignWordAlignmentModel,
    ThotSymmetrizedWordAlignmentModel,
)
from pathlib import Path


def write_condensed_files(src_path: Path, trg_path: Path) -> None:
    """
    Takes two input files and writes condensed versions to file, which only include those lines that
    are not blank in both input files.

    Inputs:
    src_path            A Path to the source file
    trg_path            A Path to the target file
    """
    # open files
    with open(src_path) as f:
        src_data = f.readlines()
    with open(trg_path) as f:
        trg_data = f.readlines()

    # make into df
    df = pd.DataFrame({"src": src_data, "trg": trg_data})

    # remove lines that contain \n in either src or trg
    df = df[df.src != "\n"]
    df = df[df.trg != "\n"]

    # remove punctuation
    punctuation_chars = ""
    for i in range(sys.maxunicode):
        if category(chr(i)).startswith("P"):
            punctuation_chars += chr(i)

    df["src"] = df["src"].str.replace("[{}]".format(string.punctuation), "", regex=True)
    df["src"] = df["src"].str.replace("[{}]".format(punctuation_chars), "", regex=True)
    df["trg"] = df["trg"].str.replace("[{}]".format(string.punctuation), "", regex=True)
    df["trg"] = df["trg"].str.replace("[{}]".format(punctuation_chars), "", regex=True)

    # make lowercase
    df["src"] = df["src"].str.lower()
    df["trg"] = df["trg"].str.lower()

    # write to condensed txt files
    with open(src_path.parent / "src_condensed.txt", "w") as f:
        for line in df["src"]:
            f.write(line)
    with open(trg_path.parent / "trg_condensed.txt", "w") as f:
        for line in df["trg"]:
            f.write(line)


def create_corpus(src_file: Path, trg_file: Path) -> TextFileTextCorpus:
    """
    Takes two line-aligned input files and produces a corpus.
    Inputs:
    src_path            A Path to the source file
    trg_path            A Path to the target file

    Outputs:
    parallel_corpus     A tokenized TextFileTextCorpus
    """
    source_corpus = TextFileTextCorpus(src_file)
    target_corpus = TextFileTextCorpus(trg_file)
    parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(
        LatinWordTokenizer()
    )
    return parallel_corpus


def train_model(corpus: TextFileTextCorpus) -> ThotSymmetrizedWordAlignmentModel:
    """
    Takes an aligned corpus as input, and trains a model on that corpus

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


def get_alignment_scores(model: ThotSymmetrizedWordAlignmentModel, corpus: TextFileTextCorpus, vrefs: list = None) -> pd.DataFrame:
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
    data = {"vref": [], "source": [], "target": [], "word_score": []}
    c = 0
    for source_verse, target_verse in tqdm(corpus.lowercase().to_tuples()):
        vref = vrefs[c] if vrefs else None
        c = c + 1
        for word1 in source_verse:
            for word2 in target_verse:
                data["source"].append(word1)
                data["target"].append(word2)
                data["word_score"].append(model.get_translation_score(word1, word2))
                data["vref"].append(vref)
    df = pd.DataFrame(data)
    return df


def get_vrefs(src_file: Path, trg_file: Path, is_bible: bool) -> list:
    """
    Takes two aligned text files and returns a list of vrefs that are non-empty in both files.

    Inputs:
    src_file            A Path to the source file
    trg_file            A Path to the target file

    Outputs:
    df["vref"].tolist()     A list of vrefs that are non-blank in both input files
    """
    with open(src_file) as f:
        src_data = f.readlines()

    with open(trg_file) as f:
        trg_data = f.readlines()

    if is_bible:
        with open("vref.txt", "r") as f:
            vrefs = f.readlines()
        vrefs = [line.strip() for line in vrefs]
    else:
        vrefs = [str(i) for i in range(len(src_data))]

    # min_len = min(len(src_data), len(trg_data), len(vrefs))
    # src_data = src_data[:min_len]
    # trg_data = trg_data[:min_len]
    # vrefs = vrefs[:min_len]

    df = pd.DataFrame({"vref": vrefs, "src": src_data, "trg": trg_data})
    df = df[df.src != "\n"]
    df = df[df.trg != "\n"]
    return df["vref"].tolist()


def remove_duplicates(df: pd.DataFrame, threshold=None) -> pd.DataFrame:
    """
    Takes a dataframe of sources and targets, groups the data by these sources and targets
    and optionally only keeps those where the word_score is above a threshold.

    Inputs:
    df          A dataframe with "source" and "target" columns

    Outputs
    no_dups     A dataframe summarising the results grouped by source and target
                with "align_count" and "word_score" columns
    """
    # remove duplicates and average out verse and word scores
    dups = df.groupby(["source", "target"]).size().reset_index()
    avgs = df.groupby(["source", "target"]).mean().reset_index()
    no_dups = pd.merge(dups, avgs)
    no_dups.rename(columns={0: "align_count"}, inplace=True)

    # apply threshold
    if threshold:
        no_dups = no_dups[no_dups["word_score"] >= threshold]
    return no_dups


def run_align(
    src_path: Path, trg_path: Path, outpath: Path, threshold: float = 0.0, is_bible: bool=False, parallel_corpus = None, symmetrized_model = None
) -> Tuple[TextFileTextCorpus, ThotSymmetrizedWordAlignmentModel]:
    # remove empty lines
    write_condensed_files(src_path, trg_path)

    # get vrefs
    vrefs = get_vrefs(src_path, trg_path, is_bible)

    # create parallel corpus
    if not parallel_corpus:
        parallel_corpus = create_corpus(src_path.parent / "src_condensed.txt", trg_path.parent / "trg_condensed.txt")

    # Train fast_align model
    if not symmetrized_model:
        symmetrized_model = train_model(parallel_corpus)

    # Get alignments
    print("Getting alignments...")
    df = get_alignment_scores(symmetrized_model, parallel_corpus, vrefs)

    # Apply threshold
    no_dups = remove_duplicates(df, threshold=threshold)

    # write results to csv
    path = outpath / f"{src_path.stem}_{trg_path.stem}_align"
    reverse_path = outpath / f"{trg_path.stem}_{src_path.stem}_align"

    # if dir doesn't exist, create it
    if not path.exists():
        path.mkdir()
    if not reverse_path.exists():
        reverse_path.mkdir()

    no_dups.to_csv(path / "all_sorted.csv")
    no_dups.to_csv(reverse_path / "all_sorted.csv")


    df.to_csv(path / "all_in_context.csv")
    df.to_csv(reverse_path / "all_in_context.csv")


    # delete temp files
    (src_path.parent / "src_condensed.txt").unlink()
    (trg_path.parent / "trg_condensed.txt").unlink()

    return (parallel_corpus, symmetrized_model)


if __name__ == "__main__":
    # command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source translation")
    parser.add_argument("--target", type=Path, help="target translation")
    parser.add_argument(
        "--threshold", type=float, default=0.5, help="word score threshold {0,1}"
    )
    parser.add_argument("--outpath", type=Path, help="where to write results")
    parser.add_argument("--is-bible", type=bool, action='store_true', help="is bible data")

    args, unknown = parser.parse_known_args()

    run_align(args.source, args.target, args.outpath, threshold=args.threshold, is_bible=args.is_bible)

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


def write_condensed_files(source: Path, target: Path, outpath: Path) -> Tuple[Path, Path]:
    """
    Takes two input files and writes condensed versions to file, which only include those lines that
    are not blank in both input files.

    Inputs:
    source            A Path to the source file
    target            A Path to the target file
    outpath           The path where the two condensed files will be written

    Outputs:
    source_path        The Path where the condensed source file has been written
    target_path        The Path where the condensed target file has been written

    """
    # open files
    with open(source) as f:
        src_data = f.readlines()
    with open(target) as f:
        trg_data = f.readlines()

    # make into df
    df = pd.DataFrame({"src": src_data, "trg": trg_data})

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

    # remove lines that contain \n in either src or trg
    df = df[df.src != "\n"]
    df = df[df.trg != "\n"]

    # write to condensed txt files
    if not outpath.exists():
        outpath.mkdir(exist_ok=True)
    source_path = outpath / f"{source.stem}_condensed.txt"
    target_path = outpath / f"{target.stem}_condensed.txt"

    with open(source_path, "w") as f:
        for line in df["src"]:
            f.write(line)
    with open(target_path, "w") as f:
        for line in df["trg"]:
            f.write(line)
    return (source_path, target_path)


def create_corpus(condensed_source: Path, condensed_target: Path) -> TextFileTextCorpus:
    """
    Takes two line-aligned condensed input files and produces a tokenized corpus. Note that this must be run on the
    output of write_condensed_files(), which removes the lines that are blank in either file.
    Inputs:
    condensed_source            A Path to the source file
    condensed_target            A Path to the target file

    Outputs:
    parallel_corpus     A tokenized TextFileTextCorpus
    """
    source_corpus = TextFileTextCorpus(condensed_source)
    target_corpus = TextFileTextCorpus(condensed_target)
    parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(
        LatinWordTokenizer()
    )
    return parallel_corpus


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


def get_vrefs(source: Path, target: Path, is_bible: bool) -> list:
    """
    Takes two aligned text files and returns a list of vrefs corresponding to lines that are non-empty in both files.

    Inputs:
    source            A Path to the source file
    target            A Path to the target file
    is_bible          Boolean for whether the text is Bible. If is_bible is true, the length of the text files must be 41,899 lines.

    Outputs:
    vref_list     A list of vrefs that are non-blank in both input files
    """
    with open(source) as f:
        src_data = f.readlines()

    with open(target) as f:
        trg_data = f.readlines()

    if is_bible:
        assert len(src_data) == 41899, "is_bible requires your source input to be 41899 lines in length"
        assert len(trg_data) == 41899, "is_bible requires your target input to be 41899 lines in length"
        with open("vref.txt", "r") as f:
            vrefs = f.readlines()
        vrefs = [line.strip() for line in vrefs]
        assert len(vrefs) == 41899,  "the vref.txt file must be 41899 lines in length"

    else:
        vrefs = [str(i) for i in range(len(src_data))]

    df = pd.DataFrame({"vref": vrefs, "src": src_data, "trg": trg_data})
    df = df[df.src != "\n"]
    df = df[df.trg != "\n"]
    vref_list = df["vref"].tolist()
    return vref_list


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
    avgs = df.groupby(["source", "target"]).mean().reset_index()
    no_dups = pd.merge(dups, avgs)
    print(no_dups)
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
    outpath            Path to base output directory
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
    vrefs = get_vrefs(source, target, is_bible)

    # create parallel corpus
    if not parallel_corpus:
        # remove empty lines
        condensed_source, condensed_target = write_condensed_files(source, target, outpath)
        parallel_corpus = create_corpus(condensed_source, condensed_target)

    # Train fast_align model
    if not symmetrized_model:
        symmetrized_model = train_model(parallel_corpus)

    # Get alignments
    print("Getting alignments...")
    df = get_translation_scores(symmetrized_model, parallel_corpus, vrefs)

    # Apply threshold
    no_dups = remove_duplicates(df)

    # write results to csv
    if not outpath.exists():
        outpath.mkdir()

    no_dups.to_csv(outpath / "all_sorted.csv")

    df.to_csv(outpath / "all_in_context.csv")

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

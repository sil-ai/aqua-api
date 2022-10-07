# imports
import argparse
import string
import os
import sys

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


def write_condensed_files(src_file: Path, trg_file: Path) -> None:
    # open files
    with open(src_file) as f:
        src_data = f.readlines()
    with open(trg_file) as f:
        trg_data = f.readlines()

    min_len = min(len(src_data), len(trg_data))
    src_data = src_data[:min_len]
    trg_data = trg_data[:min_len]

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
    with open("src_condensed.txt", "w") as f:
        for line in df["src"]:
            f.write(line)
    with open("trg_condensed.txt", "w") as f:
        for line in df["trg"]:
            f.write(line)


def create_corpus(src_file: Path, trg_file: Path):
    source_corpus = TextFileTextCorpus(src_file)
    target_corpus = TextFileTextCorpus(trg_file)
    parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(
        LatinWordTokenizer()
    )
    return parallel_corpus


def train_model(corpus):
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


def get_alignments(model, corpus, vrefs):
    data = {"vref": [], "source": [], "target": [], "word score": [], "verse score": []}
    alignments = model.get_best_alignment_batch(corpus.lowercase().to_tuples())
    c = 0
    for source_segment, target_segment, alignment in tqdm(alignments):
        pair_indices = alignment.to_aligned_word_pairs()
        verse_score = model.get_avg_translation_score(
            source_segment, target_segment, alignment
        )
        vref = vrefs[c]
        c = c + 1
        for pair in pair_indices:
            score = model.get_translation_score(
                source_segment[pair.source_index], target_segment[pair.target_index]
            )
            data["source"].append(source_segment[pair.source_index])
            data["target"].append(target_segment[pair.target_index])
            data["word score"].append(score)
            data["verse score"].append(verse_score)
            data["vref"].append(vref)

    df = pd.DataFrame(data)
    return df

def get_vrefs(src_file, trg_file, is_bible):
    with open(src_file) as f:
        src_data = f.readlines()

    with open(trg_file) as f:
        trg_data = f.readlines()

    if is_bible == "True":
        with open("vref.txt", "r") as f:
            vrefs = f.readlines()
        vrefs = [line.strip() for line in vrefs]
    else:
        vrefs = [str(i) for i in range(len(src_data))]

    min_len = min(len(src_data), len(trg_data), len(vrefs))
    src_data = src_data[:min_len]
    trg_data = trg_data[:min_len]
    vrefs = vrefs[:min_len]

    df = pd.DataFrame({"vref": vrefs, "src": src_data, "trg": trg_data})
    df = df[df.src != "\n"]
    df = df[df.trg != "\n"]
    return df["vref"].tolist()


def get_vref_scores(df):
    # remove duplicate verses
    df = df.drop_duplicates(subset=["vref"])
    vref_df = df[["vref", "verse score"]]
    return vref_df


def apply_threshold(df, threshold):
    # remove duplicates and average out verse and word scores
    dups = df.groupby(["source", "target"]).size().reset_index()
    avgs = df.groupby(["source", "target"]).mean().reset_index()
    no_dups = pd.merge(dups, avgs)
    no_dups.rename(columns={0: "align_count"}, inplace=True)

    # apply threshold
    no_dups = no_dups[no_dups["word score"] >= threshold]
    return no_dups


def run_align(
    src_file: Path, trg_file: Path, threshold: float, outpath: Path, is_bible
):
    # remove empty lines
    write_condensed_files(src_file, trg_file)

    # get vrefs
    vrefs = get_vrefs(src_file, trg_file, is_bible)

    # create parallel corpus
    parallel_corpus = create_corpus("src_condensed.txt", "trg_condensed.txt")

    # Train fast_align model
    symmetrized_model = train_model(parallel_corpus)

    # Get alignments
    print("Getting alignments...")
    df = get_alignments(symmetrized_model, parallel_corpus, vrefs)

    print("Getting reverse alignments...")
    reverse_df = get_alignments(symmetrized_model.inverse_word_alignment_model, parallel_corpus.invert(), vrefs)

    # Get verse scores
    vref_df = get_vref_scores(df)
    reverse_vref_df = get_vref_scores(reverse_df)

    # Apply threshold
    no_dups = apply_threshold(df, threshold)
    reverse_no_dups = apply_threshold(reverse_df, threshold)


    #write results to csv
    path = outpath / f'{src_file.stem}_{trg_file.stem}_align'
    reverse_path = outpath / f'{trg_file.stem}_{src_file.stem}_align'

    
    #if dir doesn't exist, create it
    if not path.exists():
        path.mkdir()
    if not reverse_path.exists():
        reverse_path.mkdir()
    
    no_dups.to_csv(path / "sorted.csv")
    reverse_no_dups.to_csv(reverse_path / "sorted.csv")

    df.to_csv(path / "in_context.csv")
    reverse_df.to_csv(reverse_path / "in_context.csv")
    
    vref_df.to_csv(path / "vref_scores.csv")
    reverse_vref_df.to_csv(reverse_path / "vref_scores.csv")

    # delete temp files
    os.remove("src_condensed.txt")
    os.remove("trg_condensed.txt")


if __name__ == "__main__":
    # command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source translation")
    parser.add_argument("--target", type=Path, help="target translation")
    parser.add_argument(
        "--threshold", type=float, default=0.5, help="word score threshold {0,1}"
    )
    parser.add_argument("--outpath", type=Path, help="where to write results")
    parser.add_argument("--is-bible", type=str, default="False", help="is bible data?")
    args, unknown = parser.parse_known_args()

    run_align(args.source, args.target, args.threshold, args.outpath, args.is_bible)

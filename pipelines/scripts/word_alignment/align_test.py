import os

from pathlib import Path
import align
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel


def test_write_condensed_files():

    align.write_condensed_files(Path("fixtures/src.txt"), Path("fixtures/trg.txt"))

    # check that files exist
    assert os.path.exists("fixtures/src.txt")
    assert os.path.exists("fixtures/trg.txt")
    assert os.path.exists("src_condensed.txt")
    assert os.path.exists("trg_condensed.txt")

    # open the files
    with open("fixtures/src.txt", "r") as f:
        src_data = f.readlines()
    with open("fixtures/trg.txt", "r") as f:
        trg_data = f.readlines()
    with open("src_condensed.txt", "r") as f:
        src_data_c = f.readlines()
    with open("trg_condensed.txt", "r") as f:
        trg_data_c = f.readlines()

    # check that the condensed files are shorter
    assert len(src_data) >= len(src_data_c)
    assert len(trg_data) >= len(trg_data_c)

    # check that the condensed files are the same length
    assert len(src_data_c) == len(trg_data_c)

    # check that the condensed files contain no empty lines
    for line in src_data_c:
        assert line != "\n"
    for line in trg_data_c:
        assert line != "\n"

    # remove the condensed files
    os.remove("src_condensed.txt")
    os.remove("trg_condensed.txt")


def test_create_corpus():
    corpus = align.create_corpus(Path("fixtures/src.txt"), Path("fixtures/trg.txt"))
    assert corpus is not None


def test_train_model():
    corpus = align.create_corpus(Path("fixtures/src.txt"), Path("fixtures/trg.txt"))
    model = align.train_model(corpus)
    assert model is not None
    assert type(model) == ThotSymmetrizedWordAlignmentModel


def test_get_alignment_scores():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    vrefs = align.get_vrefs(src_file, trg_file, False)
    corpus = align.create_corpus(Path("fixtures/src.txt"), Path("fixtures/trg.txt"))
    model = align.train_model(corpus)
    alignments = align.get_alignment_scores(model, corpus, vrefs)

    assert type(alignments) == pd.DataFrame
    assert len(alignments) > 0


def test_get_vrefs():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    # get src file length
    with open(src_file, "r") as f:
        src_data = f.readlines()
    vrefs = align.get_vrefs(src_file, trg_file, False)
    assert type(vrefs) == list
    assert len(vrefs) == len(src_data)


def test_run_align():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    threshold = 0.5
    outpath = Path("fixtures")
    is_bible = False

    align.run_align(src_file, trg_file, threshold, outpath, is_bible)

    # make out dir
    outdir = Path("fixtures/src_trg_align")
    outdir.mkdir(parents=True, exist_ok=True)

    # check the files exist
    in_context = Path(outdir, "in_context.csv")
    sorted_f = Path(outdir, "sorted.csv")
    assert in_context.exists()
    assert sorted_f.exists()

    # delete the files
    in_context.unlink()
    sorted_f.unlink()

    # delete dir
    outdir.rmdir()

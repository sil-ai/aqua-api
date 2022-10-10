import os

from pathlib import Path
import align_best
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel


def test_write_condensed_files():

    align_best.write_condensed_files(Path("fixtures/src.txt"), Path("fixtures/trg.txt"))

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
    corpus = align_best.create_corpus(
        Path("fixtures/src.txt"), Path("fixtures/trg.txt")
    )
    assert corpus is not None


def test_train_model():
    corpus = align_best.create_corpus(
        Path("fixtures/src.txt"), Path("fixtures/trg.txt")
    )
    model = align_best.train_model(corpus)
    assert model is not None
    assert type(model) == ThotSymmetrizedWordAlignmentModel


def test_get_alignments():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    vrefs = align_best.get_vrefs(src_file, trg_file, False)
    corpus = align_best.create_corpus(
        Path("fixtures/src.txt"), Path("fixtures/trg.txt")
    )
    model = align_best.train_model(corpus)
    alignments = align_best.get_alignments(model, corpus, vrefs)

    assert type(alignments) == pd.DataFrame
    assert len(alignments) > 0


def test_get_vrefs():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    # get src file length
    with open(src_file, "r") as f:
        src_data = f.readlines()
    vrefs = align_best.get_vrefs(src_file, trg_file, False)
    assert type(vrefs) == list
    assert len(vrefs) == len(src_data)


def test_get_vref_scores():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    vrefs = align_best.get_vrefs(src_file, trg_file, False)
    corpus = align_best.create_corpus(src_file, trg_file)
    model = align_best.train_model(corpus)
    alignments = align_best.get_alignments(model, corpus, vrefs)
    vref_scores = align_best.get_vref_scores(alignments)
    assert type(vref_scores) == pd.DataFrame
    assert len(vref_scores) > 0


def test_apply_threshold():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    vrefs = align_best.get_vrefs(src_file, trg_file, False)
    corpus = align_best.create_corpus(src_file, trg_file)
    model = align_best.train_model(corpus)
    alignments = align_best.get_alignments(model, corpus, vrefs)
    sorted_alignments = align_best.apply_threshold(alignments, 0.5)
    assert type(sorted_alignments) == pd.DataFrame
    assert len(sorted_alignments) > 0
    assert len(sorted_alignments) < len(alignments)


def test_run_align():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    threshold = 0.5
    outpath = Path("fixtures")
    is_bible = False

    align_best.run_best_align(src_file, trg_file, threshold, outpath, is_bible)

    # make out dir
    outdir = Path("fixtures")
    outdir.mkdir(parents=True, exist_ok=True)

    # get forwards and backwards dirs
    fwd_dir = outdir / "src_trg_align_best"
    bwd_dir = outdir / "trg_src_align_best"

    # check forward files exist
    in_context_fwd = Path(fwd_dir, "best_in_context.csv")
    sorted_f_fwd = Path(fwd_dir, "best_sorted.csv")
    vrefs_fwd = Path(fwd_dir, "best_vref_scores.csv")
    assert in_context_fwd.exists()
    assert sorted_f_fwd.exists()
    assert vrefs_fwd.exists()

    # check backward files exist
    in_context_bwd = Path(bwd_dir, "best_in_context.csv")
    sorted_f_bwd = Path(bwd_dir, "best_sorted.csv")
    vrefs_bwd = Path(bwd_dir, "best_vref_scores.csv")
    assert in_context_bwd.exists()
    assert sorted_f_bwd.exists()
    assert vrefs_bwd.exists()

    # delete the files
    in_context_bwd.unlink()
    sorted_f_bwd.unlink()
    vrefs_bwd.unlink()
    in_context_fwd.unlink()
    sorted_f_fwd.unlink()
    vrefs_fwd.unlink()

    # delete dirs
    bwd_dir.rmdir()
    fwd_dir.rmdir()
    # outdir.rmdir()

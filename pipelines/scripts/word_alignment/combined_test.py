import os
from pathlib import Path

import combined


def test_run_fa():
    # align all
    combined.run_fa(
        Path("fixtures/src.txt"),
        Path("fixtures/trg.txt"),
        0.5,
        Path("fixtures"),
        False,
        align_best_alignment=False,
    )
    assert os.path.exists("fixtures/src_trg_align/all_sorted.csv")
    assert os.path.exists("fixtures/src_trg_align/all_in_context.csv")
    os.remove("fixtures/src_trg_align/all_sorted.csv")
    os.remove("fixtures/src_trg_align/all_in_context.csv")
    os.rmdir("fixtures/src_trg_align")

    # align best
    combined.run_fa(
        Path("fixtures/src.txt"),
        Path("fixtures/trg.txt"),
        0.5,
        Path("fixtures"),
        False,
        align_best_alignment=True,
    )
    assert os.path.exists("fixtures/src_trg_align_best/best_sorted.csv")
    assert os.path.exists(
        "fixtures/src_trg_align_best/best_in_context.csv"
    )
    assert os.path.exists(
        "fixtures/src_trg_align_best/best_vref_scores.csv"
    )
    assert os.path.exists("fixtures/trg_src_align_best/best_sorted.csv")
    assert os.path.exists(
        "fixtures/trg_src_align_best/best_in_context.csv"
    )
    assert os.path.exists(
        "fixtures/trg_src_align_best/best_vref_scores.csv"
    )

    # remove all files and dirs
    for f in os.listdir("fixtures/src_trg_align_best"):
        os.remove(os.path.join("fixtures/src_trg_align_best", f))
    for f in os.listdir("fixtures/trg_src_align_best"):
        os.remove(os.path.join("fixtures/trg_src_align_best", f))
    os.rmdir("fixtures/src_trg_align_best")
    os.rmdir("fixtures/trg_src_align_best")
    # os.rmdir("fixtures")


def test_run_match_words():
    src_file = Path("fixtures/src.txt")
    trg_file = Path("fixtures/trg.txt")
    outpath = Path("fixtures")
    combined.run_match_words(src_file, trg_file, outpath, 0.5, 5, False)
    assert os.path.exists("fixtures/src_trg_match/src_trg-dictionary.json")
    # empty contents of dir and delete
    for f in os.listdir("fixtures/src_trg_match"):
        os.remove(os.path.join("fixtures/src_trg_match", f))
    for f in os.listdir("fixtures/cache"):
        os.remove(os.path.join("fixtures/cache", f))
    os.rmdir("fixtures/src_trg_match")
    os.rmdir("fixtures/cache")

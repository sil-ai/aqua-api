import os
from pathlib import Path

import combined


def test_make_output_dir():
    s, t, path = combined.make_output_dir(
        Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt"), Path("fixtures")
    )
    assert path.exists()
    assert s == "de-LU1912"
    assert t == "en-KJV"
    Path.rmdir(path)


def test_run_fa():
    combined.run_fa(
        Path("fixtures/src.txt"), Path("fixtures/trg.txt"), 0.5, Path("fixtures"), False
    )
    assert os.path.exists("fixtures/src_trg_align/sorted.csv")
    assert os.path.exists("fixtures/src_trg_align/in_context.csv")
    assert os.path.exists("fixtures/src_trg_align/vref_scores.csv")
    os.remove("fixtures/src_trg_align/sorted.csv")
    os.remove("fixtures/src_trg_align/in_context.csv")
    os.remove("fixtures/src_trg_align/vref_scores.csv")
    os.rmdir("fixtures/src_trg_align")


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

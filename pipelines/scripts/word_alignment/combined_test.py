import os
from pathlib import Path
import pytest

import combined

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    ])
def test_run_fa(source, target):
    # align all
    combined.run_fa(
        source,
        target,
        source.parent,
        0.5,
        is_bible=False,
    )
    assert os.path.exists(source.parent / "src_trg_align/all_sorted.csv")
    assert os.path.exists(source.parent / "src_trg_align/all_in_context.csv")
    os.remove(source.parent / "src_trg_align/all_sorted.csv")
    os.remove(source.parent / "src_trg_align/all_in_context.csv")
    os.rmdir(source.parent / "src_trg_align")

    # align best
    combined.run_fa(
        source,
        target,
        source.parent,
        0.5,
        is_bible=False,
        align_best_alignment=True,
    )
    assert os.path.exists(source.parent / "src_trg_align_best/best_sorted.csv")
    assert os.path.exists(
        source.parent / "src_trg_align_best/best_in_context.csv"
    )
    assert os.path.exists(
        source.parent / "src_trg_align_best/best_vref_scores.csv"
    )
    assert os.path.exists(source.parent / "trg_src_align_best/best_sorted.csv")
    assert os.path.exists(
        source.parent / "trg_src_align_best/best_in_context.csv"
    )
    assert os.path.exists(
        source.parent / "trg_src_align_best/best_vref_scores.csv"
    )

    # remove all files and dirs
    for f in (source.parent / "src_trg_align_best").iterdir():
        f.unlink()
    for f in (source.parent / "trg_src_align_best").iterdir():
        f.unlink()
    os.rmdir(source.parent / "src_trg_align_best")
    os.rmdir(source.parent / "trg_src_align_best")
    # os.rmdir("fixtures")

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    ])
def test_run_match_words(source, target):
    outpath = source.parent
    combined.run_match_words(source, target, outpath, 0.5, 5, False)
    assert os.path.exists(source.parent / "src_trg_match/src_trg-dictionary.json")
    # empty contents of dir and delete
    for f in (source.parent / "src_trg_match").iterdir():
        f.unlink()
    for f in (source.parent / "cache").iterdir():
        f.unlink()
    os.rmdir("fixtures/src_trg_match")
    os.rmdir("fixtures/cache")

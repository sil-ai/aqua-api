from cmath import isfinite
import os
import pytest

from pathlib import Path
import align
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel

@pytest.mark.parametrize("source,target,expected", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), None), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt"), None),
                                                    (Path("fixtures/de-LU1912_some_missing.txt"), Path("fixtures/en-KJV_some_missing.txt"), None),
                                                    ])
def test_write_condensed_files(source, target, expected):

    align.write_condensed_files(source, target)

    # check that files exist
    assert os.path.exists(source)
    assert os.path.exists(target)
    assert os.path.exists(source.parent / "src_condensed.txt")
    assert os.path.exists(target.parent /  "trg_condensed.txt")

    # open the files
    with open(source, "r") as f:
        src_data = f.readlines()
    with open(target, "r") as f:
        trg_data = f.readlines()
    with open(source.parent / "src_condensed.txt", "r") as f:
        src_data_c = f.readlines()
    with open(target.parent / "trg_condensed.txt", "r") as f:
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
    os.remove(source.parent / "src_condensed.txt")
    os.remove(target.parent / "trg_condensed.txt")

@pytest.mark.parametrize("source,target", [
                                                    ("fixtures/src.txt", "fixtures/trg.txt"), 
                                                    ("fixtures/de-LU1912.txt", "fixtures/en-KJV.txt"),
                                                    ("fixtures/de-LU1912_some_missing.txt", "fixtures/en-KJV_some_missing.txt"),
                                                    ])
def test_create_corpus(source, target):
    corpus = align.create_corpus(Path(source), Path(target))
    assert corpus is not None
    # print(next(corpus.to_tuples()))
    first_item_source = next(corpus.to_tuples())[0]
    first_item_target = next(corpus.to_tuples())[1]
    assert isinstance(first_item_source, list)
    assert isinstance(first_item_target, list)
    if len(first_item_source) > 0:
        assert isinstance(first_item_source[0], str)
    if len(first_item_target) > 0:
        assert isinstance(first_item_target[0], str)

@pytest.mark.parametrize("source,target", [
                                                    ("fixtures/src.txt", "fixtures/trg.txt"), 
                                                    ])
def test_train_model(source, target):
    corpus = align.create_corpus(Path(source), Path(target))
    model = align.train_model(corpus)
    assert model is not None
    assert type(model) == ThotSymmetrizedWordAlignmentModel

@pytest.mark.parametrize("source,target", [
                                                    ("fixtures/src.txt", "fixtures/trg.txt"), 
                                                    ])
def test_get_alignment_scores(source, target):
    src_file = Path(source)
    trg_file = Path(target)
    vrefs = align.get_vrefs(src_file, trg_file, False)
    corpus = align.create_corpus(Path(source), Path(target))
    model = align.train_model(corpus)
    alignments = align.get_alignment_scores(model, corpus, vrefs)
    assert isinstance(alignments, pd.DataFrame)
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

@pytest.mark.parametrize("source,target", [
                                            (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                            ])
def test_run_align(source, target):
    src_file = source
    trg_file = target
    threshold = 0.5
    outpath = source.parent / 'out'
    is_bible = False

    outdir = outpath / f"{source.stem}_{target.stem}_align"
    reverse_outdir = outpath / f"{target.stem}_{source.stem}_align"

    align.run_align(src_file, trg_file, outpath, threshold=threshold, is_bible=is_bible)

    # check the files exist
    in_context = Path(outdir, "all_in_context.csv")
    sorted_f = Path(outdir, "all_sorted.csv")
    reverse_in_context = Path(reverse_outdir, "all_in_context.csv")
    reverse_sorted_f = Path(reverse_outdir, "all_sorted.csv")
    assert in_context.exists()
    assert sorted_f.exists()
    assert reverse_in_context.exists()
    assert reverse_sorted_f.exists()
    
    # delete the files
    in_context.unlink()
    sorted_f.unlink()
    reverse_in_context.unlink()
    reverse_sorted_f.unlink()
    
    # delete dir
    outdir.rmdir()
    reverse_outdir.rmdir()


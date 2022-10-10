import os

from pathlib import Path
import pytest
import align_best
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912_some_missing.txt"), Path("fixtures/en-KJV_some_missing.txt")),
                                                    ])
def test_write_condensed_files(source, target):

    align_best.write_condensed_files(source, target)

    # check that files exist
    assert os.path.exists(source)
    assert os.path.exists(target)
    assert os.path.exists(source.parent / "src_condensed.txt")
    assert os.path.exists(target.parent / "trg_condensed.txt")

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
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912_some_missing.txt"), Path("fixtures/en-KJV_some_missing.txt")),
                                                    ])
def test_create_corpus(source, target):
    corpus = align_best.create_corpus(source, target)
    assert corpus is not None
    first_item_source = next(corpus.to_tuples())[0]
    first_item_target = next(corpus.to_tuples())[1]
    assert isinstance(first_item_source, list)
    assert isinstance(first_item_target, list)
    if len(first_item_source) > 0:
        assert isinstance(first_item_source[0], str)
    if len(first_item_target) > 0:
        assert isinstance(first_item_target[0], str)

@pytest.mark.parametrize("source,target", [
                                            (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                            ])
def test_train_model(source, target):
    corpus = align_best.create_corpus(source, target)
    model = align_best.train_model(corpus)
    assert model is not None
    assert isinstance(model, ThotSymmetrizedWordAlignmentModel)

@pytest.mark.parametrize("source,target", [
                                            (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                            ])
def test_get_alignments(source, target):
    vrefs = align_best.get_vrefs(source, target, False)
    corpus = align_best.create_corpus(source, target)
    model = align_best.train_model(corpus)
    alignments = align_best.get_alignments(model, corpus, vrefs)

    assert type(alignments) == pd.DataFrame
    assert len(alignments) > 0

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), False), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt"), True),
                                                    (Path("fixtures/de-LU1912_some_missing.txt"), Path("fixtures/en-KJV_some_missing.txt"), True),
                                                    ])
def test_get_vrefs(source, target, is_bible):
    # get src file length
    with open(source, "r") as f:
        src_data = f.readlines()
    with open(target, "r") as f:
        trg_data = f.readlines()
    src_non_empty = [line for line in src_data if line != '\n']
    trg_non_empty = [line for line in trg_data if line != '\n']

    vrefs = align_best.get_vrefs(source, target, is_bible)
    assert type(vrefs) == list
    assert len(vrefs) <= min(len(src_non_empty), len(trg_non_empty))

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    ])
def test_get_vref_scores(source, target):
    vrefs = align_best.get_vrefs(source, target, False)
    corpus = align_best.create_corpus(source, target)
    model = align_best.train_model(corpus)
    alignments = align_best.get_alignments(model, corpus, vrefs)
    vref_scores = align_best.get_vref_scores(alignments)
    assert type(vref_scores) == pd.DataFrame
    assert len(vref_scores) > 0

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    ])
def test_apply_threshold(source, target):
    vrefs = align_best.get_vrefs(source, target, False)
    corpus = align_best.create_corpus(source, target)
    model = align_best.train_model(corpus)
    alignments = align_best.get_alignments(model, corpus, vrefs)
    sorted_alignments = align_best.apply_threshold(alignments, 0.5)
    assert type(sorted_alignments) == pd.DataFrame
    assert len(sorted_alignments) > 0
    assert len(sorted_alignments) < len(alignments)

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    ])
def test_run_align(source, target):
    threshold = 0.5
    outpath = source.parent
    is_bible = False

    align_best.run_best_align(source, target, outpath, threshold=threshold, is_bible=is_bible)

    # make out dir
    outdir = source.parent
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

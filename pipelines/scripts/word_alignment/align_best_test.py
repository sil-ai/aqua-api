from cgi import test
import os

from pathlib import Path
from align import write_condensed_files, get_vrefs, create_corpus, train_model
from align_best import remove_duplicates
import pytest
import align_best
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912-some-missing.txt"), Path("fixtures/en-KJV-some-missing.txt")),
                                                    ])
def test_write_condensed_files(source, target):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    align_best.write_condensed_files(source, target, outpath)

    # check that files exist
    assert os.path.exists(source)
    assert os.path.exists(target)
    assert os.path.exists(outpath / f"{source.stem}_condensed.txt")
    assert os.path.exists(outpath / f"{target.stem}_condensed.txt")

    # open the files
    with open(source, "r") as f:
        src_data = f.readlines()
    with open(target, "r") as f:
        trg_data = f.readlines()
    with open(outpath / f"{source.stem}_condensed.txt", "r") as f:
        src_data_c = f.readlines()
    with open(outpath / f"{target.stem}_condensed.txt", "r") as f:
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
    os.remove(outpath / f"{source.stem}_condensed.txt")
    os.remove(outpath / f"{target.stem}_condensed.txt")

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912-some-missing.txt"), Path("fixtures/en-KJV-some-missing.txt")),
                                                    ])
def test_create_corpus(source, target):
    outpath =  source.parent / 'out' / f'{source.stem}_{target.stem}'
    write_condensed_files(source, target, outpath)
    corpus = align_best.create_corpus(outpath / f"{source.stem}_condensed.txt", outpath / f"{target.stem}_condensed.txt")

    assert corpus is not None
    first_item_source = next(corpus.to_tuples())[0]
    first_item_target = next(corpus.to_tuples())[1]
    assert isinstance(first_item_source, list)
    assert isinstance(first_item_target, list)
    if len(first_item_source) > 0:
        assert isinstance(first_item_source[0], str)
    if len(first_item_target) > 0:
        assert isinstance(first_item_target[0], str)
    (outpath / f'{source.stem}_condensed.txt').unlink()
    (outpath / f'{target.stem}_condensed.txt').unlink()

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
    alignments = align_best.get_best_alignment_scores(model, corpus, vrefs)

    assert type(alignments) == pd.DataFrame
    assert len(alignments) > 0

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), False), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt"), True),
                                                    (Path("fixtures/de-LU1912-some-missing.txt"), Path("fixtures/en-KJV-some-missing.txt"), True),
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
    alignments = align_best.get_best_alignment_scores(model, corpus, vrefs)
    vref_scores = align_best.get_vref_scores(alignments)
    assert type(vref_scores) == pd.DataFrame
    assert len(vref_scores) > 0


@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt")), 
                                                    ])
def test_get_best_alignment_scores(source, target):
    is_bible=False
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    write_condensed_files(source, target, outpath)
    vrefs = get_vrefs(source, target, is_bible)
    parallel_corpus = create_corpus(outpath / f"{source.stem}_condensed.txt", outpath / f"{target.stem}_condensed.txt")
    symmetrized_model = train_model(parallel_corpus)
    df = align_best.get_best_alignment_scores(symmetrized_model, parallel_corpus, vrefs)
    assert len(df) > 20
    assert len(df['alignment_score'].unique()) > 20
    assert len(df['verse_score'].unique()) > 1

    df.to_csv(f'fixtures/df_best_alignment_scores_{source.stem}_{target.stem}.csv')

    (outpath / f"{source.stem}_condensed.txt").unlink()
    (outpath / f"{target.stem}_condensed.txt").unlink()


@pytest.mark.parametrize("df_source", [
                                                    Path("fixtures/df_best_alignment_scores_src_trg.csv"), 
                                                    Path("fixtures/df_best_alignment_scores_de-LU1912-mini_en-KJV-mini.csv"), 
                                                    ])
def test_remove_duplicates(df_source):
    df = pd.read_csv(df_source)
    df = remove_duplicates(df)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert min(df['alignment_count']) >= 1
    assert max(df['verse_score']) <= 1
    assert min(df['verse_score']) >= 0
    assert len(df['verse_score'].unique()) >= 5
    assert max(df['alignment_score']) <= 1
    assert min(df['alignment_score']) >= 0
    assert len(df['alignment_score'].unique()) >= 20
    df.to_csv(df_source.parent / f'df_best_remove_duplicates_output_{df_source.stem.split("_",)[-2]}_{df_source.stem.split("_",)[-1]}.csv')


@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True),
                                                    ])
def test_run_best_align(source, target, is_bible, delete_files=False):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'

    align_best.run_best_align(source, target, outpath, is_bible=is_bible)

    # check forward files exist
    best_in_context = Path(outpath, "best_in_context.csv")
    best_sorted = Path(outpath, "best_sorted.csv")
    best_vrefs = Path(outpath, "best_vref_scores.csv")
    assert best_in_context.exists()
    assert best_sorted.exists()
    assert best_vrefs.exists()

    # delete the files
    if delete_files:
        best_in_context.unlink()
        best_sorted.unlink()
        best_vrefs.unlink()

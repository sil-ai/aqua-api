import os
import pytest

from pathlib import Path
import align
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912-some-missing.txt"), Path("fixtures/en-KJV-some-missing.txt")),
                                                    ])
def test_write_condensed_files(source, target, remove_files=True):
    outpath = source.parent / 'out' / f"{source.stem}_{target.stem}"
    align.write_condensed_files(source, target, outpath)

    # check that files exist
    assert os.path.exists(source)
    assert os.path.exists(target)
    assert os.path.exists(outpath / f"{source.stem}_condensed.txt")
    assert os.path.exists(outpath /  f"{target.stem}_condensed.txt")

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
    if remove_files:
        os.remove(outpath / f"{source.stem}_condensed.txt")
        os.remove(outpath / f"{target.stem}_condensed.txt")

@pytest.mark.parametrize("source,target", [
                                                    ("fixtures/src.txt", "fixtures/trg.txt"), 
                                                    ("fixtures/de-LU1912.txt", "fixtures/en-KJV.txt"),
                                                    ("fixtures/de-LU1912-some-missing.txt", "fixtures/en-KJV-some-missing.txt"),
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

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True), 
                                                    ])
def test_get_translation_scores(source, target, is_bible):
    outpath = source.parent / 'out' / f"{source.stem}_{target.stem}"
    align.write_condensed_files(source, target, outpath)
    vrefs = align.get_vrefs(source, target, is_bible=is_bible)
    corpus = align.create_corpus(outpath / f'{source.stem}_condensed.txt', outpath / f'{target.stem}_condensed.txt')
    model = align.train_model(corpus)
    df = align.get_translation_scores(model, corpus, vrefs)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@pytest.mark.parametrize("df_source", [
                                                    Path("fixtures/df_translation_scores_src_trg.csv"), 
                                                    Path("fixtures/df_translation_scores_de-LU1912-mini_en-KJV-mini.csv"), 
                                                    ])
def test_remove_duplicates(df_source):
    df = pd.read_csv(df_source)
    df = align.remove_duplicates(df)
    print(df)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert max(df['translation_score']) <= 1
    assert min(df['translation_score']) >= 0
    assert len(df['translation_score'].unique()) >= 20
    df.to_csv(df_source.parent / f'df_remove_duplicates_output_{df_source.stem.split("_",)[-2]}_{df_source.stem.split("_",)[-1]}.csv')


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
                                            (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt")), 

                                            ])
def test_run_align(source, target, remove_files=True):
    outpath = source.parent / 'out' / f"{source.stem}_{target.stem}"
    is_bible = False

    align.run_align(source, target, outpath, is_bible=is_bible)

    # check the files exist
    in_context = Path(outpath, "all_in_context.csv")
    sorted_f = Path(outpath, "all_sorted.csv")
    assert in_context.exists()
    assert sorted_f.exists()
    
    if remove_files:
        # delete the files
        in_context.unlink()
        sorted_f.unlink()

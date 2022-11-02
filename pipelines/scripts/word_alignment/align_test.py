import os
import pytest

from pathlib import Path
import align
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel

@pytest.mark.parametrize("source,target", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912-some-missing.txt"), Path("fixtures/en-KJV-some-missing.txt")),
                                                    ])
def test_write_condensed_files(source, target):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    align.write_condensed_files(source, target, outpath)

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
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt")), 
                                                    (Path("fixtures/de-LU1912.txt"), Path("fixtures/en-KJV.txt")),
                                                    (Path("fixtures/de-LU1912-some-missing.txt"), Path("fixtures/en-KJV-some-missing.txt")),
                                                    ])
def test_create_corpus(source, target):
    outpath =  source.parent / 'out' / f'{source.stem}_{target.stem}'
    align.write_condensed_files(source, target, outpath)
    corpus = align.create_corpus(outpath / f"{source.stem}_condensed.txt", outpath / f"{target.stem}_condensed.txt")

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
                                            (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt")), 
                                            (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt")),
                                            ])
def test_train_model(source, target):
    outpath =  source.parent / 'out' / f'{source.stem}_{target.stem}'
    condensed_source, condensed_target = align.write_condensed_files(source, target, outpath)
    corpus = align.create_corpus(condensed_source, condensed_target)
    model = align.train_model(corpus)
    assert model is not None
    assert isinstance(model, ThotSymmetrizedWordAlignmentModel)
    (outpath / f'{source.stem}_condensed.txt').unlink()
    (outpath / f'{target.stem}_condensed.txt').unlink()

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True), 
                                                    ])
def test_get_translation_scores(source, target, is_bible):
    outpath =  source.parent / 'out' / f'{source.stem}_{target.stem}'
    condensed_source, condensed_target = align.write_condensed_files(source, target, outpath)
    corpus = align.create_corpus(condensed_source, condensed_target)
    vrefs = align.get_ref_df(source, target, is_bible=is_bible)
    model = align.train_model(corpus)
    df = align.get_translation_scores(model, corpus, vrefs)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    (outpath / f'{source.stem}_condensed.txt').unlink()
    (outpath / f'{target.stem}_condensed.txt').unlink()
    df.to_csv(outpath / 'df_translation_scores.csv')


@pytest.mark.parametrize("outpath", [
                                                    Path("fixtures/out/es-test_en-test"), 
                                                    Path("fixtures/out/de-LU1912-mini_en-KJV-mini"), 
                                                    ])
def test_remove_duplicates(outpath):
    df = pd.read_csv(outpath / 'df_translation_scores.csv')
    df = align.remove_duplicates(df)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert max(df['translation_score']) <= 1
    assert min(df['translation_score']) >= 0
    assert len(df['translation_score'].unique()) >= 20
    (outpath / 'df_translation_scores.csv').unlink()

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True),
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

    vrefs = align.get_ref_df(source, target, is_bible)
    assert type(vrefs) == list
    assert len(vrefs) <= min(len(src_non_empty), len(trg_non_empty))

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                            (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True), 

                                            ])
def test_run_align(source, target, is_bible, remove_files=True):
    outpath = source.parent / 'out' / f"{source.stem}_{target.stem}"

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

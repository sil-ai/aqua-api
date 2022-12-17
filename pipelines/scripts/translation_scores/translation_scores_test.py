import os
import sys
import pytest

from pathlib import Path
import pandas as pd
from machine.translation.thot import ThotSymmetrizedWordAlignmentModel

import get_data, translation_scores, train_fa_model

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_write_condensed_files(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    outpath.mkdir(exist_ok=True, parents=True)
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    condensed_source, condensed_target = get_data.write_condensed_files(ref_df, outpath)

    # check that files exist
    assert os.path.exists(source)
    assert os.path.exists(target)
    assert os.path.exists(condensed_source)
    assert os.path.exists(condensed_target)

    # open the files
    with open(source, "r") as f:
        src_data = f.readlines()
    with open(target, "r") as f:
        trg_data = f.readlines()
    with open(condensed_source, "r") as f:
        src_data_c = f.readlines()
    with open(condensed_target, "r") as f:
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
    condensed_source.unlink()
    condensed_target.unlink()

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_create_corpus(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    condensed_source, condensed_target = get_data.write_condensed_files(ref_df, outpath)
    corpus = get_data.create_corpus(condensed_source, condensed_target)

    assert corpus is not None
    first_item_source = next(corpus.to_tuples())[0]
    first_item_target = next(corpus.to_tuples())[1]
    assert isinstance(first_item_source, list)
    assert isinstance(first_item_target, list)
    if len(first_item_source) > 0:
        assert isinstance(first_item_source[0], str)
    if len(first_item_target) > 0:
        assert isinstance(first_item_target[0], str)
    condensed_source.unlink()
    condensed_target.unlink()

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_train_model(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    condensed_source, condensed_target = get_data.write_condensed_files(ref_df, outpath)
    corpus = get_data.create_corpus(condensed_source, condensed_target)
    model = train_fa_model.train_model(corpus)
    assert model is not None
    assert isinstance(model, ThotSymmetrizedWordAlignmentModel)
    condensed_source.unlink()
    condensed_target.unlink()

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_get_translation_scores(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    vrefs = list(ref_df['vref'])
    condensed_source, condensed_target = get_data.write_condensed_files(ref_df, outpath)
    corpus = get_data.create_corpus(condensed_source, condensed_target)
    model = train_fa_model.train_model(corpus)
    df = translation_scores.get_translation_scores(model, corpus, vrefs)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    condensed_source.unlink()
    condensed_target.unlink()
    df.to_csv(outpath / 'df_translation_scores.csv')


@pytest.mark.parametrize("outpath", [
                                            Path("fixtures/out/de-LU1912-mini_en-KJV-mini/"), 
                                            Path("fixtures/out/en-KJV-mini_es-NTV-mini"),
                                            Path("fixtures/out/es-NTV-mini_de-LU1912-mini"), 
                                            ])
def test_remove_duplicates(outpath):
    df = pd.read_csv(outpath / 'df_translation_scores.csv')
    df = translation_scores.remove_duplicates(df)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert max(df['translation_score']) <= 1
    assert min(df['translation_score']) >= 0
    assert len(df['translation_score'].unique()) >= 20
    (outpath / 'df_translation_scores.csv').unlink()


@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_run_align(source, target, is_bible):
    outpath =  Path('fixtures/out') / f'{source.stem}_{target.stem}'

    translation_scores.run_align(source, target, outpath, is_bible=is_bible)

    # check the files exist
    translation_scores_csv = Path(outpath, "translation_scores.csv")
    assert translation_scores_csv.exists()

import os
from pathlib import Path
import pytest
import json
import pandas as pd

import combined
import align
import align_best

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True), 
                                                    ])
def test_run_fa(source, target, is_bible, remove_files=True):
    # align all
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    combined.run_fa(
        source,
        target,
        outpath,
        is_bible=is_bible,
    )
    assert os.path.exists(outpath / "all_sorted.csv")
    assert os.path.exists(outpath / "all_in_context.csv")
    assert os.path.exists(outpath / "best_sorted.csv")
    assert os.path.exists(outpath / "best_in_context.csv")
    assert os.path.exists(outpath / "best_vref_scores.csv")
    
    if remove_files:
        os.remove(outpath / "all_sorted.csv")
        os.remove(outpath / "all_in_context.csv")
        os.remove(outpath / "best_sorted.csv")
        os.remove(outpath / "best_in_context.csv")
        os.remove(outpath / "best_vref_scores.csv")


@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True), 
                                                    ])
def test_run_match_words(source, target, is_bible, remove_files=True):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    combined.run_match_words(source, target, outpath, 0.0, 0, False)
    assert (outpath / "dictionary.json").exists()
    assert (outpath / "ref_df.csv").exists()
    if remove_files:
        (outpath / "ref_df.csv").unlink()

@pytest.mark.parametrize("source,target,source_word,target_word", [
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), 'gott', 'god'), 
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), 'le', 'the'), 

                                                    ])
def test_get_scores_from_match_dict(source, target, source_word, target_word):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    dictionary_file = outpath / "dictionary.json"
    with open(dictionary_file) as f:
        dictionary = json.load(f)
    assert source_word in dictionary
    jac_sim, match_count = combined.get_scores_from_match_dict(dictionary, source_word, target_word)
    assert isinstance(dictionary, dict)
    assert len(dictionary) > 10
    assert jac_sim > 0 and jac_sim <= 1
    assert match_count > 0
    dictionary_file.unlink()


@pytest.mark.parametrize("source, target,source_word,target_word,is_bible", [
                                                    (Path("fixtures/es-test.txt"), Path("fixtures/en-test.txt"), 'el', 'the', False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), 'gott', 'god', True), 
                                                    ])
def test_run_combine_results(source, target, source_word, target_word, is_bible):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    combined.run_fa(source, target, outpath, is_bible=is_bible)
    combined.run_match_words(source, target, outpath)
    combined.run_combine_results(outpath)
    df = pd.read_csv(outpath / "combined.csv")
    assert len(df) > 10
    assert len(df['translation_score'].unique()) > 10
    assert len(df['alignment_count'].unique()) > 5
    assert len(df['avg_aligned'].unique()) > 5
    assert max(df['avg_aligned']) <= 3  # This is average num of times aligned divided by average number of verses they co-occur in. Normally
                                        # less than 1.0, but can occasionally be > 1 if there are multiple aligned occurences in a single verse!
    assert min(df['avg_aligned']) >= 0
    assert len(df['jac_sim'].unique()) > 5
    assert df[(df['source'] == source_word) & (df['target'] == target_word)]['translation_score'].values[0] > 0.1
    assert df[(df['source'] == source_word) & (df['target'] == target_word)]['alignment_score'].values[0] > 0.1
    assert df[(df['source'] == source_word) & (df['target'] == target_word)]['jac_sim'].values[0] > 0.1
    (outpath / 'all_in_context.csv').unlink()
    (outpath / 'all_sorted.csv').unlink()
    (outpath / 'best_in_context.csv').unlink()
    (outpath / 'best_sorted.csv').unlink()
    (outpath / 'best_vref_scores.csv').unlink()
    (outpath / 'combined.csv').unlink()
    (outpath / 'dictionary.json').unlink()
    (outpath / 'ref_df.csv').unlink()

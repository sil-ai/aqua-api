import os
from pathlib import Path
import pytest
import json

import combined

@pytest.mark.parametrize("source,target,is_bible", [
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), False),
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
                                                    (Path("fixtures/src.txt"), Path("fixtures/trg.txt"), False), 
                                                    (Path("fixtures/de-LU1912-mini.txt"), Path("fixtures/en-KJV-mini.txt"), True), 
                                                    ])
def test_run_match_words(source, target, is_bible, remove_files=True):
    outpath = source.parent / 'out' / f'{source.stem}_{target.stem}'
    combined.run_match_words(source, target, outpath, 0.0, 0, False)
    assert (outpath / f"{source.stem}_{target.stem}-dictionary.json").exists()
    assert (outpath / f"{source.stem}_{target.stem}_ref_df.csv").exists()
    if remove_files:
        os.remove(outpath / f"{source.stem}_{target.stem}-dictionary.json")
        (outpath / f"{source.stem}_{target.stem}_ref_df.csv").unlink()

@pytest.mark.parametrize("dictionary_file,source_word,target_word", [
                                                    (Path(f"fixtures/de-LU1912-mini_en-KJV-mini-dictionary.json"), 'gott', 'god'), 
                                                    (Path(f"fixtures/src_trg-dictionary.json"), 'le', 'the'), 

                                                    ])
def test_get_scores_from_match_dict(dictionary_file, source_word, target_word):
    with open(dictionary_file) as f:
        dictionary = json.load(f)
    assert source_word in dictionary
    jac_sim, match_count = combined.get_scores_from_match_dict(dictionary, source_word, target_word)
    assert isinstance(dictionary, dict)
    assert len(dictionary) > 10
    assert jac_sim > 0 and jac_sim <= 1
    assert match_count > 0


@pytest.mark.parametrize("align_path,best_path,match_path,source_word,target_word", [
                                    (Path("fixtures/src_trg_all_sorted.csv"), Path("fixtures/src_trg_align_best-best_sorted.csv"), Path("fixtures/src_trg-dictionary.json"), 'el', 'the'), 
                                    (Path('fixtures/de-LU1912-mini_en-KJV-mini_align-all_sorted.csv'), Path('fixtures/de-LU1912-mini_en-KJV-mini_align_best-best_sorted.csv'), Path('fixtures/de-LU1912-mini_en-KJV-mini-dictionary.json'), 'gott', 'god')
                                                    ])
def test_combine_df(align_path, best_path, match_path, source_word, target_word):
    df = combined.combine_df(align_path, best_path, match_path)
    assert len(df) > 10
    assert len(df['translation_score'].unique()) > 10
    assert len(df['alignment_count'].unique()) > 5
    assert len(df['avg_aligned'].unique()) > 10
    assert max(df['avg_aligned']) <= 1
    assert min(df['avg_aligned']) >= 0
    assert len(df['jac_sim'].unique()) > 5
    assert df[(df['source' == source_word]) & (df['target' == target_word])]['translation_score'] > 0.3
    assert df[(df['source' == source_word]) & (df['target' == target_word])]['alignment_score'] > 0.3
    assert df[(df['source' == source_word]) & (df['target' == target_word])]['jac_sim'] > 0.3





# align_path, best_path, match_path = (Path('fixtures/de-LU1912-mini_en-KJV-mini_align-all_sorted.csv'), Path('fixtures/de-LU1912-mini_en-KJV-mini_align_best-best_sorted.csv'), Path('fixtures/de-LU1912-mini_en-KJV-mini-dictionary.json'))
# test_combine_df(align_path, best_path, match_path)
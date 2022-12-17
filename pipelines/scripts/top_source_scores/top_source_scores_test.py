from pathlib import Path
import json
import argparse

import pytest
import pandas as pd

import top_source_scores

@pytest.mark.parametrize("df_path", [
                                            Path("fixtures/in/de-LU1912-mini_en-KJV-mini/total_scores.csv"), 
                                            Path("fixtures/in/en-KJV-mini_es-NTV-mini/total_scores.csv"),
                                            Path("fixtures/in/es-NTV-mini_de-LU1912-mini/total_scores.csv") 
                                            ])
def test_remove_leading_and_trailing_blanks(df_path):
    col = 'total_score'
    df = pd.read_csv(df_path)
    reduced_df = top_source_scores.remove_leading_and_trailing_blanks(df, col)
    assert reduced_df.shape[0] <= df.shape[0]
    assert col in reduced_df.columns


@pytest.mark.parametrize("total_scores_path", [
                                            Path("fixtures/in/de-LU1912-mini_en-KJV-mini/total_scores.csv"), 
                                            Path("fixtures/in/en-KJV-mini_es-NTV-mini/total_scores.csv"),
                                            Path("fixtures/in/es-NTV-mini_de-LU1912-mini/total_scores.csv") 
                                            ])
def test_get_verse_scores(total_scores_path):
    total_scores = pd.read_csv(total_scores_path)
    verse_scores = top_source_scores.get_verse_scores(total_scores)
    assert verse_scores.shape[0] > 0
    assert verse_scores.shape[0] < total_scores.shape[0]
    assert 'total_score' in verse_scores.columns
    assert verse_scores['total_score'].sum() > 0

@pytest.mark.parametrize("total_scores_dir", [
                                            Path("fixtures/in/")
                                                ])
def test_main(total_scores_dir):
    for dir in total_scores_dir.iterdir():
        meta_file = dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        outpath = Path('fixtures/out') / f'{source_str}_{target_str}'
        outpath.mkdir(parents=True, exist_ok=True)
        total_scores = pd.read_csv(dir / 'total_scores.csv')
        top_source_scores_df = top_source_scores.get_verse_scores(total_scores)
        top_source_scores_df.to_csv(outpath / 'top_source_scores.csv', index=False)
        with open(outpath / 'meta.json', 'w') as f:
            json.dump(meta, f)

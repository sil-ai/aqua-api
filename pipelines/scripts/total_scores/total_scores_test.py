import argparse
from pathlib import Path
import json
from typing import Tuple

import pandas as pd
import pytest

import get_data, total_scores

@pytest.mark.parametrize("source,target,is_bible", [
                                            (Path("fixtures/in/sources/de-LU1912-mini/de-LU1912-mini.txt"), Path("fixtures/in/targets/en-KJV-mini/en-KJV-mini.txt"), True), 
                                            (Path("fixtures/in/sources/en-KJV-mini/en-KJV-mini.txt"), Path("fixtures/in/targets/es-NTV-mini/es-NTV-mini.txt"), True),
                                            (Path("fixtures/in/sources/es-NTV-mini/es-NTV-mini.txt"), Path("fixtures/in/targets/de-LU1912-mini/de-LU1912-mini.txt"), True) 
                                            ])
def test_create_empty_df(source, target, is_bible):
    df = total_scores.create_empty_df(source, target, is_bible)
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] > 0
    assert 'vref' in df.columns

@pytest.mark.parametrize("dictionary_path, source_word, target_word", [
                                            (Path("fixtures/in/match_contexts/de-LU1912-mini_en-KJV-mini/dictionary.json"), 'gott', 'god'), 
                                            (Path("fixtures/in/match_contexts/en-KJV-mini_es-NTV-mini/dictionary.json"), 'god', 'dios'),
                                            (Path("fixtures/in/match_contexts/es-NTV-mini_de-LU1912-mini/dictionary.json"), 'dios', 'gott') 
                                            ])
def test_get_scores_from_match_dict(
                                dictionary_path: Path, 
                                source_word: str, 
                                target_word: str,
                                normalized: bool=True,
                                ):
    with open(dictionary_path) as f:
        dictionary = json.load(f)
    jac_sim, match_count = total_scores.get_scores_from_match_dict(dictionary, source_word, target_word)
    assert jac_sim > 0
    assert jac_sim <= 1
    assert match_count > 0


@pytest.mark.parametrize("base_alignment_dir,base_translation_dir,base_embedding_dir,base_match_dir", [
                                            (Path("fixtures/in/alignment_scores"), 
                                            Path("fixtures/in/translation_scores"),
                                            Path("fixtures/in/embeddings"),
                                            Path("fixtures/in/match_contexts"))
                                            ])
def test_main(base_alignment_dir, base_translation_dir, base_embedding_dir, base_match_dir):
    for alignment_dir in base_alignment_dir.iterdir():
        meta_file = alignment_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        alignment_scores = pd.read_csv(alignment_dir / 'alignment_scores.csv')
        avg_alignment_scores = pd.read_csv(alignment_dir / 'avg_alignment_scores.csv')
        translation_scores = pd.read_csv(base_translation_dir / f'{source_str}_{target_str}/translation_scores.csv')
        embedding_scores = pd.read_csv(base_embedding_dir / f'{source_str}_{target_str}/embedding_scores.csv')
        with open(base_match_dir / f'{source_str}_{target_str}/dictionary.json') as f:
            match_scores = json.load(f)
        outpath = Path('fixtures/out') / f'{source_str}_{target_str}'
        outpath.mkdir(parents=True, exist_ok=True)
        alignment_scores['vref'] = alignment_scores['vref'].astype('object')  # Necessary for non-Bible, where vrefs are ints.
        alignment_scores = alignment_scores.merge(avg_alignment_scores, how = 'left', on=['source', 'target']).fillna(0)
        source = Path('fixtures/in') / f'sources/{source_str}/{source_str}.txt'
        target = Path('fixtures/in') / f'targets/{target_str}/{target_str}.txt'

        all_results = total_scores.create_empty_df(source, target, is_bible=True)
        all_results = all_results.merge(alignment_scores, how='left', on=['vref', 'source', 'target']).fillna(0)
        all_results = all_results.merge(translation_scores, how='left', on=['source', 'target'])
        all_results.loc[:, 'avg_aligned'] = all_results.apply(lambda row: row['alignment_count'] / row['co-occurrence_count'], axis = 1).astype('float16')
        all_results.loc[:, 'translation_score'] = all_results.loc[:, 'translation_score'].apply(lambda x: 0 if x < 0.00001 else x).astype('float16')
        all_results.loc[:, "match_score"] = get_data.faster_df_apply(all_results, lambda x: total_scores.get_scores_from_match_dict(match_scores, x["source"], x["target"], normalized=False)[0]).astype('float16')
        all_results = all_results.merge(embedding_scores, how='left', on=['source', 'target'])
        all_results.loc[:, 'total_score'] = get_data.faster_df_apply(all_results,lambda row: (row['avg_aligned'] + row['translation_score'] + row['alignment_score'] + row['match_score'] + row['embedding_score']) / 5)
        
        total_scores_df = all_results[['vref', 'source', 'target', 'total_score']]
        print(total_scores_df.head())
        total_scores_df.to_csv(outpath / 'total_scores.csv', index=False)

    with open(outpath / 'meta.json', 'w') as f:
                json.dump(meta, f)

    assert total_scores_df.shape[0] > 0
    assert total_scores_df['total_score'].sum() > 0

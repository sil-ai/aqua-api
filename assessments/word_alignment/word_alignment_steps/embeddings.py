from typing import Optional
from pathlib import Path
import argparse
import json
import math

import pandas as pd
import numpy as np

import word_alignment_steps.prepare_data as prepare_data


def get_embeddings(
        condensed_df: pd.DataFrame,
        source_index_cache: dict,
        target_index_cache: dict,
        weights_path: Optional[Path]=Path(__file__).parent.resolve() / 'encoder_weights.txt',
        ):
    word_dict_src = prepare_data.get_words_from_cache(source_index_cache)
    word_dict_trg = prepare_data.get_words_from_cache(target_index_cache)
    if not weights_path.exists():
        weights_path = Path('/data/models/encoder_weights.txt')  # For Modal
    weights = np.loadtxt(weights_path)
    for word in word_dict_src.values():
        word.get_encoding(weights)
    for word in word_dict_trg.values():
        word.get_encoding(weights)

    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    ref_df = get_data.get_words_from_txt_file(ref_df, Path('/tmp'))
    df = ref_df.explode('src_words').explode('trg_words')
    df = df[['src_words', 'trg_words']].drop_duplicates()
    if df.shape[0] > 0:
        df.loc[:, 'embedding_dist'] = df.apply(lambda row: word_dict_src[row['src_words']].get_norm_distance(word_dict_trg[row['trg_words']]), axis=1).astype('float16')
        df.loc[:, 'embedding_score'] = df['embedding_dist'].apply(lambda x: math.log1p(max(1-x, -0.99))).astype('float16')
    df = df[['src_words', 'trg_words', 'embedding_score']].rename(columns={'src_words': 'source', 'trg_words': 'target'})
    # df.to_csv(outpath / "embedding_scores.csv", index=False)
    return df

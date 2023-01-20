from pathlib import Path
import math

import pandas as pd
import numpy as np

import word_alignment_steps.prepare_data as prepare_data


def run_embeddings(
        condensed_df: pd.DataFrame,
        source_index_cache: dict,
        target_index_cache: dict,
        ):
    word_dict_src = prepare_data.get_words_from_cache(source_index_cache)
    word_dict_trg = prepare_data.get_words_from_cache(target_index_cache)
    weights_path = Path('/root/encoder_weights.txt') 
    weights = np.loadtxt(weights_path)
    for word in word_dict_src.values():
        word.get_encoding(weights)
    for word in word_dict_trg.values():
        word.get_encoding(weights)
    condensed_df.loc[:, 'src_list'] = condensed_df['src'].apply(lambda x: str(x).split())
    condensed_df.loc[:, 'trg_list'] = condensed_df['trg'].apply(lambda x: str(x).split())
    df = condensed_df.explode('src_list').explode('trg_list')
    df = df[['src_list', 'trg_list']].drop_duplicates()
    if df.shape[0] > 0:
        df.loc[:, 'embedding_dist'] = df.apply(lambda row: word_dict_src[row['src_list']].get_norm_distance(word_dict_trg[row['trg_list']]), axis=1).astype('float16')
        df.loc[:, 'embedding_score'] = df['embedding_dist'].apply(lambda x: math.log1p(max(1-x, -0.99))).astype('float16')
    df = df[['src_list', 'trg_list', 'embedding_score']].rename(columns={'src_list': 'source', 'trg_list': 'target'})
    
    return df

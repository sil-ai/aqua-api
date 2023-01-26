from typing import Dict

import pandas as pd

import word_alignment_steps.prepare_data as prepare_data


def create_index_cache(tokenized_df):
    tokenized_df.loc[:, 'src_list'] = tokenized_df['src_tokenized'].apply(lambda x: str(x).split())
    word_dict = get_indices_from_df(tokenized_df)
    index_cache = {key: value.index_list for key, value in word_dict.items()}
    return index_cache


def get_indices_from_df(tokenized_df: pd.DataFrame) -> Dict[str, prepare_data.Word]:
    """
    Takes a DataFrame and constructs a dictionary of Words from all the words in a column of that dataframe.
    Inputs:
    df          A dataframe containing the words
    Outputs:
    word_dict      A dictionary of {word (str): Word} items
    """
    all_source_words = list(tokenized_df['src_list'].explode().unique())
    word_dict = {word: prepare_data.Word(word) for word in all_source_words if type(word) == str}

    word_series = tokenized_df['src_list'].explode().apply(lambda x: prepare_data.normalize_word(x))
    for word in word_dict.values():
        word.get_indices(word_series)
    return word_dict

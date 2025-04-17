import math
import os
from typing import Dict

import modal
import pandas as pd
import word_alignment_steps.prepare_data as prepare_data

# Manage deployment suffix on modal endpoint if testing.
suffix = ""
if os.environ.get("MODAL_TEST") == "TRUE":
    suffix = "test"


suffix = f"-{suffix}" if len(suffix) > 0 else ""


async def create_index_cache(tokenized_df):
    tokenized_df.loc[:, "src_list"] = tokenized_df["src_tokenized"].apply(
        lambda x: str(x).split()
    )
    word_dict = await get_indices_from_df(tokenized_df)
    index_cache = {key: value.index_list for key, value in word_dict.items()}
    return index_cache


async def get_indices_from_df(
    tokenized_df: pd.DataFrame,
) -> Dict[str, prepare_data.Word]:
    """
    Takes a DataFrame and constructs a dictionary of Words from all the words in a column of that dataframe.
    Inputs:
    tokenized_df          A dataframe containing the words
    Outputs:
    word_dict      A dictionary of {word (str): Word} items
    """
    all_source_words = list(tokenized_df["src_list"].explode().unique())
    word_dict = {
        word: prepare_data.Word(word)
        for word in all_source_words
        if isinstance(word, str)
    }

    word_series = (
        tokenized_df["src_list"]
        .explode()
        .apply(lambda x: prepare_data.normalize_word(x))
    )

    # Depending on word count, split words into up to 100 batches
    batch_size = min(len(word_dict), math.ceil(len(word_dict) / 100))
    words = list(word_dict.values())
    words_batched = [
        words[i : i + batch_size] for i in range(0, len(word_dict), batch_size)
    ]

    # Run batches in parallel
    get_indices = modal.Function.lookup(f"word-alignment{suffix}", "get_indices")
    word_dict = {}
    async for words in get_indices.map.aio(
        words_batched, kwargs={"word_series": word_series}
    ):
        for word in words:
            word_dict[word.word] = word

    return word_dict

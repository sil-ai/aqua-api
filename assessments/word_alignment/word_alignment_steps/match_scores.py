import logging
import argparse
from typing import Tuple, List, Optional
import json

import pandas as pd
from collections import Counter
from pathlib import Path

import word_alignment_steps.prepare_data as prepare_data


def get_jaccard_similarity(set_1: set, set_2: set) -> float:
    """
    Gets the jacard similarity between two lists.
    Inputs:
    list1           First list
    list2           Second list

    Outputs:
    jac_sim         The Jaccard Similarity between the two sets - i.e. the size of the intersection divided by the
                    size of the union.
    """
    intersection = len(list(set(set_1).intersection(set_2)))
    union = (len(set_1) + len(set_2)) - intersection
    jac_sim = float(intersection) / union if union != 0 else 0
    return jac_sim


def get_correlations_between_sets(
    indexes_set_1: set,
    indexes_set_2: set,
) -> Tuple[float, int]:
    """
    Returns both the Jaccard Similarity between two sets, and the size of their intersection.
    Inputs:
    indexes_set_1:      A set of indexes, to be compared for correlation
    indexes_set_2:      A set of indexes, to be compared for correlation

    Outputs:
        jaccard similarity:     The jaccard similarity between the two sets
        intersection_count:     The number of overlapping items between the two sets
    """
    intersection_count = len(indexes_set_1 & indexes_set_2)
    jaccard_similarity = get_jaccard_similarity(indexes_set_1, indexes_set_2)
    return jaccard_similarity, intersection_count


def update_matches_for_lists(
    source_list: list,
    target_list: list,
    js_cache: dict,
    matches: dict,
    jaccard_similarity_threshold: float = 0.0,
    count_threshold: int = 0,
) -> Tuple[dict, dict]:
    """
    Updates the matches dictionary with any matches between the keys and values in a verse that pass the thresholds for significance.
    Inputs:
        source_list:          A list of source words in a particular verse, to be looped through
        target_list:        A list of target words in a particular verse, to be looped through
        js_cache:         A dictionary cache of the Jaccard similarities and counts between pairs of items already calculated
        matches:        A dictionary containing those matches which pass the thresholds for significance
        jaccard_similarity_threshold: (Default 0.0) The threshold for Jaccard Similarity for a match to be logged as significant
                                and entered into the matches dictionary
        count_threshold: (Default 0)    The threshold for count (number of occurences of the two items in the same verse) for a
                             match to be logged as significant and entered into the matches dictionary
    Outputs:
        matches:        An updated dictionary containing those matches which pass the thresholds for significance
        js_cache:         An updated dictionary cache of the Jaccard similarities between pairs of items already calculated
    """
    cache_counter = Counter()
    for source_item in source_list:
        for target_item in target_list:
            if target_item.normalized in source_item.matched:
                continue
            if (source_item.normalized, target_item.normalized) in js_cache:
                jaccard_similarity = js_cache[(source_item.normalized, target_item.normalized)][
                    "jaccard_similarity"
                ]
                count = js_cache[(source_item.normalized, target_item.normalized)]["count"]
                cache_counter.update(["Cached"])
            else:
                jaccard_similarity, count = get_correlations_between_sets(
                    set(source_item.reduced_index_list),
                    set(target_item.reduced_index_list),
                )
                js_cache[(source_item.normalized, target_item.normalized)] = {
                    "jaccard_similarity": jaccard_similarity,
                    "count": count,
                }
                cache_counter.update(["Calculated"])
            source_item.matched.append(target_item.normalized)
            if (
                jaccard_similarity > jaccard_similarity_threshold
                and count > count_threshold
            ):
                if source_item.normalized not in matches:
                    matches[source_item.normalized] = []

                matches[source_item.normalized].append(
                    {
                        "value": target_item.normalized,
                        "jaccard_similarity": jaccard_similarity,
                        "count": count,
                    }
                )
    return (matches, js_cache)
    

def run_match_scores(
            condensed_df: pd.DataFrame,
            source_index_cache: dict,
            target_index_cache: dict,
            jaccard_similarity_threshold: float = 0.0,
            count_threshold: int = 0,
            ) -> pd.DataFrame:
    """
    Runs the matching algorithm, taking source and target text files and returning a dictionary.json file of matches.
    Inputs:
    source          Path to source txt file
    target          Path to target txt file
    outpath         Output path
    logging_level   Logging level. Default is 'INFO'
    jaccard_similarity_threshold    Jaccard Similarity threshold for being included in the dictionary
    count_threshold                 Count threshold for being included in the dictionary
    refresh_cache    Whether to force a cache refresh or use any cached data from previous runs on this source and/or target

    """
    # condensed_df.loc[:, 'src_list'] = condensed_df['src_tokenized'].apply(lambda x: str(x).split())

    # freq_cache_file = cache_dir / f"{source.stem}-{target.stem}-freq-cache.json"
    # freq_cache = get_data.initialize_cache(freq_cache_file, to_tuples=True, refresh=refresh_cache)
    freq_cache = {}    
    word_dict_src = prepare_data.get_words_from_cache(source_index_cache)
    word_dict_trg = prepare_data.get_words_from_cache(target_index_cache)

    # ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    # condensed_df = get_data.condense_files(ref_df)
    # condensed_df = get_data.get_words_from_txt_file(condensed_df, outpath)
    condensed_df.loc[:, 'src_list'] = condensed_df['src'].apply(lambda x: str(x).split())
    condensed_df.loc[:, 'trg_list'] = condensed_df['trg'].apply(lambda x: str(x).split())
    condensed_df.loc[:, 'normalized_src_words'] = condensed_df['src'].apply(lambda x: prepare_data.normalize_word(x).split())
    condensed_df.loc[:, 'normalized_trg_words'] = condensed_df['trg'].apply(lambda x: prepare_data.normalize_word(x).split())
    
    condensed_df_indexes = list(condensed_df.index)  
    for word_object in word_dict_src.values():
        word_object.reduced_index_list = list(set(word_object.index_list).intersection(set(condensed_df_indexes)))
    for word_object in word_dict_trg.values():
        word_object.reduced_index_list = list(set(word_object.index_list).intersection(set(condensed_df_indexes)))
    
    matches = {}
    print("Getting matches...")
    for _, row in condensed_df.iterrows():
        paired_source_objects: List[prepare_data.Word] = list(set([word_dict_src[x] for x in row["src_list"]]))
        paired_target_objects: List[prepare_data.Word] = list(set([word_dict_trg[x] for x in row["trg_list"]]))
        matches, freq_cache = update_matches_for_lists(
            paired_source_objects,
            paired_target_objects,
            matches=matches,
            js_cache=freq_cache,
            jaccard_similarity_threshold=jaccard_similarity_threshold,
            count_threshold=count_threshold,
        )
    # get_data.write_dictionary_to_file(freq_cache, freq_cache_file, to_strings=True)
    # get_data.write_dictionary_to_file(matches, matches_file)
    flat_list = [
       {'source': k, **x}
        for k, v in matches.items()
        for x in v
    ]
    df = pd.DataFrame(flat_list)

    return df


    

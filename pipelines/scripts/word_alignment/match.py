import logging
import argparse
from typing import Tuple, List, Optional

import pandas as pd
from tqdm import tqdm
from collections import Counter
from pathlib import Path

import get_data


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
                jaccard_similarity, count = get_data.get_correlations_between_sets(
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
    

def run_match(
            source: Path,
            target: Path,
            outpath: Path,
            source_index_cache_file: Optional[Path]=None,
            target_index_cache_file: Optional[Path]=None,
            word_dict_src: Optional[dict]=None,
            word_dict_trg: Optional[dict]=None,
            jaccard_similarity_threshold: float = 0.0,
            count_threshold: int = 0,
            refresh_cache: bool = False,
            is_bible: bool=True,
            ) -> None:
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
    outpath.mkdir(parents=True, exist_ok=True)
    cache_dir = outpath.parent / "cache"
    if not cache_dir.exists():
        cache_dir.mkdir(exist_ok=True)

    if not source_index_cache_file:
        source_index_cache_file = cache_dir / f'{source.stem}-index-cache.json'
    if not target_index_cache_file:
        target_index_cache_file = cache_dir / f'{target.stem}-index-cache.json'

    freq_cache_file = cache_dir / f"{source.stem}-{target.stem}-freq-cache.json"
    freq_cache = get_data.initialize_cache(freq_cache_file, to_tuples=True, refresh=refresh_cache)
    matches_file = outpath / "dictionary.json"
    
    if word_dict_src == None:
        word_dict_src = get_data.create_words(source, source_index_cache_file, outpath, is_bible=is_bible, refresh_cache=refresh_cache)
    if word_dict_trg == None:
        word_dict_trg = get_data.create_words(target, target_index_cache_file, outpath, is_bible=is_bible, refresh_cache=refresh_cache)

    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    condensed_df = get_data.condense_files(ref_df)
    condensed_df = get_data.get_words_from_txt_file(condensed_df, outpath)
    condensed_df.loc[:, 'normalized_src_words'] = condensed_df['src'].apply(lambda x: get_data.normalize_word(x).split())
    condensed_df.loc[:, 'normalized_trg_words'] = condensed_df['trg'].apply(lambda x: get_data.normalize_word(x).split())
    
    logging.info(f"ref_df: {condensed_df.head(25)}")
    
    condensed_df_indexes = list(condensed_df.index)  
    for word_object in word_dict_src.values():
        word_object.reduced_index_list = list(set(word_object.index_list).intersection(set(condensed_df_indexes)))
    for word_object in word_dict_trg.values():
        word_object.reduced_index_list = list(set(word_object.index_list).intersection(set(condensed_df_indexes)))
    
    matches = {}
    print("Getting matches...")
    for _, row in tqdm(condensed_df.iterrows(), total=condensed_df.shape[0]):
        paired_source_objects: List[get_data.Word] = list(set([word_dict_src[x] for x in row["src_words"]]))
        paired_target_objects: List[get_data.Word] = list(set([word_dict_trg[x] for x in row["trg_words"]]))
        matches, freq_cache = update_matches_for_lists(
            paired_source_objects,
            paired_target_objects,
            matches=matches,
            js_cache=freq_cache,
            jaccard_similarity_threshold=jaccard_similarity_threshold,
            count_threshold=count_threshold,
        )
    get_data.write_dictionary_to_file(freq_cache, freq_cache_file, to_strings=True)
    get_data.write_dictionary_to_file(matches, matches_file)
    logging.info("END RUN")
    return (word_dict_src, word_dict_trg)


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    tqdm.pandas()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'", required=True,)
    parser.add_argument("--target", type=Path, help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'", required=True)
    parser.add_argument("--jaccard-similarity-threshold", type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.05)
    parser.add_argument("--count-threshold", type=int, help="Threshold for count (number of co-occurences) score to be significant", default=0)
    # parser.add_argument("--logging-level", type=str, help="Logging level, default is INFO", default="INFO")
    parser.add_argument("--refresh-cache", action="store_true", help="Refresh and overwrite the existing cache")
    parser.add_argument("--outpath", type=Path, help="Output path for matches")
    args = parser.parse_args()
    outpath = args.outpath / f'{args.source.stem}_{args.target.stem}'
    
    run_match(
        args.source,
        args.target,
        outpath,
        jaccard_similarity_threshold = args.jaccard_similarity_threshold,
        count_threshold = args.count_threshold,
        refresh_cache = args.refresh_cache,
    )

import logging
import argparse
from typing import Tuple, List

import pandas as pd
from tqdm import tqdm
from collections import Counter
from pathlib import Path

import get_data





# def text_to_words(text: str) -> List[str]:
#     """
#     Takes a sentence, line or verse of text, tokenizes and "normalizes" it and returns a list of words.
#     Inputs:
#         text:   Normally a sentence, or Bible verse
#     Outputs:
#         A list of words, where the sentence has had its punctuation removed, and words splits into a list of words
#     """
#     word_tokenizer = LatinWordTokenizer()
#     word_list = [normalize_word(word) for word in word_tokenizer.tokenize(text)]
#     # remove any blanks and make character replacements from replace_dict
#     word_list = [align.replace_chars(word) for word in word_list]
#     return word_list


# def get_text_data(source: Path) -> pd.DataFrame:
#     """
#     Takes the text source as an input, and returns a dataframe of the text.
#     Inputs:
#         source:      Path to input txt file
#     Outputs:
#         df:         A dataframe with the text in one column and the separate words in another
#     """
#     with open(source, "r") as f:
#         data = f.readlines()
#     words = [text_to_words(line) for line in data]
#     normalized_words = [
#         [normalize_word(word) for word in word_list] for word_list in words
#     ]
#     df = pd.DataFrame(
#         {"text": data, "words": words, "normalized_words": normalized_words}
#     )
#     df = df[df["text"].apply(lambda x: len(x) > 2)]
#     df = df[df["text"] != "b'\n'"]
#     return df


# def get_indices_with_item(item: str, list_series: pd.Series) -> List[pd.Index]:
#     """
#     Returns indices from a series of lists, filtered by rows whose list contains a particular item
#     Inputs:
#     item:           A single item from list_series
#     list_series:     A series containing the lists to filter by list_item
    
#     Outputs:
#     index_list    A list of indices for the list_series series, corresponding to rows that contain item
#     """
#     index_list = list(
#         list_series[
#             list_series.apply(lambda x: item in x if isinstance(x, Iterable) else False)
#         ].index
#     )
#     return index_list


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





# def get_single_df(
#     source: Path,
#     list_name: str,
# ) -> pd.DataFrame:
#     """
#     Reads a text file and creates a dataframe corresponding to either the keys or values being investigated.
#     The 'normalized_words' columns of the dataframe is renamed to either 'keys' or 'values'.
#     Inputs:
#         source:    Name of the dataframe
#         list_name:  The name that should be used for the df column with the relevant verse lists. Normally 'keys' or 'values'.
#     Outputs:
#         df:         The dataframe containing text, and either a 'keys' or 'values' column with the relevant list data.
#     """
#     df = get_text_data(source)
#     df = df.rename(columns={"normalized_words": list_name})
#     return df


# def get_combined_df(
#     source: Path,
#     target: Path,
#     outpath: Path,
# ) -> pd.DataFrame:
#     """
#     Takes the source and target text files and creates ref_df - a single dataframe with aligned words from both inputs.
#     Inputs:
#         source: Path to the source txt file
#         target: Path to the target txt file
#         outpath:            The output file path
#     Outputs:
#         ref_df:     A dataframe that combines the keys and values data into a single dataframe by line
#     """
#     if not outpath.exists():
#         outpath.mkdir(exist_ok=True)

#     source_ref_df = get_single_df(
#         source,
#         list_name="source",
#     )
#     target_ref_df = get_single_df(
#         target,
#         list_name="target",
#     )

#     target_ref_series = target_ref_df["target"]
#     ref_df = pd.concat([source_ref_df, target_ref_series], axis=1)
#     ref_df = ref_df.dropna(subset=["source", "target"])
#     ref_df.to_csv(outpath / "ref_df.csv")
#     logging.info(ref_df.head())
#     return (source_ref_df, target_ref_df, ref_df)


def run_match(
            source: Path,
            target: Path,
            outpath: Path,
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
    source_list_name = source.stem
    target_list_name = target.stem

    outpath.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        format="%(asctime)s - %(funcName)20s() - %(message)s",
        level='INFO',
        filename=f"{outpath}/match_words_in_aligned_verse.log",
        filemode="a",
        force=True,
    )
    logging.info("START RUN")

    cache_dir = outpath.parent / "cache"
    cache_dir.mkdir(exist_ok=True)
    js_cache_file = cache_dir / f"{source_list_name}-{target_list_name}-freq-cache.json"
    reverse_freq_cache_file = (
        cache_dir / f"{target_list_name}-{source_list_name}-freq-cache.json"
    )
    matches_file = outpath / "dictionary.json"

    js_cache = get_data.initialize_cache(js_cache_file, to_tuples=True, refresh=refresh_cache)
    js_cache_reverse = get_data.initialize_cache(
        reverse_freq_cache_file, reverse=True, to_tuples=True
    )
    js_cache = {**js_cache, **js_cache_reverse}
    word_dict_src = get_data.create_words(source, cache_dir, outpath, is_bible=is_bible, refresh_cache=refresh_cache)
    word_dict_trg = get_data.create_words(target, cache_dir, outpath, is_bible=is_bible, refresh_cache=refresh_cache)
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
        # for paired_object in [*paired_source_objects, *paired_target_objects]:
            # paired_object.index_list = set(paired_object.index_list).intersection(set(ref_df_indexes))
        matches, js_cache = update_matches_for_lists(
            paired_source_objects,
            paired_target_objects,
            matches=matches,
            js_cache=js_cache,
            jaccard_similarity_threshold=jaccard_similarity_threshold,
            count_threshold=count_threshold,
        )
    get_data.write_dictionary_to_file(js_cache, js_cache_file, to_strings=True)
    get_data.write_dictionary_to_file(matches, matches_file)
    logging.info("END RUN")
    return (word_dict_src, word_dict_trg)


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    pd.set_option("display.max_rows", 500)
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
        args.jaccard_similarity_threshold,
        args.count_threshold,
        args.refresh_cache,
    )

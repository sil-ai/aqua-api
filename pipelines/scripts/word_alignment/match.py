import logging
import json
import argparse
import re
from typing import Iterable, Tuple, List

import pandas as pd
from tqdm import tqdm
from collections import Counter
from pathlib import Path
from machine.tokenization import LatinWordTokenizer


def write_dictionary_to_file(
    dictionary: dict, filename: str, to_strings: bool = False
) -> None:
    """
    Takes a dictionary and writes it to a json file.
    Inputs:
    dictionary          Dictionary to be written
    filename            Filename to write to
    to_strings          Whether the keys should be converted to strings, e.g. because json doesn't support tuple keys
    
    """
    if to_strings:
        dictionary = tuple_keys_to_string(dictionary)
    with open(filename, "w", encoding="utf8") as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=4)
    logging.info(f"Written file {filename}")


def text_to_words(text: str) -> List[str]:
    """
    Takes a sentence, line or verse of text, tokenizes and "normalizes" it and returns a list of words.
    Inputs:
        text:   Normally a sentence, or Bible verse
    Outputs:
        A list of words, where the sentence has had its punctuation removed, and words splits into a list of words
    """
    word_tokenizer = LatinWordTokenizer()
    word_list = [normalize_word(word) for word in word_tokenizer.tokenize(text)]
    # remove any blanks
    word_list = [word for word in word_list if word]

    return word_list


def normalize_word(word: str) -> str:
    """
    Strips the punctuation from words. Note that in some languages this punctuation includes important meaning from the word!
    But this is used to "normalize" words, and group together words that are similar (and often have similar meanings) but not identical.
    Inputs:
    word        The string to normalize

    Outputs:
    word        Normalized string
    """
    word = re.sub("[^\w\s]", "", word.lower()) if word else ""    #  Gives 18,159 unique Hebrew ords in the OT, rather than 87,000
    return word


def get_text_data(source: Path) -> pd.DataFrame:
    """
    Takes the text source as an input, and returns a dataframe of the text.
    Inputs:
        source:      Path to input txt file
    Outputs:
        df:         A dataframe with the text in one column and the separate words in another
    """
    with open(source, "r") as f:
        data = f.readlines()
    words = [text_to_words(line) for line in data]
    normalized_words = [
        [normalize_word(word) for word in word_list] for word_list in words
    ]
    df = pd.DataFrame(
        {"text": data, "words": words, "normalized_words": normalized_words}
    )
    df = df[df["text"].apply(lambda x: len(x) > 2)]
    df = df[df["text"] != "b'\n'"]
    return df


def get_indices_with_item(item: str, list_series: pd.Series) -> List[pd.Index]:
    """
    Returns indices from a series of lists, filtered by rows whose list contains a particular item
    Inputs:
    item:           A single item from list_series
    list_series:     A series containing the lists to filter by list_item
    
    Outputs:
    index_list    A list of indices for the list_series series, corresponding to rows that contain item
    """
    index_list = list(
        list_series[
            list_series.apply(lambda x: item in x if isinstance(x, Iterable) else False)
        ].index
    )
    return index_list


def get_jaccard_similarity(list1: list, list2: list) -> float:
    """
    Gets the jacard similarity between two lists.
    Inputs:
    list1           First list
    list2           Second list

    Outputs:
    jac_sim         The Jaccard Similarity between the two sets - i.e. the size of the intersection divided by the
                    size of the union.
    """
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(set(list1)) + len(set(list2))) - intersection
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
    intersection_count = len(list(indexes_set_1 & indexes_set_2))
    jaccard_similarity = get_jaccard_similarity(indexes_set_1, indexes_set_2)
    return jaccard_similarity, intersection_count


def update_matches_for_lists(
    keys_list: list,
    values_list: list,
    js_cache: dict,
    matches: dict,
    keys_index: dict,
    values_index: dict,
    jaccard_similarity_threshold: float = 0.0,
    count_threshold: int = 0,
) -> Tuple[dict, dict]:
    """
    Updates the matches dictionary with any matches between the keys and values in a verse that pass the thresholds for significance.
    Inputs:
        keys_list:          A list of keys in a particular verse, to be looped through
        values_list:        A list of values in a particular verse, to be looped through
        js_cache:         A dictionary cache of the Jaccard similarities and counts between pairs of items already calculated
        matches:        A dictionary containing those matches which pass the thresholds for significance
        keys_index:     A cache of the indices of ref_df where keys_item appears
        values_index:     A cache of the indices of ref_df where values_item appears
        jaccard_similarity_threshold: (Default 0.0) The threshold for Jaccard Similarity for a match to be logged as significant
                                and entered into the matches dictionary
        count_threshold: (Default 0)    The threshold for count (number of occurences of the two items in the same verse) for a
                             match to be logged as significant and entered into the matches dictionary
    Outputs:
        matches:        An updated dictionary containing those matches which pass the thresholds for significance
        js_cache:         An updated dictionary cache of the Jaccard similarities between pairs of items already calculated
    """
    cache_counter = Counter()
    for keys_item in keys_list:
        for values_item in list(set(values_list)):
            if (keys_item, values_item) in js_cache:
                jaccard_similarity = js_cache[(keys_item, values_item)][
                    "jaccard_similarity"
                ]
                count = js_cache[(keys_item, values_item)]["count"]
                cache_counter.update(["Cached"])
            else:
                jaccard_similarity, count = get_correlations_between_sets(
                    set(keys_index.get(keys_item, [])),
                    set(values_index.get(values_item, [])),
                )
                js_cache[(keys_item, values_item)] = {
                    "jaccard_similarity": jaccard_similarity,
                    "count": count,
                }
                cache_counter.update(["Calculated"])
            if (
                jaccard_similarity > jaccard_similarity_threshold
                and count > count_threshold
            ):
                if keys_item not in matches:
                    matches[keys_item] = []
                # if values_item not in reverse_matches:
                #     reverse_matches[values_item] = []

                if values_item not in [
                    item.get("value", "") for item in matches[keys_item]
                ]:  # matches[keys_item][:]['value']:
                    matches[keys_item].append(
                        {
                            "value": values_item,
                            "jaccard_similarity": jaccard_similarity,
                            "count": count,
                        }
                    )
                # reverse_matches[values_item].append(
                #         {
                #             "value": keys_item,
                #             "jaccard_similarity": jaccard_similarity,
                #             "count": count,
                #         }
                # )
    return (matches, js_cache)


def tuple_keys_to_string(dictionary: dict) -> dict:
    """
    Changes the tuple keys of a dictionary into strings, to they can be saved as json, and returns the dictionary
    """
    return {f"{key[0]}-{key[1]}": value for key, value in dictionary.items()}


def string_keys_to_tuple(dictionary: dict) -> dict:
    """
    Changes the string keys from a json file back to tuples, and returns the dictionary
    """
    return {
        (key.split("-")[0], key.split("-")[1]): value
        for key, value in dictionary.items()
    }


def initialize_cache(
    cache_file: Path,
    to_tuples: bool = False,
    reverse: bool = False,
    refresh: bool = False,
) -> dict:
    """
    Either reads a cache file from a json file or creates an empty dictionary to use as a cache file.
    Inputs:
        cache_file:     Name of the file to read from (if it exists)
        to_tuples:      If the keys of the json file need to be converted to tuples
        reverse:        If the keys and values should be switched
        refresh:        Returns a blank cache dictionary, even if there is an existing one at cache_file
    Returns:
        cache:          A dictionary to be used as a cache
    """
    if cache_file.exists() and not refresh:
        with open(cache_file, "r") as f:
            cache = json.load(f)
            if to_tuples:
                cache = string_keys_to_tuple(cache)
            if reverse:
                cache = {(key[1], key[0]): value for key, value in cache.items()}
    else:
        cache = {}
    return cache


def get_single_df(
    source: Path,
    list_name: str = "keys",
) -> pd.DataFrame:
    """
    Reads a text file and creates a dataframe corresponding to either the keys or values being investigated.
    The 'normalized_words' columns of the dataframe is renamed to either 'keys' or 'values'.
    Inputs:
        source:    Name of the dataframe
        list_name:  The name that should be used for the df column with the relevant verse lists. Normally 'keys' or 'values'.
    Outputs:
        df:         The dataframe containing text, and either a 'keys' or 'values' column with the relevant list data.
    """
    df = get_text_data(source)
    df = df.rename(columns={"normalized_words": list_name})
    return df


def get_combined_df(
    source: Path,
    target: Path,
    outpath: Path,
) -> pd.DataFrame:
    """
    Takes the source and target text files and creates ref_df - a single dataframe with aligned words from both inputs.
    Inputs:
        source: Path to the source txt file
        target: Path to the target txt file
        outpath:            The output file path
    Outputs:
        ref_df:     A dataframe that combines the keys and values data into a single dataframe by line
    """
    if not outpath.exists():
        outpath.mkdir(exist_ok=True)

    keys_ref_df = get_single_df(
        source,
        list_name="keys",
    )
    values_ref_df = get_single_df(
        target,
        list_name="values",
    )

    values_ref_series = values_ref_df["values"]
    ref_df = pd.concat([keys_ref_df, values_ref_series], axis=1)
    ref_df = ref_df.dropna(subset=["keys", "values"])
    ref_df.to_csv(outpath / "ref_df.csv")
    logging.info(ref_df.head())
    return ref_df


def run_match(
            source: Path,
            target: Path,
            outpath: Path,
            logging_level: str = 'INFO',
            jaccard_similarity_threshold: float = 0.0,
            count_threshold: int = 0,
            refresh_cache: bool = False,
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
    keys_list_name = source.stem
    values_list_name = target.stem

    if not outpath.exists():
        outpath.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        format="%(asctime)s - %(funcName)20s() - %(message)s",
        level=logging_level.upper(),
        filename=f"{outpath}/match_words_in_aligned_verse.log",
        filemode="a",
    )
    logging.info("START RUN")

    cache_dir = outpath / "cache"
    cache_dir.mkdir(exist_ok=True)
    js_cache_file = cache_dir / f"{keys_list_name}-{values_list_name}-freq-cache.json"
    reverse_freq_cache_file = (
        cache_dir / f"{values_list_name}-{keys_list_name}-freq-cache.json"
    )
    keys_index_cache_file = cache_dir / f"{keys_list_name}-index-cache.json"
    values_index_cache_file = cache_dir / f"{values_list_name}-index-cache.json"

    matches_file = outpath / "dictionary.json"

    ref_df = get_combined_df(source, target, outpath)
    logging.info(f"Total lines: {len(ref_df)}")

    js_cache = initialize_cache(js_cache_file, to_tuples=True, refresh=refresh_cache)
    js_cache_reverse = initialize_cache(
        reverse_freq_cache_file, reverse=True, to_tuples=True
    )
    js_cache = {**js_cache, **js_cache_reverse}

    if refresh_cache or not keys_index_cache_file.exists():
        keys_index = {}
        print("Getting sentences that contain each word in keys")
        for word in tqdm(list(ref_df["keys"].explode().unique())):
            keys_index[word] = get_indices_with_item(word, ref_df["keys"])
        write_dictionary_to_file(keys_index, keys_index_cache_file)
    else:
        keys_index = initialize_cache(keys_index_cache_file, refresh=False)

    if refresh_cache or not values_index_cache_file.exists():
        values_index = {}
        print("Getting sentences that contain each word in values")
        for word in tqdm(list(ref_df["values"].explode().unique())):
            values_index[word] = get_indices_with_item(word, ref_df["values"])
        write_dictionary_to_file(values_index, values_index_cache_file)
    else:
        values_index = initialize_cache(values_index_cache_file, refresh=False)

    ref_df = ref_df.dropna(subset=["keys", "values"])  # Reduce ref_df to only lines present in both texts
    logging.info(f"ref_df: {ref_df}")
    
    ref_df_indexes = list(ref_df.index)  
    # Reduce the keys_index and values_index dicts to only those lines present in the reduced ref_df
    print("Getting keys_index")
    keys_index = {
        key: [item for item in values if item in ref_df_indexes]
        for key, values in tqdm(keys_index.items())
    }
    print("Getting values_index")
    values_index = {
        key: [item for item in values if item in ref_df_indexes]
        for key, values in tqdm(values_index.items())
    }

    matches = {}
    print("Getting matches...")
    for index, row in tqdm(ref_df.iterrows(), total=ref_df.shape[0]):
        keys: List[str] = list(set(row["keys"]))
        values: List[str] = list(set(row["values"]))
        matches, js_cache = update_matches_for_lists(
            keys,
            values,
            matches=matches,
            js_cache=js_cache,
            keys_index=keys_index,
            values_index=values_index,
            jaccard_similarity_threshold=jaccard_similarity_threshold,
            count_threshold=count_threshold,
        )
    write_dictionary_to_file(js_cache, js_cache_file, to_strings=True)
    write_dictionary_to_file(matches, matches_file)
    logging.info("END RUN")


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    pd.set_option("display.max_rows", 500)
    tqdm.pandas()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keys",
        type=Path,
        help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'",
        required=True,
    )
    parser.add_argument(
        "--values",
        type=Path,
        help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'",
        required=True,
    )
    parser.add_argument(
        "--jaccard-similarity-threshold",
        type=float,
        help="Threshold for Jaccard Similarity score to be significant",
        default=0.5,
    )
    parser.add_argument(
        "--count-threshold",
        type=int,
        help="Threshold for count (number of co-occurences) score to be significant",
        default=5,
    )
    parser.add_argument(
        "--logging-level",
        type=str,
        help="Logging level, default is INFO",
        default="INFO",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Refresh and overwrite the existing cache",
    )
    parser.add_argument("--outpath", type=Path, help="Output path for matches")
    args = parser.parse_args()

    run_match(
        args.keys,
        args.values,
        args.outpath,
        args.logging_level,
        args.jaccard_similarity_threshold,
        args.count_threshold,
        args.refresh_cache,
    )

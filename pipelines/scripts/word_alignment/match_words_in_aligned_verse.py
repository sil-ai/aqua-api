import pandas as pd
import json
from tqdm import tqdm
from argparse import ArgumentParser
import os
from typing import Iterable, Tuple, List
import re
from collections import Counter
import logging

def write_dictionary_to_file(dictionary: dict, filename: str, to_strings: bool=False) -> None:
    """
    Takes a dictionary and writes it to a json file
    """
    if to_strings:
        dictionary = tuple_keys_to_string(dictionary)
    with open(filename, 'w', encoding='utf8') as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=4)
    logging.info(f"Written file {filename}")

def text_to_words(text: str) -> List[str]:
    """
    Inputs:
        text:   Normally a sentence, or Bible verse
    Outputs:
        A list of words, where the sentence has had its punctuation removed, and words splits into a list of words
    """
    word_list = re.sub("[^\w\s]", "", text.lower()).split()
    return word_list

def get_bible_data(bible: str) -> pd.DataFrame:
    """
    Takes the Bible version as an input, and returns a dataframe of the text.
    Inputs:
        bible_version:      A string, corresponding to the Bible version in https://github.com/BibleNLP/ebible-corpus/tree/main/corpus 
                            or https://raw.githubusercontent.com/BibleNLP/biblical-humanities-corpus/main/corpus/scripture/
        corpus_urls:        A dict of URLs showing where the corpus texts are found for each repo
        vref_urls:        A dict of URLs showing where the vref files are found for each repo
        repo:   A string, either ebible or pabnlp
        credentials_file  A file storing your github username and Personal Access Token, to access private github repositories (e.g. pabnlp)
    Outputs:
        df:         A dataframe with the Bible text in the version specified
    """
    with open(bible, 'r') as f:
        bible_data = f.readlines()
    words = [text_to_words(line) for line in bible_data]
    df = pd.DataFrame({'text': bible_data, 'words': words})
    df = df[df['text'].apply(lambda x: len(x) > 2)]
    df = df[df['text'] != "b'\n'"]
    return df

def get_indices_with_item(item: str, list_series: pd.Series) -> List[pd.Index]:
    """
    Returns indices from a series of lists, filtered by rows whose list contains a particular item
    Inputs:
        item:   A single item from list_series
        list_series:     A series containing the lists to filter by list_item
    Outputs:
        A list of indices for the list_series series, corresponding to rows that contain item
    """
    index_list = list(list_series[list_series.apply(lambda x: item in x if isinstance(x, Iterable) else False)].index)
    return index_list

def get_jaccard_similarity(list1: list, list2: list) -> float:
    """
    Gets the jacard similarity between two lists
    """
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(set(list1)) + len(set(list2))) - intersection
    return float(intersection) / union if union != 0 else 0

def get_correlations_between_sets(
                                        indexes_set_1: set,
                                        indexes_set_2: set,
                                        ) -> Tuple[float, int]:
    """
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
                            jaccard_similarity_threshold: float = 0.5,
                            count_threshold: int = 5,
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
        jaccard_similarity_threshold: (Default 0.5) The threshold for Jaccard Similarity for a match to be logged as significant
                                and entered into the matches dictionary
        count_threshold: (Default 5)    The threshold for count (number of occurences of the two items in the same verse) for a
                             match to be logged as significant and entered into the matches dictionary
    Outputs:
        matches:        A dictionary containing those matches which pass the thresholds for significance
        js_cache:         A dictionary cache of the Jaccard similarities between pairs of items already calculated
    """
    cache_counter = Counter()
    for keys_item in keys_list:
        for values_item in list(set(values_list)):
            if (keys_item in matches) and (values_item in matches[keys_item]):
                cache_counter.update(['Already in matches'])
                continue
            elif (keys_item, values_item) in js_cache:
                jaccard_similarity = js_cache[(keys_item, values_item)]['jaccard_similarity']
                count = js_cache[(keys_item, values_item)]['count']
                cache_counter.update(['Cached'])
            else:
                jaccard_similarity, count = get_correlations_between_sets(
                                                                            set(keys_index.get(keys_item, [])),
                                                                            set(values_index.get(values_item, [])),
                                                                            )
                js_cache[(keys_item, values_item)] = {'jaccard_similarity': jaccard_similarity, 'count': count} 
                cache_counter.update(['Calculated'])
            if jaccard_similarity > jaccard_similarity_threshold and count > count_threshold:
                if keys_item not in matches:
                    matches[keys_item] = []
                if values_item not in [item.get('value', '') for item in matches[keys_item]]: #matches[keys_item][:]['value']:
                    matches[keys_item].append({'value': values_item, 'jaccard_similarity': jaccard_similarity, "count": count})
    logging.debug(cache_counter)
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
    return {(key.split("-")[0], key.split("-")[1]): value for key, value in dictionary.items()}

def initialize_cache(cache_file: dict, to_tuples:bool=False, reverse:bool=False, refresh:bool=False) -> dict:
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
    if os.path.exists(cache_file) and not refresh:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
            if to_tuples:
                cache = string_keys_to_tuple(cache)
            if reverse:
                cache = {(key[1], key[0]): value for key, value in cache.items()}
    else:
        cache = {}
    return cache

def get_single_df(
                    df_path:str, 
                    df_name:str, 
                    list_name:str = 'keys'
                    ) -> pd.DataFrame:
    """
    Reads a dataframe corresponding to either the keys or values being investigated
    Inputs:
        df_path:    Path to the df parquet file, which may or may not currently exist. The dataframe is saved to this path.
        df_name:    Name of the dataframe, normally the Bible version name, or 'greek', 'hebrew', 'OT_domains' or 'NT_domains'
        repo:       A string, either 'ebible' or 'pabnlp'
        credentials_file:  A file storing your github username and Personal Access Token, to access private github repositories (e.g. pabnlp)
        list_name:  The name that should be used for the df column with the relevant verse lists. Normally 'keys' or 'values'.
    Outputs:
        df:         The dataframe containing Bible verses by vref index, and either a 'keys' or 'values' column with the relevant list data.
    """
    df = get_bible_data(df_name)
    df.to_parquet(df_path)
    df = df.rename(columns={'words': list_name})
    return df

def get_combined_df(keys_list_name: str, values_list_name: str) -> pd.DataFrame: 
    """
    Takes the names of the keys_list and values_list and creates ref_df - the dataframe that will be used in the rest of the script.
    Inputs: 
        keys_list_name:     Name of the keys_list. Either a Bible text name or "OT_domains", "NT_domains", "greek" or "hebrew".
        values_list_name:   Name of the values_list. Either a Bible text name or "OT_domains", "NT_domains" or "greek" or "hebrew".
        repo:   A string, either ebible or pabnlp
        credentials_file  A file storing your github username and Personal Access Token, to access private github repositories (e.g. pabnlp)
    Outputs:
        ref_df:     A dataframe that combines the keys and values data into a single dataframe by Bible verse
    """
    keys_list_name = args.keys_name.split('/')[-1]
    values_list_name = args.values_name.split('/')[-1]

    p = str(f"{args.outpath}/{keys_list_name.split('.')[0]}-{values_list_name.split('.')[0]}")
   
    keys_ref_df_path = f"{p}/{keys_list_name.split('.')[0]}_ref_df.parquet"
    values_ref_df_path = f"{p}/{values_list_name.split('.')[0]}_ref_df.parquet"

    keys_ref_df = get_single_df(keys_ref_df_path, args.keys_name, list_name = 'keys')
    values_ref_df = get_single_df(values_ref_df_path, args.values_name, list_name = 'values') 

    values_ref_series = values_ref_df['values']
    ref_df = pd.concat([keys_ref_df, values_ref_series], axis=1)
    ref_df = ref_df.dropna(subset=['keys', 'values'])
    ref_df.to_csv(f"{p}/{keys_list_name.split('.')[0]}-{values_list_name.split('.')[0]}_ref_df.csv")
    logging.info(ref_df.head())
    return ref_df

def main(args):
    keys_list_name = args.keys_name.split('/')[-1]
    values_list_name = args.values_name.split('/')[-1]

    p = str(f"{args.outpath}/{keys_list_name.split('.')[0]}-{values_list_name.split('.')[0]}")
    if not os.path.exists(p):
        os.makedirs(p)
    logging.basicConfig(format='%(asctime)s - %(funcName)20s() - %(message)s', level=args.logging_level.upper(), filename=f'{p}/match_words_in_aligned_verse.log', filemode='a')
    logging.info("START RUN")

    os.makedirs(p + "/cache", exist_ok=True)
    js_cache_file = f"{p}/cache/{keys_list_name}-{values_list_name}-freq-cache.json"
    reverse_freq_cache_file = f"{p}/cache/{values_list_name}-{keys_list_name}-freq-cache.json"
    keys_index_cache_file = f"{p}/cache/{keys_list_name}-index-cache.json"
    values_index_cache_file = f"{p}/cache/{values_list_name}-index-cache.json"

    matches_file = f"{p}/{keys_list_name.split('.')[0]}-{values_list_name.split('.')[0]}-dictionary.json"
    ref_df = get_combined_df(keys_list_name, values_list_name,)
    logging.info(f"Total verses: {len(ref_df)}")
    
    js_cache = initialize_cache(js_cache_file, to_tuples=True, refresh = args.refresh_cache)
    js_cache_reverse = initialize_cache(reverse_freq_cache_file, reverse=True, to_tuples=True)
    js_cache = {**js_cache, **js_cache_reverse}

    if args.refresh_cache or not os.path.exists(keys_index_cache_file):
        keys_index = {}
        print("Getting sentences that contain each word in keys")
        for word in tqdm(list(ref_df['keys'].explode().unique())):
            keys_index[word] = get_indices_with_item(word, ref_df['keys'])
        write_dictionary_to_file(keys_index, keys_index_cache_file)
    else:
        keys_index = initialize_cache(keys_index_cache_file, refresh = False)

    if args.refresh_cache or not os.path.exists(values_index_cache_file):
        values_index = {}
        print("Getting sentences that contain each word in values")
        for word in tqdm(list(ref_df['values'].explode().unique())):
            values_index[word] = get_indices_with_item(word, ref_df['values'])
        write_dictionary_to_file(values_index, values_index_cache_file)
    else:    
        values_index = initialize_cache(values_index_cache_file, refresh = False)

    ref_df = ref_df.dropna(subset=['keys', 'values'])  # Reduce ref_df to only verses present in both texts
    ref_df_indexes = list(ref_df.index)
    print("Getting keys_index")
    keys_index = {key: [item for item in values if item in ref_df_indexes] for key, values in tqdm(keys_index.items())}  # Reduce the keys_index dict to only those verses present in the reduced ref_df
    print("Getting values_index")
    values_index = {key: [item for item in values if item in ref_df_indexes] for key, values in tqdm(values_index.items())}  # Reduce the values_index dict to only those verses present in the reduced ref_df
   
    matches={}
 
    print("Getting matches...")
    for index, row in tqdm(ref_df.iterrows()):
        keys: List[str] = list(set(row['keys']))
        values: List[str] = list(set(row['values']))
        matches, js_cache = update_matches_for_lists(
                                                    keys, 
                                                    values, 
                                                    matches=matches,
                                                    js_cache=js_cache, 
                                                    keys_index=keys_index,
                                                    values_index=values_index,
                                                    jaccard_similarity_threshold=args.jaccard_similarity_threshold,
                                                    count_threshold=args.count_threshold,
                                                    )
    logging.info(f"Matches: {matches}")
    
    write_dictionary_to_file(js_cache, js_cache_file, to_strings=True)   
    write_dictionary_to_file(matches, matches_file)
    logging.info(f"Matches: {matches}")
    logging.info("END RUN")

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None  # default='warn'
    pd.set_option('display.max_rows', 500)
    tqdm.pandas()
    parser = ArgumentParser()
    parser.add_argument('--keys-name', help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'", required=True)
    parser.add_argument('--values-name', help="Can be a Bible version string, or 'OT_domains', 'NT_domains', 'hebrew' or 'greek'", required=True)
    parser.add_argument('--jaccard-similarity-threshold', type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.5)
    parser.add_argument('--count-threshold', type=int, help="Threshold for count (number of co-occurences) score to be significant", default=5)
    parser.add_argument('--logging-level', type=str, help="Logging level, default is INFO", default='INFO')
    parser.add_argument('--refresh-cache', action="store_true", help="Refresh and overwrite the existing cache")
    parser.add_argument('--outpath', type=str, help="Output path for matches")

    args = parser.parse_args()

    main(args)
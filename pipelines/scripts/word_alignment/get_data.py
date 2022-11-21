import json
import logging
import re
from pathlib import Path
from typing import Tuple, Iterable, Dict, Optional

import numpy as np
import pandas as pd
from machine.corpora import TextFileTextCorpus
from machine.tokenization import LatinWordTokenizer
from tqdm import tqdm


def faster_df_apply(df, func):
    cols = list(df.columns)
    data, index = [], []
    for row in tqdm(df.itertuples(index=True), total=df.shape[0]):
        row_dict = {f:v for f,v in zip(cols, row[1:])}
        data.append(func(row_dict))
        index.append(row[0])
    return pd.Series(data, index=index)
    

def write_dictionary_to_file(
    dictionary: dict, filepath: Path, to_strings: bool = False
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
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf8") as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=4)
    logging.info(f"Written file {filepath}")


def normalize_word(word:str)-> str:
    """
    Strips the punctuation from the word. Note that in some languages this punctuation includes important meaning from the word!
    But this is used to "normalize" words, and group together words that are similar (and often have similar meanings) but not identical.
    """
    word_norm = re.sub("[^\w\s]", "", str(word).lower()) if word else ''    #  Gives 18,159 unique Hebrew ords in the OT, rather than 87,000
    if len(word_norm) == 0:
        return word
    return word_norm


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


def condense_files(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes an input dataframe and writes condensed versions of the source and target
    columns to file, which only include those lines that are not blank in both input files.
    Also condenses <range> lines into the previous line in both source and target, 
    and removes the vref for that line.

    Inputs:
    df                 A dataframe with vref, source and target columns

    Outputs:
    df                  The condensed dataframe

    """
    df['to_drop'] = [False] * df.shape[0]
    df = df[df['src'] != '\n']
    df = df[df['trg'] != '\n']
    if df.shape[0] == 0:
        return df
    # merge up lines that contain \n or <range> in either src or trg
    src_to_append = ''
    trg_to_append = ''
    for index, row in df[:1:-1].iterrows():
        if row['src'].replace('\n', '').replace('<range>', '') == '':
            trg_to_append = row['trg'].replace('\n', ' ').replace('<range>', '') + trg_to_append
            df.loc[index-1, 'trg'] = df.loc[index-1, 'trg'].replace('\n', ' ') + trg_to_append + '\n'
            df.loc[index, 'to_drop'] = True
        if row['trg'].replace('\n', '').replace('<range>', '') == '':
            src_to_append = row['src'].replace('\n', ' ').replace('<range>', '') + src_to_append
            df.loc[index-1, 'src'] = df.loc[index-1, 'src'].replace('\n', ' ') + src_to_append + '\n'
            df.loc[index, 'to_drop'] = True
        if len(row['src'].replace('\n', '').replace('<range>', '')) > 0 and len(row['trg'].replace('\n', '').replace('<range>', '')) > 0:
            src_to_append = ''
            trg_to_append = ''
    if df.iloc[0].loc['src'].replace('\n', '').replace('<range>', '') == '' or df.iloc[0].loc['trg'].replace('\n', '').replace('<range>', '') == '':
        df.iloc[0].loc['to_drop'] = True
    df = df.drop(df[df['to_drop'] == True].index)

    df = remove_blanks_and_ranges(df)

    df["src"] = df["src"].str.lower()
    df["trg"] = df["trg"].str.lower()
    
    return df


def remove_blanks_and_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a df of source ('src') and optionally target ('trg') texts and removes any lines that are just new line chars
    or '<range>'.
    """
    df = df[df.src != "\n"]
    if 'trg' in df.columns:
        df = df[df.trg != "\n"]
    df = df[df.src != "<range>\n"]
    if 'trg' in df.columns:
        df = df[df.trg != "<range>\n"]

    return df

    
def write_condensed_files(df: pd.DataFrame, outpath: Path) -> Tuple[Path, Path]:
    """
    Writes text files from the text in the 'src' and (optionally) 'trg' columns of a dataframe.
    Inputs:
    df                A dataframe with the data in 'src' and (optionally) 'trg' columns
    outpath           The path where the (one or) two condensed files will be written
    Outputs:
                    The paths to the source and target output files. The second output is None if there is no 'trg'
                    column in the input df.
    """
    # write to condensed txt files
    if not outpath.exists():
        outpath.mkdir(exist_ok=True)
    source_path = outpath / f"src.txt"
    target_path = None

    with open(source_path, "w") as f:
        for line in df["src"]:
            f.write(line)
    if 'trg' in df.columns:
        target_path =  outpath / f"trg.txt"
        with open(target_path, "w") as f:
            for line in df["trg"]:
                f.write(line)
    return (source_path, target_path)


def create_corpus(condensed_source: Path, condensed_target: Optional[Path]=None) -> TextFileTextCorpus:
    """
    Takes two line-aligned condensed input files and produces a tokenized corpus. Note that this must be run on the
    output of write_condensed_files(), which removes the lines that are blank in either file.
    Inputs:
    condensed_source            A Path to the source file
    condensed_target            A Path to the target file

    Outputs:
    parallel_corpus     A tokenized TextFileTextCorpus
    """
    source_corpus = TextFileTextCorpus(condensed_source)
    target_corpus = TextFileTextCorpus(condensed_target) if condensed_target else TextFileTextCorpus(condensed_source) # If there is no target, just get the corpus using the source twice
    parallel_corpus = source_corpus.align_rows(target_corpus).tokenize(
        LatinWordTokenizer()
    )
    return parallel_corpus


class Word():
    def __init__(self, word: str):
        self.word = word
        self.matched = []
        self.index_list = []
        self.index_ohe = np.array([])
        self.norm_ohe = np.array([])
        self.encoding = np.array([])
        self.norm_encoding = np.array([])
        self.distances = {}
        self.normalize()

    def normalize(self):
        self.normalized = normalize_word(self.word)
    
    def get_indices(self, word_series):
        # self.index_list = list(list_series[list_series.apply(lambda x: self.normalized in x if isinstance(x, Iterable) else False)].index)
        self.index_list = list(word_series[word_series == self.normalized].index)

    def remove_index_list(self):
        self.index_list = None  # To save memory, once they're no longer needed.

    def get_matches(self, word):
        jac_sim, count = get_correlations_between_sets(set(self.index_list), set(word.index_list))
        return (jac_sim, count)
    
    def get_encoding(self, weights: np.ndarray):
        # self.encoding = model.encoder(torch.tensor(self.index_ohe).float()).cpu().detach().numpy()
        self.get_ohe()
        self.encoding = np.matmul(weights, self.index_ohe)
        self.norm_encoding = self.encoding / np.linalg.norm(self.encoding)
        self.index_ohe = np.array([])
    
    def get_ohe(self, max_num=41899):
        a = np.zeros(max_num)
        np.put(a, np.array(self.index_list, dtype='int64'), 1)    
        self.index_ohe = a
                       
    def get_norm_ohe(self, max_num=41899):
        a = np.zeros(max_num)
        np.put(a, np.array(self.index_list), 1)  
        norm_a = a / np.linalg.norm(a)
        self.norm_ohe = norm_a
        
    def get_distance(self, word):
        if word.encoding is None or self.encoding is None:
            return
        distance = np.linalg.norm(self.encoding - word.encoding)
        return distance
    
    def get_norm_distance(self, word, language):
        # if language not in self.distances:
        #     self.distances[language] = {}
        # if word not in self.distances[language]:
        #     self.distances[language][word] = np.linalg.norm(self.norm_encoding - word.norm_encoding)
        # return self.distances[language][word]
        return np.linalg.norm(self.norm_encoding - word.norm_encoding)

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


def get_words_from_cache(index_cache_file: Path) -> Dict[str, Word]:
    """
    Creates a word_lang_dict from a cache file of the indices of words in a text.
    Inputs:
    index_cache_file        The cache file of indices of all words in a text.
    Outputs:
    word_dict_lang      A dictionary of {word (str): Word} items
    """
    index_lists = initialize_cache(index_cache_file, refresh=False)
    word_dict_lang = {word: Word(word) for word in index_lists}
    for word in word_dict_lang.values():
        word.index_list = index_lists[word.word]
        # word.get_ohe()
    return word_dict_lang


def create_words_from_df(ref_df: pd.DataFrame) -> Dict[str, Word]:
    """
    Takes a DataFrame and constructs a dictionary of Words from all the words in a column of that dataframe.
    Inputs:
    df          A dataframe containing the words
    Outputs:
    word_dict_lang      A dictionary of {word (str): Word} items
    """
    all_source_words = list(ref_df['src_words'].explode().unique())
    word_dict_lang = {word: Word(word) for word in all_source_words if type(word) == str}

    word_series = ref_df['src_words'].explode().apply(lambda x: normalize_word(x))
    for word in tqdm(word_dict_lang.values()):
        # word.get_indices(ref_df['src'])
        word.get_indices(word_series)
        # word.get_ohe()
    return word_dict_lang


def save_word_dict_lang_to_cache(word_dict_lang: Dict[str,Word], index_cache_file: Path) -> None:
    """
    Takes a word_dict_lang and saves all the index lists to a cache file.
    Inputs:
    word_dict_lang      A dictionary of {word (str): Word} items
    index_cache_file        A path to store the cache file of indices of all words in a text.
    """
    all_source_index = {word.word: word.index_list for word in word_dict_lang.values()}
    write_dictionary_to_file(all_source_index, index_cache_file)


def get_words_from_txt_file(df: pd.DataFrame, outpath: Path) -> pd.DataFrame:
    """
    Takes a dataframe and processes the words contained by creating a corpus, returning a dataframe.
    Inputs:
    df          The dataframe with the data
    output      Where to save the txt files needed to create the corpus
    Outputs:
    df          A dataframe with the processed words.
    """

    source_file, target_file = write_condensed_files(df, outpath)
    condensed_parallel_corpus = create_corpus(source_file, target_file)
    corpus_df = condensed_parallel_corpus.lowercase().to_pandas()
    df.loc[:, 'src'] = list(corpus_df['source'])
    df.loc[:, 'trg'] = list(corpus_df['target'])
    df.loc[:, 'src_words'] = df['src'].apply(lambda x: x.split())
    df.loc[:, 'trg_words'] = df['trg'].apply(lambda x: x.split())
    return df

def create_words(
                source: Path, 
                cache_path: Path, 
                outpath: Path, 
                refresh_cache: bool=False, 
                is_bible: bool=True
                ) -> Dict[str, Word]:
    """
    Creates a dictionary. Keys are strings of the language. Values are dictionaries where keys are the string of the word and
    values are Word objects.
    Inputs:
    source              Path to txt file
    cache_path   Path to the cache directory
                        used for that language. Normally args.outpath / cache.
    outpath             The base outpath directory
    refresh_cache       Bool. Where to force not using the cache files.

    Outputs:
    word_dict           Dictionary of:  keys: word string
                                        values: Word object
    """
    word_dict_lang = {}
    ref_df = get_ref_df(source, is_bible=is_bible)
    ref_df = get_words_from_txt_file(ref_df, outpath)
    index_cache_file = cache_path / f'{source.stem}-index-cache.json'
    
    if index_cache_file.exists() and not refresh_cache:
        print(f"Getting sentences that contain each word in {source.stem} from {index_cache_file}")    
        word_dict_lang = get_words_from_cache(index_cache_file)
    else:
        if not index_cache_file.parent.exists():
            index_cache_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"Getting sentences that contain each word in {source.stem}")    
        word_dict_lang = create_words_from_df(ref_df)
        save_word_dict_lang_to_cache(word_dict_lang, index_cache_file)

    return word_dict_lang


def get_ref_df(source: Path, target: Optional[Path] = None, is_bible: bool=True) -> list:
    """
    Takes two aligned text files and returns a dataframe of vrefs, source and target columns.

    Inputs:
    source            A Path to the source file
    target            A Path to the target file. Optional, if you just want a ref_df for one language.
    is_bible          Boolean for whether the text is Bible. If is_bible is true, the length of the text files must be 41,899 lines.

    Outputs:
    vref_list     A list of vrefs that are non-blank in both input files
    """
    with open(source) as f:
        src_data = f.readlines()
        if is_bible:
            assert len(src_data) == 41899, "is_bible requires your source input to be 41,899 lines in length"
    if target:
        with open(target) as f:
            trg_data = f.readlines()
            if is_bible:
                assert len(trg_data) == 41899, "is_bible requires your target input to be 41,899 lines in length"
            assert len(src_data) == len(trg_data), "Source and target txt files must be the same length"

    if is_bible:
        with open("vref.txt", "r") as f:
            vrefs = f.readlines()
        vrefs = [line.strip() for line in vrefs]
        assert len(vrefs) == 41899,  "the vref.txt file must be 41899 lines in length"
    else:
        vrefs = [str(i) for i in range(len(src_data))]

    df = pd.DataFrame({"vref": vrefs, "src": src_data})
    if target:
        df['trg'] = trg_data

    return df

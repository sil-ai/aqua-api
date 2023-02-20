from pathlib import Path
from typing import List
import re
from typing import Dict
import pickle

class Word():
    def __init__(self, word: str):
        self.word = word
        self.matched = []
        self.index_list = []
        self.normalized = normalize_word(self.word)
    
    def get_indices(self, word_series):
        self.index_list = list(set(word_series[word_series == self.normalized].index))

    def get_ohe(self, max_num=41899):
        import numpy as np
        a = np.zeros(max_num)
        np.put(a, np.array(self.index_list, dtype='int64'), 1)    
        self.index_ohe = a

    def get_encoding(self, weights):
        import numpy as np
        self.get_ohe()
        self.encoding = np.matmul(weights, self.index_ohe)
        self.norm_encoding = self.encoding / np.linalg.norm(self.encoding)
        self.index_ohe = np.array([])
    
    def get_norm_distance(self, word):
        import numpy as np
        return np.linalg.norm(self.norm_encoding - word.norm_encoding)


def normalize_word(word:str)-> str:
    """
    Strips the punctuation from the word. Note that in some languages this punctuation includes important meaning from the word!
    But this is used to "normalize" words, and group together words that are similar (and often have similar meanings) but not identical.
    """
    word_norm = re.sub("[^\w\s]", "", str(word).lower()) if word else ''    #  Gives 18,159 unique Hebrew ords in the OT, rather than 87,000
    if len(word_norm) == 0:
        return word
    return word_norm

def create_tokens(src_data: List[str], vref_filepath: Path):

    """
    Takes a list of strings where each item corresponds to a verse, and the filepath for vref.text.
    Returns a (pickled) tokenixed dataframe of words for each verse, tokenized using the LatinWordTokenizer.
    """
    from machine.corpora import TextFileTextCorpus
    from machine.tokenization import LatinWordTokenizer
    import pandas as pd
    
    with open(vref_filepath, "r") as f:
        vrefs = f.readlines()
        vrefs = [line.strip() for line in vrefs]
    ref_df = pd.DataFrame({"vref": vrefs, "src": src_data}).astype("object")
    ref_df['vref'] = ref_df['vref'].apply(lambda x: x.replace('\n', ''))
    condensed_df = ref_df[ref_df['src'] != '']
    with open(Path('condensed_src.txt'), 'w') as f:
        for line in condensed_df['src']:
            f.writelines(line)
            if len(str(line)) > 0 and str(line)[-1:] != '\n':
                f.write('\n')
    condensed_corpus = TextFileTextCorpus('condensed_src.txt')
    tokenized_corpus = condensed_corpus.tokenize(LatinWordTokenizer())
    tokenized_list = list(tokenized_corpus.align_rows(tokenized_corpus).lowercase().to_pandas()['source'])
    condensed_df.loc[:, 'src_tokenized'] = tokenized_list
    tokenized_df = condensed_df[['vref', 'src_tokenized']]
    tokenized_df_pkl = pickle.dumps(tokenized_df)

    return tokenized_df_pkl


def condense_df(df_pkl: bytes) -> bytes:
    """
    Takes a (pickled) input dataframe with src_tokenized and trg_tokenized coumns, and outputs
    a dataframe which only include those lines that are not blank in both input files.
    Also condenses < range > lines into the previous line in both source and target, 
    and removes the vref for that line, adding the removed indices into the 'indices'
    column, so you know which indices have been combined.

    Inputs:
    df_pkl                 A pickled dataframe with vref, source and target columns

    Outputs:
    df_pkl                  The (pickled) condensed dataframe

    """
    df = pickle.loads(df_pkl)
    df = df.rename(columns={'src_tokenized': 'src', 'trg_tokenized': 'trg'})
    df['indices'] = df.index
    df.loc[:, 'indices'] = df['indices'].apply(lambda x: str(x))
    df.loc[:, 'src'] = df['src'].apply(lambda x: str(x))
    df.loc[:, 'trg'] = df['trg'].apply(lambda x: str(x))
    df = df[(df['src'] != '\n') & (df['src'] != '')]
    df = df[(df['trg'] != '\n') & (df['trg'] != '')]
    df['next_src'] = df['src'].shift(-1)
    df['next_trg'] = df['trg'].shift(-1)
    df['range_next'] = (df['next_src'] == '< range >') | (df['next_trg'] == '< range >')
    df = df.reset_index()
    for index, row in df[:1:-1].iterrows():
        if row['range_next']:
            df.loc[index, 'indices'] += ' ' + df.loc[index+1, 'indices']
            if len(df.loc[index+1, 'src'].replace('< range >', '')) > 0:
                df.loc[index, 'src'] += ' ' + df.loc[index+1, 'src'].replace('< range >', '')
            if len(df.loc[index+1, 'trg'].replace('< range >', '')) > 0:
                df.loc[index, 'trg'] += ' ' + df.loc[index+1, 'trg'].replace('< range >', '')
    df = df[(df['src'] != '< range >') & (df['trg'] != '< range >')]
    df = df.drop(['next_src', 'next_trg', 'range_next'], axis=1)
    df = df.set_index('index')
    df_pkl = pickle.dumps(df)
    return df_pkl


def get_words_from_cache(index_cache: dict) -> Dict[str, Word]:
    """
    Creates a word_lang_dict from a cache file of the indices of words in a text.
    Inputs:
    index_cache        A dictionary of the indices of the verses where each word occurs.
    Outputs:
    word_dict      A dictionary of {word (str): Word} items
    """

    word_dict = {word: Word(word) for word in index_cache}
    for word in word_dict.values():
        word.index_list = index_cache[word.word]
    return word_dict

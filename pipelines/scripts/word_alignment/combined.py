import json
import argparse
import math
from pathlib import Path
from typing import Tuple, Optional, Dict

import pandas as pd
import numpy as np
from tqdm import tqdm
# from xgboost import XGBClassifier

import align
import align_best
import match
import get_data

tqdm.pandas()


# run fast_align
def run_fa(
            source: Path, 
            target: Path, 
            outpath: Path, 
            is_bible: bool=False,
            ) -> None:
    """
    Runs both alignment models: getting translation scores from all combinations of words in source and target
    and getting counts for how many times those words are aligned together. Various csv files are outputted to
    the outpath directory.

    Inputs:
    source              Path to the source file
    target              Path to the target file
    outpath             Path to the output directory
    is_bible            Boolean for whether the lines correspond to Bible verses. If True, the length of both
                        source and target files must be 41,899 lines.
    """
    # Get all alignment scores
    corpus, model = align.run_align(source, target, outpath, is_bible=is_bible)
    
    # Get count of best alignments
    align_best.run_best_align(source, target, outpath, is_bible=is_bible, parallel_corpus=corpus, symmetrized_model=model)


# run match words
def run_match_words(
    source: Path,
    target: Path,
    outpath: Path,
    word_dict_src: Optional[dict]=None,
    word_dict_trg: Optional[dict]=None,
    jaccard_similarity_threshold: float = 0.05,
    count_threshold: int = 0,
    refresh_cache: bool=False,
    is_bible: bool=True
    ) -> None:
    """
    Runs match.run_match with the supplied arguments to get jaccard similarity scores and counts for pairs of
    source and target words. These are then saved to dictionary.json in the outpath directory.
    Inputs:
    source      A path to the source text
    target      A path to the target text
    outpath     Path to the base output directory
    jaccard_similarity_threshold        Jaccard similiarty threshold above which word matches will be kept
    count_threshold                     Count threshold above which word matches will be kept
    refresh_cache           Force a cache refresh, rather than using cache from the last time the source and/or target were run
    """
    
    word_dict_src, word_dict_trg = match.run_match(
        source,
        target,
        outpath,
        word_dict_src=word_dict_src,
        word_dict_trg=word_dict_trg,
        jaccard_similarity_threshold=jaccard_similarity_threshold,
        count_threshold=count_threshold,
        refresh_cache=refresh_cache,
        is_bible=is_bible,
    )
    return(word_dict_src, word_dict_trg)


def get_scores_from_match_dict(
                                dictionary: dict, 
                                source_word: str, 
                                target_word: str,
                                normalized: bool=True,
                                ) -> Tuple[float, float]:
    """
    Takes a source word and a target word, looks them up in the match dictionary and returns the 
    jaccard similarity and count fields for their match in the dictionary.
    Inputs:
    dictionary          The match dictionary for look up
    source              A string word to look up
    target              A string word to look up
    normalized          Whether the source and target have already been normalized, as they need to be before dictionary look up
    
    Outputs:
    jac_sim             The jaccard similarity between the source and target in the dictionary
    match_count         The count between the source and target in the dictionary
    """
    if not normalized:
        source_word = get_data.normalize_word(source_word)
        target_word = get_data.normalize_word(target_word)

    list_for_source = dictionary.get(source_word, [])
    match_list = [match for match in list_for_source if match.get("value") == target_word]
    if len(match_list) == 0:
        return 0, 0
    jac_sim = match_list[0]["jaccard_similarity"]
    match_count = match_list[0]["count"]
    return jac_sim, match_count


def combine_word_scores(translation_path: Path, avg_alignment_path: Path, match_path: Path) -> pd.DataFrame:
    """
    Reads the outputs saved to file from match.run_match(), align.run_align() and best_align.run_best_align() and saves to a single df
    Inputs:
    align_path             Path to the translation_scores.csv alignment file
    best_path              Path to the avg_alignment_scores.csv best alignments file
    match_path             Path to the dictionary.json match dictionary file

    Output:
    df                  A dataframe containing pairs of source and target words, with metrics from the three algorithms
    """
    # open results
    print(f"Combining results from the three algorithms from {translation_path}, {avg_alignment_path} and {match_path}")
    

    all_results = pd.read_csv(translation_path)
    best_results = pd.read_csv(avg_alignment_path)
    all_results = all_results.merge(best_results, how='left', on=['source', 'target'])
    # print(all_results)
    all_results.loc[:, ['avg_aligned']] = all_results.apply(
        lambda row: row['alignment_count'] / row['co-occurrence_count'], axis = 1
        )
    all_results.loc[:, 'alignment_count'] = all_results.loc[:, 'alignment_count'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'verse_score'] = all_results.loc[:, 'verse_score'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'avg_aligned'] = all_results.loc[:, 'avg_aligned'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'alignment_count'] = all_results.loc[:, 'alignment_count'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'translation_score'] = all_results.loc[:, 'translation_score'].apply(
        lambda x: 0 if x < 0.00001 else x
        )

    with open(match_path) as f:
        match_results = json.load(f)

    # write to df and merge with fa results
    df = all_results
    df.loc[:, "jac_sim"] = get_data.faster_df_apply(df, 
        lambda x: get_scores_from_match_dict(
            match_results, x["source"], x["target"], normalized=False
        )[0],
        # axis=1,
    )
    df.loc[:, "match_counts"] = get_data.faster_df_apply(df, 
        lambda x: get_scores_from_match_dict(
            match_results, x["source"], x["target"], normalized=False
        )[1],
        # axis=1,
    )

    df.drop(columns=["Unnamed: 0_x", "Unnamed: 0_y"], inplace=True)
    return df


def run_all_alignments(
                        source: Path,
                        target: Path,
                        outpath: Path,
                        word_dict_src: Optional[dict]=None,
                        word_dict_trg: Optional[dict]=None,
                        is_bible: bool=True,
                        jaccard_similarity_threshold: float=0.05,
                        count_threshold: int=0,
                        refresh_cache: bool=False,
                        ):
    print(f"Running Fast Align to get alignment scores and translation scores for {source.stem} to {target.stem}")
    run_fa(
            source,
            target,
            outpath,
            is_bible=is_bible,
            )

    print(f"Running Match to get word match scores for {source.stem} to {target.stem}")

    word_dict_src, word_dict_trg = match.run_match(
            source,
            target,
            outpath,
            word_dict_src=word_dict_src,
            word_dict_trg=word_dict_trg,
            is_bible=is_bible,
            jaccard_similarity_threshold=jaccard_similarity_threshold,
            count_threshold=count_threshold,
            refresh_cache=refresh_cache,
                )
    return (word_dict_src, word_dict_trg)


def run_combine_results(outpath: Path) -> None:
    """
    Runs combined.combine_df to combine the three output files in the outpath directory. They are saved
    to align_and_match_word_scores.csv in the same outpath directory.
    Inputs:
    outpath         The directory where all three files are located.
    """
    translation_path = outpath / "translation_scores.csv"
    avg_alignment_path = outpath / "avg_alignment_scores.csv"
    match_path = outpath / "dictionary.json"
    print("Combining word-pair scores across the corpus")
    df = combine_word_scores(translation_path, avg_alignment_path, match_path)

    # save results
    df.to_csv(outpath / "align_and_match_word_scores.csv")


def combine_by_verse_scores(
                            source: Path, 
                            target: Path,
                            outpath: Path, 
                            word_dict_src: Optional[Dict[str, get_data.Word]] = None, 
                            word_dict_trg: Optional[Dict[str, get_data.Word]] = None, 
                            weights_path: Optional[Path]=None, 
                            is_bible: bool=True
                            ) -> None:
    """
    Takes "best_in_context" scores and "combined" scores for source-target word pairs and merges them.
    Produces "verse_scores" which are a mean of the various metrics by verse.
    Takes "all_in_context" scores and "combined" scores for source-target word pairs and merges them,
    adding a "simple_total" score which is the mean of the first four metrics, and a "total_score" which
    is a mean of all five, where the fifth (encoding distance) is converted to approximately the same [0,1] scale.
    Saves to "all_in_context_with_scores".
    NB: Code for creating a total score with an XGBoost model is commented out for possible future use.
    Inputs:
    source          A path to the source text
    target          A path to the target text
    outpath         Path to the base output directory
    weights_path    Path to the txt file with the encoder weights
    is_bible        Whether the text is Bible
    """

    print("Combining scores for each word-pair in each verse")
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.remove_blanks_and_ranges(ref_df)
    ref_df = get_data.get_words_from_txt_file(ref_df, outpath)
    ref_df = ref_df.explode('src_words').explode('trg_words')
    ref_df = ref_df.drop(['src', 'trg'], axis=1).rename(columns={'src_words': 'source', 'trg_words': 'target'})
    by_verse_scores = pd.read_csv(outpath / 'alignment_scores_by_verse.csv')
    by_verse_scores = by_verse_scores.drop('Unnamed: 0', axis=1)
    by_verse_scores = by_verse_scores.astype({col: 'float16' for col in by_verse_scores.columns if col not in ['vref', 'source', 'target']})
    word_scores = pd.read_csv(outpath / 'align_and_match_word_scores.csv')
    word_scores = word_scores.drop('Unnamed: 0', axis=1)
    word_scores = word_scores.astype({col: 'float16' for col in word_scores.columns if col not in ['vref', 'source', 'target']})
    by_verse_scores['vref'] = by_verse_scores['vref'].astype('object')  # Necessary for non-Bible, where vrefs are ints.
    by_verse_scores = pd.merge(ref_df, by_verse_scores, on=['vref', 'source', 'target'], how='left')
    by_verse_scores = pd.merge(by_verse_scores.drop(columns=[
                'alignment_count', 
                ]), 
                word_scores.drop(columns=[
                    'verse_score', 
                    'alignment_count', 
                    'alignment_score',
                    ])
                    , on=['source', 'target'], how='left')
    by_verse_scores['alignment_score'].fillna(0, inplace=True)
    if word_dict_src == None:
        word_dict_src = get_data.create_words(source, outpath.parent / 'cache', outpath, is_bible=is_bible)
    if word_dict_trg == None:
        word_dict_trg = get_data.create_words(target, outpath.parent / 'cache', outpath, is_bible=is_bible)
    
    if weights_path is None:
        weights_path = Path('data/models/encoder_weights.txt')
    by_verse_scores = add_distances_to_df(word_dict_src, word_dict_trg, target.stem, outpath, weights_path=weights_path, df=by_verse_scores)
    word_dict_src = None
    word_dict_trg = None
    print("Calculating total score of all five metrics")
    by_verse_scores.loc[:, 'total_score'] = get_data.faster_df_apply(by_verse_scores,lambda row: (row['avg_aligned'] + row['translation_score'] + row['alignment_score'] + row['jac_sim'] + math.log1p(max(1 - row['encoding_dist'], -0.99))) / 5)
    # by_verse_scores = by_verse_scores.loc[:, ['vref', 'source', 'target', 'total_score']]
    by_verse_scores.to_csv(outpath / 'by_verse_scores.csv')
    word_scores = remove_leading_and_trailing_blanks(by_verse_scores, 'total_score')
    del by_verse_scores
    word_scores = word_scores.fillna(0)
    word_scores = word_scores.loc[word_scores.groupby(['vref', 'source'], sort=False)['total_score'].idxmax(), :].reset_index(drop=True)
    word_scores.to_csv(outpath / 'word_scores.csv')
    verse_scores = word_scores.groupby('vref', sort=False).mean()
    del word_scores
    verse_scores = verse_scores.fillna(0)
    verse_scores.to_csv(outpath / 'verse_scores.csv')


def get_encodings(word_dict_lang:Dict[str,get_data.Word], weights: np.ndarray) -> None:
    """
    Takes a dictionary of {word: Word} items and calculates the embedding of each word in the model. This
    is saved as encoding and norm_encoding attributes of the Word.
    Inputs:
    word_dict_lang      A dictionary of word (string) : Word (object)
    weights             np.ndarray with encoder weights
    """
    for word in tqdm(word_dict_lang.values()):
        # print(word.index_ohe.shape)
        word.get_encoding(weights)


def add_distances_to_df(
                        word_dict_src: dict, 
                        word_dict_trg: dict, 
                        target_lang: str,
                        outpath: Path, 
                        weights_path: Path, 
                        df: Optional[pd.DataFrame]=None, 
                        cache_path: Optional[Path]=None, 
                        # refresh_cache: bool=False
                        ) -> None:
    """
    Takes source and target words, calculates the embeddings for each of the words with respect to the model. Calculates
    distances between embeddings for each combination of words that co-occur in lines within their aligned corpuses. Saves
    these distances to the 'encoding_dist' column of a df.
    Inputs:
    source          Dictionary of Words from source text
    target          Dictionary of Words from target text
    target_lang     String of target language
    outpath         Path to output files. Normally args.outpath / {source.stem}_{target.stem}
    weights_path    Path to the txt file containing the encoder model weights
    df              Optionally pass in the df to write to, to save reading it from disk, since it is large.
    cache_path      Cache path, if it is different from default args.cache / cache
    refresh_cache   Boolean. Whether to force calculating the Word.index_lists from text data rather than from cache
    """
    cache_path = cache_path if cache_path else outpath.parent / 'cache'
    # language_paths = {language_path.stem: language_path for language_path in [source, target]}
    # index_cache_paths = {}
    # for word_dict_lang in [source_word_dict, target_word_dict]:
        # index_cache_paths[language] = cache_path
    # word_dict = create_words(language_paths, index_cache_paths, outpath, refresh_cache=refresh_cache)
    # for language in language_paths:
    if df is None:
        df = pd.read_csv(outpath / 'summary_scores.csv')
    word_dict = {'source': word_dict_src, 'target': word_dict_trg}
    weights = np.loadtxt(weights_path)
    for lang, word_dict_lang in word_dict.items():
        print(f"Getting {lang} encodings")
        assert isinstance(word_dict_lang, dict)
        get_encodings(word_dict_lang, weights=weights)
        unmatched_words = set(df[lang].unique()) - set(df[lang].unique()).intersection(set(word_dict_lang.keys()))
        assert len(unmatched_words) == 0, f"{unmatched_words} not in word_dict[{lang}]. Run again with --refresh-cache, or combined.py --refresh cache for just this source-target combination."
    print("Adding encoding distances to the data")
    
    df.loc[:, 'encoding_dist'] = get_data.faster_df_apply(df, lambda row: word_dict_src[row['source']].get_norm_distance(word_dict_trg[row['target']], target_lang))
    # df.to_csv(outpath / 'summary_scores.csv')
    return df


def remove_leading_and_trailing_blanks(df:pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Takes a dataframe and removes all rows before the first non-blank entry in a column, and after the last non-blank entry.
    """
    df = df[(df.loc[:, col].notna().cumsum() > 0) & (df.loc[::-1, col].notna().cumsum() > 0)]
    return df


def main(args):
    outpath = args.outpath / f"{args.source.stem}_{args.target.stem}"
    outpath.mkdir(parents=True, exist_ok=True)
    word_dict_src, word_dict_trg = None, None
    if not args.combine_only or not (outpath / 'dictionary.json').exists():
        word_dict_src, word_dict_trg = run_all_alignments(
                                            args.source,
                                            args.target,
                                            outpath,
                                            is_bible=args.is_bible,
                                            jaccard_similarity_threshold=args.jaccard_similarity_threshold,
                                            count_threshold=args.count_threshold,
                                            refresh_cache = args.refresh_cache,
                                            )
    # if word_dict_src:
    #     for word in word_dict_src.values():
    #         word.remove_index_list()        # To save memory
    # if word_dict_trg:
    #     for word in word_dict_trg.values():
    #         word.remove_index_list()
   
    run_combine_results(outpath)
    combine_by_verse_scores(
                            args.source, 
                            args.target, 
                            outpath, 
                            word_dict_src=word_dict_src, 
                            word_dict_trg=word_dict_trg, 
                            weights_path=args.weights, 
                            is_bible=args.is_bible,
                            )


if __name__ == "__main__":
    # #command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source bible")
    parser.add_argument("--target", type=Path, help="target bible")
    parser.add_argument("--jaccard-similarity-threshold", type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.05)
    parser.add_argument("--is-bible", action='store_true', help="is bible")
    parser.add_argument("--count-threshold", type=int, help="Threshold for count (number of co-occurences) score to be significant", default=0)
    parser.add_argument("--outpath", type=Path, help="where to store results")
    parser.add_argument("--weights", type=Path, default=Path('data/models/encoder_weights.txt'), help="Path to txt file containing weights for encoder")
    parser.add_argument("--combine-only", action='store_true', help="Only combine the results, since the alignment and matching files already exist")
    parser.add_argument("--refresh-cache", action='store_true', help="Refresh the cache of match scores")

    args, unknown = parser.parse_known_args()
    main(args)
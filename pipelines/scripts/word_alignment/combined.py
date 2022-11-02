# imports
from enum import auto
import os
import json
import argparse
import logging
import math

import pandas as pd
import align
import align_best
import match
import autoencoder
from tqdm import tqdm
import torch
from xgboost import XGBClassifier

tqdm.pandas()

from pathlib import Path
from typing import Tuple, Optional

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
    jaccard_similarity_threshold: float = 0.0,
    count_threshold: int = 0,
    refresh_cache: bool=False,
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
    
    match.run_match(
        source,
        target,
        outpath,
        jaccard_similarity_threshold=jaccard_similarity_threshold,
        count_threshold=count_threshold,
        refresh_cache=refresh_cache,
    )


def get_scores_from_match_dict(
                                dictionary: dict, 
                                source_word: str, 
                                target_word: str
    ) -> Tuple[float, float]:
    """
    Takes a source word and a target word, looks them up in the match dictionary and returns the 
    jaccard similarity and count fields for their match in the dictionary.
    Inputs:
    dictionary          The match dictionary for look up
    source              A string word to look up
    target              A string word to look up
    
    Outputs:
    jac_sim             The jaccard similarity between the source and target in the dictionary
    match_count         The count between the source and target in the dictionary
    """
    list_for_source = dictionary.get(source_word, [])
    match_list = [match for match in list_for_source if match.get("value") == target_word]
    if len(match_list) == 0:
        return 0, 0
    jac_sim = match_list[0]["jaccard_similarity"]
    match_count = match_list[0]["count"]
    return jac_sim, match_count


def combine_df(align_path: Path, best_path: Path, match_path: Path) -> pd.DataFrame:
    """
    Reads the outputs saved to file from match.run_match(), align.run_align() and best_align.run_best_align() and saves to a single df
    Inputs:
    align_path             Path to the all_sorted.csv alignment file
    best_path              Path to the best_sorted.csv best alignments file
    match_path             Path to the dictionary.json match dictionary file

    Output:
    df                  A dataframe containing pairs of source and target words, with metrics from the three algorithms
    """
    # open results
    print(f"Combining results from the three algorithms from {align_path}, {best_path} and {match_path}")
    

    all_results = pd.read_csv(align_path)
    best_results = pd.read_csv(best_path)
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
    all_results.loc[:, "normalized_source"] = all_results["source"].apply(
        match.normalize_word
    )
    all_results.loc[:, "normalized_target"] = all_results["target"].apply(
        match.normalize_word
    )

    match_results = json.load(open(match_path))

    # write to df and merge with fa results
    df = all_results
    df.loc[:, "jac_sim"] = df.progress_apply(
        lambda x: get_scores_from_match_dict(
            match_results, x["normalized_source"], x["normalized_target"]
        )[0],
        axis=1,
    )
    df.loc[:, "match_counts"] = df.progress_apply(
        lambda x: get_scores_from_match_dict(
            match_results, x["normalized_source"], x["normalized_target"]
        )[1],
        axis=1,
    )

    df.drop(columns=["Unnamed: 0_x", "Unnamed: 0_y"], inplace=True)
    return df


def run_all_alignments(
                        source: Path,
                        target: Path,
                        outpath: Path,
                        is_bible: bool=True,
                        jaccard_similarity_threshold: float=0.0,
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

    run_match_words(
            source,
            target,
            outpath,
            jaccard_similarity_threshold,
            count_threshold,
            refresh_cache=refresh_cache,
                )


def run_combine_results(outpath: Path) -> None:
    """
    Runs combined.combine_df to combine the three output files in the outpath directory. They are saved
    to combined.csv in the same outpath directory.
    Inputs:
    outpath         The directory where all three files are located.
    """
    align_path = outpath / "all_sorted.csv"
    best_path = outpath / "best_sorted.csv"
    match_path = outpath / "dictionary.json"
    df = combine_df(align_path, best_path, match_path)

    # save results
    df.to_csv(outpath / "combined.csv")


def add_scores_to_alignments(source: Path, target: Path, outpath: Path, model_path: Optional[Path]=None, is_bible: bool=True) -> None:
    if model_path is None:
        model_path = Path('data/models/autoencoder_50')
    df = pd.read_csv(outpath / 'best_in_context.csv')
    df['vref'] = df['vref'].astype('str')
    combined_df = pd.read_csv(outpath / 'combined.csv')
    all_vrefs = align.get_ref_df(source, target, is_bible, remove_blanks=False) 
    all_vrefs = pd.DataFrame(all_vrefs, columns = ['vref'], dtype='str')
    df = pd.merge(df.drop(columns=[
                'alignment_count', 
                'Unnamed: 0', 
                'alignment_score',
                ]), 
                combined_df.drop(columns=[
                    'Unnamed: 0', 
                    'verse_score', 
                    'alignment_count', 
                    'normalized_source', 
                    'normalized_target'
                    ])
                    , on=['source', 'target'], how='left')
    df = pd.merge(all_vrefs, df, on='vref', how = 'left')    
    df.loc[:, 'total_score'] = df.apply(lambda row: (row['avg_aligned'] + row['translation_score'] + row['alignment_score'] + row['jac_sim']) / 4, axis=1)
    df.to_csv(outpath / 'best_in_context_with_scores.csv')
    verse_scores = df.loc[:, ['vref', 'verse_score', 'avg_aligned', 'alignment_score', 'jac_sim', 'total_score']].groupby('vref', sort=False).mean()
    verse_scores = remove_leading_and_trailing_blanks(verse_scores, 'verse_score')
    verse_scores = verse_scores.fillna(0)
    verse_scores.to_csv(outpath / 'verse_scores.csv')

    df = pd.read_csv(outpath / 'all_in_context.csv')
    df = pd.merge(df.drop(columns=['Unnamed: 0', 'translation_score']), combined_df.drop(columns=['Unnamed: 0']), on=['source', 'target'], how='left')
    df['alignment_score'].fillna(0, inplace=True)
    model = autoencoder.Autoencoder(in_size=41899, out_size=50)
    model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu')))
    df = autoencoder.add_distances_to_df(source, target, outpath, model, df=df)
    # df.loc[:, 'total_score'] = df.progress_apply(lambda row: (row['avg_aligned'] + row['translation_score'] + math.log1p(row['alignment_count']) * row['alignment_score'] + math.log1p(row['match_counts']) * row['jac_sim'] + row['encoding_score']) / 5, axis=1)
    # model_xgb = XGBClassifier()
    # model_xgb.load_model("data/models/xgb_model_4.txt")
    # X = df[['translation_score', 'alignment_count', 'alignment_score', 'avg_aligned', 'jac_sim', 'match_counts', 'encoding_dist']]
    # df.loc[:, 'total_score'] = model_xgb.predict_proba(X)[:, 1]
    df.loc[:, 'simple_total'] = df.progress_apply(lambda row: (row['avg_aligned'] + row['translation_score'] + row['alignment_score'] + row['jac_sim']) / 4, axis=1)
    df.to_csv(outpath / 'all_in_context_with_scores.csv')


def remove_leading_and_trailing_blanks(df:pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Takes a dataframe and removes all rows before the first non-blank entry in a column, and after the last non-blank entry.
    """
    df = df[(df.loc[:, col].notna().cumsum() > 0) & (df.loc[::-1, col].notna().cumsum() > 0)]
    return df


if __name__ == "__main__":
    # #command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source bible")
    parser.add_argument("--target", type=Path, help="target bible")
    parser.add_argument(
        "--jaccard-similarity-threshold",
        type=float,
        help="Threshold for Jaccard Similarity score to be significant",
        default=0.05,
    )
    parser.add_argument("--is-bible", action='store_true', help="is bible")
    parser.add_argument(
        "--count-threshold",
        type=int,
        help="Threshold for count (number of co-occurences) score to be significant",
        default=0,
    )
    parser.add_argument("--outpath", type=Path, help="where to store results")
    parser.add_argument("--model", type=Path, help="Path to model for distance encodings")
    parser.add_argument("--combine-only", action='store_true', help="Only combine the results, since the alignment and matching files already exist")
    parser.add_argument("--refresh-cache", action='store_true', help="Refresh the cache of match scores")

    args, unknown = parser.parse_known_args()
    # make output dir
    # s, t, path = make_output_dir(args.source, args.target, args.outpath)
    outpath = args.outpath / f"{args.source.stem}_{args.target.stem}"

    outpath.mkdir(parents=True, exist_ok=True)
    if not args.combine_only:
        run_all_alignments(
                            args.source,
                            args.target,
                            outpath,
                            is_bible=args.is_bible,
                            jaccard_similarity_threshold=args.jaccard_similarity_threshold,
                            count_threshold=args.count_threshold,
                            refresh_cache = args.refresh_cache,
                            )
        

    run_combine_results(outpath)
    add_scores_to_alignments(args.source, args.target, outpath, args.model, args.is_bible)
    
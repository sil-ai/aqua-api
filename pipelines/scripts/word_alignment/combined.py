# imports
import os
import json
import argparse
import logging

import pandas as pd
import align
import align_best
import match
from tqdm import tqdm

tqdm.pandas()

from pathlib import Path
from typing import Tuple

# run fast_align
def run_fa(source, target, word_score_threshold, path, is_bible: bool, align_best_alignment: bool):
    align.run_align(source, target, word_score_threshold, path, is_bible)
    if align_best_alignment:
        align_best.run_best_align(source, target, word_score_threshold, path, is_bible)


# run match words
def run_match_words(
    source: Path,
    target: Path,
    outpath: Path,
    jaccard_similarity_threshold: float,
    count_threshold: int,
    refresh_cache: bool=False,
    ) -> None:
    """
    Runs match.run_match with the supplied arguments.
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
        "INFO",
        jaccard_similarity_threshold,
        count_threshold,
        refresh_cache=refresh_cache,
    )


def get_scores_from_match_dict(
    dictionary: dict, source: str, target: str
    ) -> Tuple[float, float]:
    """
    Takes a source word and a target word, looks them up in the match dictionary and returns the jaccard similarity and count fields for their match.
    Inputs:
    dictionary          The match dictionary for look up
    source              A string word to look up
    target              A string word to look up
    
    Outputs:
    jac_sim             The jaccard similarity between the source and target in the dictionary
    match_count         The count between the source and target in the dictionary
    """
    list_for_source = dictionary.get(source, [])
    match_list = [match for match in list_for_source if match.get("value") == target]
    if len(match_list) == 0:
        return 0, 0
    jac_sim = match_list[0]["jaccard_similarity"]
    match_count = match_list[0]["count"]
    return jac_sim, match_count


# combine results
def combine_df(outpath: Path, s: str, t: str) -> pd.DataFrame:
    """
    Reads the outputs saved to file from match.run_match(), align.run_align() and best_align.run_best_align() and saves to a single df
    Inputs:
    outpath             Path to the output directory
    s                   Name of the source input (generally the stem of the filename)
    t                   Name of the target input (generally the stem of the filename)

    Output:
    df                  A dataframe containing pairs of source and target words, with metrics from the three algorithms
    """
    # open results
    print(f"Combining results from the two algorithms from {s} to {t}")
    align_path = outpath / f"{s}_{t}_align/all_sorted.csv"
    best_path = outpath / f"{s}_{t}_align_best/best_sorted.csv"
    match_path = outpath / f"{s}_{t}_match/{s}_{t}-dictionary.json"

    all_results = pd.read_csv(align_path)
    best_results = pd.read_csv(best_path)
    all_results = all_results.merge(best_results, how='left', on=['source', 'target'])
    all_results = all_results.rename(columns = {
                                                'align_count_x': 'co-occurrences', 
                                                'word score': 'FA_translation_score', 
                                                'align_count_y': 'FA_align_count', 
                                                'verse score': 'FA_verse_score',
                                                })
    all_results.loc[:, ['avg_aligned']] = all_results.apply(
        lambda row: row['FA_align_count'] / row['co-occurrences'], axis = 1
        )
    all_results.loc[:, 'FA_align_count'] = all_results.loc[:, 'FA_align_count'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'verse score'] = all_results.loc[:, 'FA_verse_score'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'avg_aligned'] = all_results.loc[:, 'avg_aligned'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'alignment_count'] = all_results.loc[:, 'alignment_count'].apply(
        lambda x: 0 if pd.isnull(x) else x
        )
    all_results.loc[:, 'FA_translation_score'] = all_results.loc[:, 'FA_translation_score'].apply(
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


if __name__ == "__main__":
    # #command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument("--source", type=Path, help="source bible")
    parser.add_argument("--target", type=Path, help="target bible")
    parser.add_argument(
        "--align-best",
        type=bool,
        action='store_true',
        help="Get only the best alignments",
    )
    parser.add_argument(
        "--word-score-threshold",
        type=float,
        default=0.5,
        help="word score threshold {0,1}",
    )
    parser.add_argument(
        "--jaccard-similarity-threshold",
        type=float,
        help="Threshold for Jaccard Similarity score to be significant",
        default=0.5,
    )
    parser.add_argument("--is-bible", type=bool, action='store_true', help="is bible")
    parser.add_argument(
        "--count-threshold",
        type=int,
        help="Threshold for count (number of co-occurences) score to be significant",
        default=1,
    )
    parser.add_argument("--outpath", type=Path, help="where to store results")
    args, unknown = parser.parse_known_args()

    # make output dir
    # s, t, path = make_output_dir(args.source, args.target, args.outpath)
    path = args.outpath / f"{args.source.stem}_{args.target.stem}_combined"
    reverse_path = args.outpath / f"{args.target.stem}_{args.source.stem}_combined"

    if not path.exists():
        path.mkdir(exist_ok=True)
    if not reverse_path.exists():
        reverse_path.mkdir(exist_ok=True)

    # run fast align
    run_fa(
        args.source,
        args.target,
        args.word_score_threshold,
        path,
        args.is_bible,
        args.align_best,
    )

    # run match words
    run_match_words(
        args.source,
        args.target,
        path,
        args.jaccard_similarity_threshold,
        args.count_threshold,
    )

    # combine results
    df = combine_df(path, args.source.stem, args.target.stem)
    reverse_df = combine_df(reverse_path, args.target.stem, args.source.stem)

    # save results
    df.to_csv(path / f"{args.source.stem}_{args.target.stem}_combined.csv")
    reverse_df.to_csv(
        reverse_path / f"{args.target.stem}_{args.source.stem}_combined.csv"
    )

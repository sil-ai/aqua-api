#imports
import os
import json
import argparse 
import logging

import pandas as pd
import align
import match

from pathlib import Path
from typing import Tuple

#make ouput directory
def make_output_dir(source: Path, target: Path, outpath: Path) -> Tuple[str, str, Path]:
    s = source.stem
    t = target.stem
    path = outpath / f'{s}_{t}_combined' 
    path.mkdir(exist_ok=True)
    return s, t, path

#run fast_align
def run_fa(source, target, word_score_threshold, path, is_bible):
    align.run_align(source, target, word_score_threshold, path, is_bible)


#run match words
def run_match_words(source, target, path, jaccard_similarity_threshold, count_threshold, refresh_cache=False):
    match.run_match(source, target, path, 'INFO', jaccard_similarity_threshold, count_threshold, refresh_cache=refresh_cache)

def get_scores_from_match_dict(dictionary, source, target):
    list_for_source = dictionary.get(source, [])
    match_list = [match for match in list_for_source if match.get('value') == target]
    if len(match_list) == 0:
        return -1, -1
    jac_sim = match_list[0]['jaccard_similarity']
    match_count = match_list[0]['count']
    return jac_sim, match_count

#combine results
def combine_df(outpath: Path, s: str, t: str) -> pd.DataFrame:
    #open results
    align_path = outpath / f'{s}_{t}_align/sorted.csv'
    match_path = outpath / f'{s}_{t}_match/{s}_{t}-dictionary.json'
    fa_results = pd.read_csv(align_path)
    fa_results.loc[:, 'normalized_source'] = fa_results['source'].apply(match.normalize_word)
    fa_results.loc[:, 'normalized_target'] = fa_results['target'].apply(match.normalize_word)

    match_results = json.load(open(match_path))
    #explode the match data
    # sources = []
    # targets = []
    # jac_sims = []
    # counts = []

    # for lemma in match_results:
    #     for features in match_results[lemma]:
    #         sources.append(lemma)
    #         targets.append(features['value'])
    #         jac_sims.append(features['jaccard_similarity'])
    #         counts.append(features['count'])
    
    # data = {
    #     'normalized_source':sources,
    #     'normalized_target':targets,
    #     'jac_sim':jac_sims,
    #     'match_count':counts,
    # }
    
    #write to df and merge with fa results
    # match_results = pd.DataFrame(data)
    # df = pd.merge(fa_results, match_results, how='left', on=['normalized_source', 'normalized_target']).fillna(-1)
    df = fa_results
    df.loc[:, 'jac_sim'] = df.apply(lambda x: get_scores_from_match_dict(match_results, x['normalized_source'], x['normalized_target'])[0], axis=1)
    df.loc[:, 'match_counts'] = df.apply(lambda x: get_scores_from_match_dict(match_results, x['normalized_source'], x['normalized_target'])[1], axis=1)

    df.drop(columns=['Unnamed: 0'], inplace=True)
    return df


if __name__ == "__main__":
    # #command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument('--source', type=Path, help='source bible')
    parser.add_argument('--target', type=Path, help='target bible')
    parser.add_argument('--word-score-threshold', type=float, default=0.5, help='word score threshold {0,1}')
    parser.add_argument('--jaccard-similarity-threshold', type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.5)
    parser.add_argument('--is-bible', type=str, default="False", help='is bible')
    parser.add_argument('--count-threshold', type=int, help="Threshold for count (number of co-occurences) score to be significant", default=1)
    parser.add_argument('--outpath', type=Path, help='where to store results')
    args, unknown = parser.parse_known_args()

        

    #make output dir
    s, t, path = make_output_dir(args.source, args.target, args.outpath)

    #run fast align
    run_fa(args.source, args.target, args.word_score_threshold, path, args.is_bible)

    #run match words
    run_match_words(args.source, args.target, path, args.jaccard_similarity_threshold, args.count_threshold)

    #combine results
    df = combine_df(path, s, t)
    #save results
    df.to_csv(f"{path}/{s}_{t}_combined.csv")

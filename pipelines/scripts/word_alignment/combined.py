#imports
import os
import json
import argparse 

import pandas as pd
import align
import match

#make ouput directory
def make_output_dir(source, target, outpath):
    s = source.split('/')[-1].split('.')[0]
    t = target.split('/')[-1].split('.')[0]
    path = str(f'{outpath}/{s}_{t}_combined')
    os.makedirs(path, exist_ok=True)
    return s, t, path

#run fast_align
def run_fa(source, target, word_score_threshold, path, is_bible):
    align.run_align(source, target, word_score_threshold, path, is_bible)


#run match words
def run_match_words(source, target, path, jaccard_similarity_threshold, count_threshold):
    match.run_match(source, target, path, 'INFO', jaccard_similarity_threshold, count_threshold, True)


#combine results
def combine_df(outpath, s, t):
    #open results
    align_path = str(f'{outpath}/{s}_{t}_align/sorted.csv')
    match_path = str(f'{outpath}/{s}_{t}_match/{s}-{t}-dictionary.json')
    fa_results = pd.read_csv(align_path)
    match_results = json.load(open(match_path))

    #explode the match data
    sources = []
    targets = []
    jac_sims = []
    counts = []

    for lemma in match_results:
        for features in match_results[lemma]:
            sources.append(lemma)
            targets.append(features['value'])
            jac_sims.append(features['jaccard_similarity'])
            counts.append(features['count'])
            
    data = {
        'source':sources,
        'target':targets,
        'jac_sim':jac_sims,
        'match_count':counts,
    }

    #write to df and merge with fa results
    match_results = pd.DataFrame(data)
    df = pd.merge(fa_results, match_results, how='left', on=['source', 'target']).fillna(-1)
    df.drop(columns=['Unnamed: 0'], inplace=True)
    return df


if __name__ == "__main__":
    # #command line args
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument('--source', type=str, help='source bible')
    parser.add_argument('--target', type=str, help='target bible')
    parser.add_argument('--word-score-threshold', type=float, default=0.5, help='word score threshold {0,1}')
    parser.add_argument('--jaccard-similarity-threshold', type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.5)
    parser.add_argument('--is-bible', type=str, default="False", help='is bible')
    parser.add_argument('--count-threshold', type=int, help="Threshold for count (number of co-occurences) score to be significant", default=1)
    parser.add_argument('--outpath', type=str, help='where to store results')
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




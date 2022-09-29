# %%
#imports
import pandas as pd
import argparse
import json
import fast_align
import match_words_in_aligned_verse
import os

# %%
#command line args
parser = argparse.ArgumentParser(description="Argparser")
parser.add_argument('--source', type=str, help='source bible')
parser.add_argument('--target', type=str, help='target bible')
parser.add_argument('--word-score-threshold', type=float, default=0.5, help='word score threshold {0,1}')
parser.add_argument('--jaccard-similarity-threshold', type=float, help="Threshold for Jaccard Similarity score to be significant", default=0.5)
parser.add_argument('--is-bible', type=str, default="False", help='is bible')
parser.add_argument('--count-threshold', type=int, help="Threshold for count (number of co-occurences) score to be significant", default=1)
parser.add_argument('--outpath', type=str, help='where to store results')
args, unknown = parser.parse_known_args()

# %%
s = args.source.split('/')[-1].split('.')[0]
t = args.target.split('/')[-1].split('.')[0]
path = str(f'{args.outpath}/{s}_{t}_combined')
os.makedirs(path, exist_ok=True)

# get fast_align results
fast_align.run_fast_align(args.source, args.target, args.word_score_threshold, path, args.is_bible)

# %%
# get mark's results
match_words_in_aligned_verse.run_match_words_in_aligned_verse(args.source, args.target, path, 'INFO', args.jaccard_similarity_threshold, args.count_threshold, True)

# # %%
# #combine into df
fast_align_path = str(f'{args.outpath}/{s}_{t}_fast_align/sorted.csv')
mark_path = str(f'{args.outpath}/{s}_{t}_MWIAV/{s}-{t}-dictionary.json')

fa_results = pd.read_csv(fast_align_path)
mark_results = json.load(open(mark_path))

sources = []
targets = []
jac_sims = []
counts = []

for lemma in mark_results:
    for features in mark_results[lemma]:
        sources.append(lemma)
        targets.append(features['value'])
        jac_sims.append(features['jaccard_similarity'])
        counts.append(features['count'])
        
data = {
    'source':sources,
    'target':targets,
    'jac_sim':jac_sims,
    'mwiav_count':counts,
}

mark_results = pd.DataFrame(data)

# # %%
df = pd.merge(fa_results, mark_results, how='left', on=['source', 'target']).fillna(-1)

df.to_csv(f"{path}/{s}_{t}_combined.csv")




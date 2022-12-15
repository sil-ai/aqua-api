import argparse
from pathlib import Path
import json
from typing import Tuple

import pandas as pd

import get_data


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


def create_empty_df(source, target, is_bible=True):
    ref_df = get_data.get_ref_df(source, target, is_bible=is_bible)
    ref_df = get_data.condense_files(ref_df)
    ref_df = get_data.get_words_from_txt_file(ref_df, Path('/tmp'))
    df = ref_df.explode('src_words').explode('trg_words')
    return df

def main(args):
    for alignment_dir in args.alignment_dir.iterdir():
        print(alignment_dir)
        meta_file = alignment_dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        alignment_scores = pd.read_csv(alignment_dir / 'alignment_scores.csv')
        avg_alignment_scores = pd.read_csv(alignment_dir / 'avg_alignment_scores.csv')
    for translation_dir in args.translation_dir.iterdir():
        translation_scores = pd.read_csv(translation_dir / 'translation_scores.csv')
    for embedding_dir in args.embedding_dir.iterdir():
        embedding_scores = pd.read_csv(embedding_dir / 'embedding_scores.csv')
    for match_dir in args.match_dir.iterdir():
        with open(match_dir / 'dictionary.json') as f:
            match_scores = json.load(f)
    outpath = args.outpath / f'{source_str}_{target_str}'
    if not outpath.exists():
        outpath.mkdir()
    alignment_scores['vref'] = alignment_scores['vref'].astype('object')  # Necessary for non-Bible, where vrefs are ints.
    alignment_scores['alignment_score'] = alignment_scores['alignment_score'].fillna(0)
    source = args.sources_dir / f'{source_str}.txt'
    target = args.targets_dir / f'{target_str}.txt'

    all_results = create_empty_df(source, target, is_bible=args.is_bible)
    all_results = all_results.merge(alignment_scores, how='left', on=['vref', 'source', 'target'])
    all_results = all_results.fillna(0)
    all_results = alignment_scores.merge(avg_alignment_scores, how='left', on=['source', 'target'])
    all_results = all_results.merge(translation_scores, how='left', on=['source', 'target'])
    all_results.loc[:, 'avg_aligned'] = all_results.apply(lambda row: row['alignment_count'] / row['co-occurrence_count'], axis = 1).astype('float16')
    all_results.loc[:, 'translation_score'] = all_results.loc[:, 'translation_score'].apply(lambda x: 0 if x < 0.00001 else x).astype('float16')
    all_results.loc[:, "match_score"] = get_data.faster_df_apply(all_results, lambda x: get_scores_from_match_dict(match_scores, x["source"], x["target"], normalized=False)[0]).astype('float16')
    all_results = all_results.merge(embedding_scores, how='left', on=['source', 'target'])
    all_results.loc[:, 'total_score'] = get_data.faster_df_apply(all_results,lambda row: (row['avg_aligned'] + row['translation_score'] + row['alignment_score'] + row['match_score'] + row['embedding_score']) / 5)
    
    total_scores = all_results[['vref', 'source', 'target', 'total_score']]
    total_scores.to_csv(outpath / 'total_scores.csv', index=False)

    with open(outpath / 'meta.json', 'w') as f:
                json.dump(meta, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--alignment-dir", type=Path, help="directory with alignment scores")
    parser.add_argument("--translation-dir", type=Path, help="directory with translation scores")
    parser.add_argument("--match-dir", type=Path, help="directory with match scores")
    parser.add_argument("--embedding-dir", type=Path, help="directory with embedding scores")
    parser.add_argument("--sources-dir", type=Path, help="directory with source texts")
    parser.add_argument("--targets-dir", type=Path, help="directory with target texts")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--is-bible", action="store_true", help="Whether text is Bible, in which case the length of the text file must be 41,899 lines")
    args = parser.parse_args()

    main(args)
    
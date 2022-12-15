import argparse
import json
from pathlib import Path
from typing import Tuple, Optional

import pandas as pd


def identify_red_flags(target_str: str, top_source_scores: pd.DataFrame, ref_top_source_scores: Optional[pd.DataFrame], threshold: float=0.1) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Takes the directory of the source-target outputs, and a dictionary of reference language to reference language source-target outputs.
    Returns "red flags", which are source words that score low in the target language alignment data, compared to how they
    score in the source - reference language data.
    Inputs:
    target_str              String of the current target language
    top_source_scores         A dataframe with the top scores for each source word for the target translation
    ref_top_source_scores        A dataframe with a summary of the top scores for each source word across all translations
    threshold               A float for the score below which a match will be considered a possible red flag

    Outputs:
    possible_red_flags      A dataframe with low scores for source-target alignments
    red_flags               A dataframe with low scores for source-target alignments, when those same source words score highly in that
                            context in the reference languages.
    """
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].apply(lambda x: max(x, 0))
    top_source_scores.loc[:, 'total_score'] = top_source_scores['total_score'].fillna(0)
    possible_red_flags = top_source_scores[top_source_scores['total_score'] < 0.1]
    if not isinstance(ref_top_source_scores, pd.DataFrame):
        return possible_red_flags, possible_red_flags

    ref_top_source_scores = ref_top_source_scores.drop([target_str], axis=1)
    references  = [col for col in ref_top_source_scores.columns if col not in ['vref', 'source']]
    if len(references) > 0:
        ref_top_source_scores['mean'] = ref_top_source_scores.loc[:, references].mean(axis=1)
        ref_top_source_scores['min'] = ref_top_source_scores.loc[:, references].min(axis=1)
    if len(references) > 1:
        ref_top_source_scores['second_min'] = ref_top_source_scores.loc[:, references].apply(lambda row: sorted(list(row))[1], axis=1)
    
    possible_red_flags = possible_red_flags.merge(ref_top_source_scores, how='left', on=['vref', 'source'], sort=False)
    red_flags = possible_red_flags[possible_red_flags.apply(lambda row: row['mean'] > 5 * row['total_score'] and row['mean'] > 0.35, axis=1)]
    
    return possible_red_flags, red_flags


def main(args):
    top_source_scores_dir = args.top_source_scores_dir
    ref_dir = args.ref_dir
    ref_top_source_scores = pd.read_csv(ref_dir / 'summary_top_source_scores.csv')
    for dir in top_source_scores_dir.iterdir():  
        meta_file = dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        print(f'Source: {source_str}')
        print(f'Target: {target_str}')
        top_source_scores = pd.read_csv(dir / f'{source_str}_{target_str}/top_source_scores.csv')
        outpath = args.outpath / f'{source_str}_{target_str}'
        if not outpath.exists():
            outpath.mkdir()
        print(f"Identifying red flags for {source_str} to {target_str}...")
        possible_red_flags, red_flags = identify_red_flags(top_source_scores, ref_top_source_scores, threshold=args.threshold)
        red_flags.to_csv(outpath / f'red_flags.csv', index=False)
        possible_red_flags.to_csv(outpath / f'possible_red_flags.csv', index=False)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-source-scores-dir", type=Path, help="Input directory with top_source_scores.csv files", required=True)
    parser.add_argument("--ref-dir", type=Path, help="Directory with summary of all scores", required=True)
    parser.add_argument("--outpath", type=Path, help="Base output path, to write to.", required=True)
    parser.add_argument("--threshold", type=float, help="Threshold below which a score will be considered a possible red flag")

    args = parser.parse_args()
    main(args)


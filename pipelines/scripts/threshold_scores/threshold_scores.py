from pathlib import Path
import json
import argparse

import pandas as pd


def remove_leading_and_trailing_blanks(df:pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Takes a dataframe and removes all rows before the first non-blank entry in a column, and after the last non-blank entry.
    """
    df = df[(df.loc[:, col].notna().cumsum() > 0) & (df.loc[::-1, col].notna().cumsum() > 0)]
    return df

def get_threshold_scores(total_scores: pd.DataFrame, threshold: float):
    threshold_scores = remove_leading_and_trailing_blanks(total_scores, 'total_score')
    threshold_scores = threshold_scores.fillna(0)
    threshold_scores = threshold_scores.loc[threshold_scores['total_score'] > threshold]
    return threshold_scores
    
    
def main(args):
    for dir in args.total_scores_dir.iterdir():
        print(dir)
        meta_file = dir / 'meta.json'
        with open(meta_file) as f:
            meta = json.load(f)
        source_str = meta['source']
        target_str = meta['target']
        outpath = args.outpath / f'{source_str}_{target_str}'
        if not outpath.exists():
            outpath.mkdir()
        total_scores = pd.read_csv(dir / 'total_scores.csv')
        top_source_scores = get_threshold_scores(total_scores, args.threshold)
        top_source_scores.to_csv(outpath / 'threshold_scores.csv', index=False)
        with open(outpath / 'meta.json', 'w') as f:
            json.dump(meta, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total-scores-dir", type=Path, help="directory with total scores")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    parser.add_argument("--threshold", type=float, default=Path("/pfs/out"), help="Threshold for keeping scores")

    args = parser.parse_args()

    main(args) 
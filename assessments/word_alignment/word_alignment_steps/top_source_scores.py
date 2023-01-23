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

def get_verse_scores(total_scores: pd.DataFrame):
    top_source_scores = remove_leading_and_trailing_blanks(total_scores, 'total_score')
    top_source_scores = top_source_scores.fillna(0)
    top_source_scores = top_source_scores.loc[top_source_scores.groupby(['vref', 'source'], sort=False)['total_score'].idxmax(), :].reset_index(drop=True)
    return top_source_scores
    
    
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
        top_source_scores = get_verse_scores(total_scores)
        top_source_scores.to_csv(outpath / 'top_source_scores.csv', index=False)
        with open(outpath / 'meta.json', 'w') as f:
            json.dump(meta, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total-scores-dir", type=Path, help="directory with total scores")
    parser.add_argument("--outpath", type=Path, default=Path("/pfs/out"), help="Output directory")
    args = parser.parse_args()

    main(args) 